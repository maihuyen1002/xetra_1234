"""Connector and methods accessing S3."""

import os
import logging
from io import StringIO, BytesIO

import boto3
import pandas as pd

from xetra.common.constants import S3FileTypes # type: ignore

from xetra.common.constants import S3FileTypes
from xetra.common.Custom_exceptions import WrongFormatException, WrongMetaFileException

class S3BucketConnector:
    """
    Class for interacting with S3 bucket.
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor for S3BucketConnector.
        :param access_key: access key for accessing S3
        :param secret_key :secret key for accessing S3
        :param endpoint_url: endpoint URL to S3
        :param bucket: S3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session( 
                aws_access_key_id = os.environ[access_key],
                aws_secret_access_key = os.environ[secret_key],
                )
        self._s3 = self.session.resource(
                service_name = 's3',
                endpoint_url = endpoint_url,
                )
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str):
        """
        List all files in a given prefix in the S3 bucket.
        :param prefix: prefix on the S3 bucket thay should be filtered with

        return: 
           file: list of files in the given prefix
        """
        # def list_files_in_prefix(bucket, prefix):
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files
    
    # def list_files_in_prefix(self, prefix: str):
    #     self._logger.info(f"Listing files with prefix: {prefix}")
    #     objects = list(self._bucket.objects.filter(Prefix=prefix))
    #     if not objects:
    #         self._logger.warning(f"No files found for prefix: {prefix}")
    #     return [obj.key for obj in objects]

    # def list_files_in_prefix(self, prefix: str):
    #     """
    #     List all files in a given prefix in the S3 bucket.
    #     :param prefix: prefix on the S3 bucket that should be filtered with

    #     return: 
    #     list of file keys in the given prefix
    #     """
    #     if not prefix.endswith('/'):
    #         prefix += '/'
    #     self._logger.info(f"Listing files with prefix: {prefix}")
    #     try:
    #         files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
    #         return files
    #     except Exception as e:
    #         self._logger.error(f"Error accessing prefix {prefix}: {e}")
    #         raise

   
    # def list_files_in_prefix(self, prefix: str):
    #     self._logger.info(f"Listing files with prefix: {prefix}")
    #     try:
    #         objects = list(self._bucket.objects.filter(Prefix=prefix))
    #         if not objects:
    #             self._logger.warning(f"No files found for prefix: {prefix}")
    #         return [obj.key for obj in objects]
    #     except Exception as e:
    #         self._logger.error(f"Exception when listing files with prefix {prefix}: {e}")
    #         raise



    def read_csv_to_df(self, key: str, encoding: str = 'utf-8', sep: str = ','):
        """
        Reading a csv file from S3 bucket and returning a dataframe.

        :param key: key of the file in S3 bucket
        :param encoding: encoding of the file
        :param sep: separator of the file

        returns:
            dataframe: containing the data from the csv file
        """
        self._logger.info('Reading file %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        dataframe = pd.read_csv(data, delimiter=sep)

        return dataframe    

    def write_df_to_s3(self, data_frame: pd.DataFrame, key: str, file_format: str = 'csv'):
        """
        Writing a pandas dataframe to S3.
        Supported formats: csv, parquet

        :data_frame: Pandas dataframe to be written
        :key: key of the file in S3 bucket
        :file_format: format ò the saved file
        """
        if data_frame.empty:
            self._logger.info('The dataframe is empty! No file will be written!')
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        # else:
        #     raise WrongFormatException(f'File format {file_format} is not supported!')
        
    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()

        :out_buffer: StringIO | BytesIO that should be written
        :key: target key of the saved file
        """
        self._logger.info('Writing file to %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True