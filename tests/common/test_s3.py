"""TestS3BuketConnectorMethods"""
import os
import unittest

import boto3
from moto import mock_aws as mock_s3

from xetra.common.s3 import S3BucketConnector

class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the S3BucketConnector class.
    """

    def setUp(self):
        """
        Setting up the 
        """
        # mocking the S3 bucket
        self.mock_s3 = mock_s3()  # Correctly using moto.mock_s3()
        self.mock_s3.start()
        # Define the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'http://s3.eu-central-1.amazonaws.com'
        self.s3_bucket = 'test-bucket'
        #Creating s3 access key as  environment variable
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # Creating the S3 bucket
        self.s3 = boto3.resource(
            service_name='s3',
            endpoint_url=self.s3_endpoint_url,
            # aws_access_key_id=os.environ[self.s3_access_key],
            # aws_secret_access_key=os.environ[self.s3_secret_key]
        )
        self.s3.create_bucket(Bucket=self.s3_bucket, 
                                CreateBucketConfiguration={
                'LocationConstraint': 'eu-central-1'
            }
        )
        self.s3_bucket = self.s3.Bucket(self.s3_bucket)
        # Creating the S3BucketConnector instance
        self.s3_connector = S3BucketConnector(
            access_key=self.s3_access_key,
            secret_key=self.s3_secret_key,
            endpoint_url=self.s3_endpoint_url,
            bucket=self.s3_bucket.name
        )


    def tearDown(self):
        """
        Tear down the test case.
        Tear down – Clean up anything the test created (e.g. delete files, close connections, reset settings)
        """
        #mocking s3 connection stop
        self.mock_s3.stop()

    def test_list_files_in_prefix_ok(self):
        """
        Test the list_files_in_prefix method for getting 2 file keys
        as list on the mocked s3 bucket.
        """
        # Expected results
        prefix_exp =  'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'
        # Test init
        csv_content = """col1,col2
        value1,value2"""
        self.s3_bucket.put_object(
                                    Key=key1_exp,
                                    Body=csv_content
                                )
        self.s3_bucket.put_object(
                                    Key=key2_exp,
                                    Body=csv_content
                                )
        # Method excution
        list_result = self.s3_connector.list_files_in_prefix(prefix=prefix_exp)
        #Tests after the method execution
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)
        #Clean up after the method execution
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {'Key': key1_exp},
                    {'Key': key2_exp}
                ]
            }
        )
        print("Test list_files_in_prefix_ok passed.")

    def test_list_files_in_prefix_wrong_prefix(self):
        """
        Test the list_files_in_prefix method in case of a
        wrong or non-existing prefix.
        as list on the mocked s3 bucket.
        """
         # Expected results
        prefix_exp =  'no-prefix/'
        # key1_exp = f'{prefix_exp}test1.csv'
        # key2_exp = f'{prefix_exp}test2.csv'
        # # Test init
        # csv_content = """col1,col2
        # value1,value2"""
        # self.s3_bucket.put_object(
        #                             Key=key1_exp,
        #                             Body=csv_content
        #                         )
        # self.s3_bucket.put_object(
        #                             Key=key2_exp,
        #                             Body=csv_content
        #                         )
        # Method excution
        list_result = self.s3_connector.list_files_in_prefix(prefix=prefix_exp)
        #Tests after the method execution
        # self.assertEqual(len(list_result), 2)
        # self.assertIn(key1_exp, list_result)
        self.assertTrue(not list_result)

        

if __name__ == '__main__':
    unittest.main()
    # testIns = TestS3BucketConnectorMethods()
    # testIns.setUp()
    # testIns.test_list_files_in_prefix_ok()
    # testIns.tearDown()