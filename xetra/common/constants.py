"""
File to store constants
"""

from enum import Enum

class S3FileTypes(Enum):
    """
    Enum to store S3 file 
    Supported file types for S3BucketConnector
    """
    CSV = "csv"
    PARQUET = "parquet"

class MetaProcessFormat(Enum):
    """
    Enum to store meta file format
    Supported formats for MetaProcess
    """
    META_DATE_FORAMT = '%Y-%M-%d'
    META_PROCESS_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
    META_SOURCE_DATE_COL = 'datetime_of_processing'
    META_FILE_FORMAT = 'csv'

