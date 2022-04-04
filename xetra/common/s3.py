"""Connector and methods accessing S3"""
from io import StringIO, BytesIO

import os
import logging

import pandas as pd

import boto3

from xetra.common.constants import S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException


class S3BucketConnector():
    """
    Class for interacting with S3 buckets
    """

    def __init__(self, access_key: str, secret_key: str,
                 endpoint_url: str, bucket: str):
        """
        Constructor for S3BucketConnector

        Params:
            access_key (str): access key for accessing S3 from AWS account
            secret_key (str): secret key for accessing S3 from AWS account
            endpoint_url (str): endpoint url to S3 from AWS account
            bucket (str): S3 bucket name from AWS account
        """
        self._logger = logging.getLogger(__name__)
        self._endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_key])
        self._s3 = self.session.resource(
            service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str):
        """Listing all files with a prefix on the S3 Bucket

        Params:
            prefix (str): prefix on the S3 bucket that should be filtererd with

        Returns:
            files (lst): list of all file names containing the prefix in the key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(
            self, key: str, encoding: str = 'utf-8', sep: str = ','):
        """Reading a csv file from the S3 bucket and returning a dataframe

        Params:
            key (str): key of the file that should be read
            encoding (str): encoding of the data inside the file
            sep (str): seperator of teh csv file

        Returns:
            data_frame (DataFrame): Pandas DataFrame containing the csv file
        """
        self._logger.info('Reading file %s/%s/%s',
                          self._endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get(
            'Body').read().decode(encoding)
        data = StringIO(csv_obj)
        data_frame = pd.read_csv(data, sep=sep)

        return data_frame

    def write_df_to_s3(self, data_frame: pd.DataFrame,
                       key: str, file_format: str):
        """
        Writing a Pandas DataFrame to S3 supported formats: .csv, .parquet

        Params:
            data_frame (pd.DataFrame): Pandas DataFrame that should be written to S3
            key (str): taget ky of the saved file
            file_format (str) format of the saved filed

        """
        if data_frame.empty:
            self._logger.info(
                'The dataframe is empty! No such file will be written!')
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        self._logger.info(
            'The file format %s is not supported to be written to S3!', file_format)
        raise WrongFormatException

    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()

        Params:
            out_buffer (StringIO | BytesIO):
            key (str): target key of saved file
        """
        self._logger.info('Writing file to %s/%s/%s',
                          self._endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
