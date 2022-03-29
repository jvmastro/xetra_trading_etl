""" Test Meta Process methods"""

from datetime import date, datetime, timedelta
from io import BytesIO, StringIO
import os
import unittest

import boto3
from moto import mock_s3
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException
from xetra.common.meta_process import MetaProcess

from xetra.common.s3 import S3BucketConnector

import pandas as pd

class TestMetaProcessMethods(unittest.TestCase):
    """
    Testing for the MetaProcess Class
    """
    def setUp(self):
        # Mocking S3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining the class arguments 
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.us-east-2.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        # create s3 access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # Create bucket on mocked S3 
        self.s3 = boto3.resource(service_name = 's3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                                CreateBucketConfiguration = {
                                    'LocationConstraint': 'us-east-2'
                                })
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creat testing instances
        self.s3_bucket_meta = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)
        self.dates = [(datetime.today().date() - timedelta(days=day)).strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(8)]
    
    def tearDown(self):
        # Mocking s3 connection stop
        self.mock_s3.stop()
    
    def test_update_meta_file_no_meta_file(self):
        """
        Tests the update_meta_file method when there is no meta file
        """
        # Expected results 
        date_list_exp = ['2022-03-27', '2022-03-28']
        proc_date_list_exp = [datetime.today().date()] * 2
        # Test init
        meta_key = 'meta.csv'
        # Method execution
        MetaProcess.update_meta_file(date_list_exp, meta_key, self.s3_bucket_meta)
        # Read meta file
        data= self.s3_bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        proc_date_list_result = list(pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value]).dt.date)
        # Test method after execution
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proc_date_list_exp, proc_date_list_result)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete ={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_empty_date_list(self):
        """
        Tests the update_meta_file_method when the argument extract_date_list is empty
        """
        # Expected results
        return_exp = True
        log_exp = ' The dataframe is empty! No such file will be written'
        # Test init
        date_list = []
        meta_key = 'meta.csv'
        # Method execution
        with self.assertLogs() as logm:
            result = MetaProcess.update_meta_file(date_list, meta_key, self.s3_bucket_meta)
            # Log test after the method execution
            self.assertIn(log_exp, logm.output[1])
        # Test after method execution
        self.assertEqual(return_exp, result)
    
    def test_update_meta_file_meta_file_ok(self):
        """
        Tests the update_meta_file method when a meta file already exists 
        """
        # Expected results
        date_list_old = ['2022-03-15','2022-03-16']
        date_list_new = ['2022-03-21','2022-03-22']
        date_list_exp = date_list_old + date_list_new
        proc_date_list_exp = [datetime.today().date()] * 4
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
            f'{MetaProcessFormat.META_PROCESS_COL.value},'
            f'{date_list_old[0]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
            f'{date_list_old[1]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}'
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        # Method Execution
        MetaProcess.update_meta_file(date_list_new, meta_key, self.s3_bucket_meta)
        # Read meta file
        data = self.s3_bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        proc_date_list_result = list(pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value]).dt.date)
        # Test after method execution
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proc_date_list_exp, proc_date_list_result)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Object': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )
    def test_update_meta_file_meta_file_wrong(self):
        """
        Tests the update_meta_file method when there is a wrong meta file
        """
        # Expected results
        date_list_old = ['2022-03-15','2022-03-16']
        date_list_new = ['2022-03-21','2022-03-22']
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'wrong_column, {MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{date_list_old[0]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
            f'{date_list_old[1]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}'
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        # Method Execution
        with self.assertRaises(WrongMetaFileException):
            MetaProcess.update_meta_file(date_list_new, meta_key, self.s3_bucket_meta)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Object': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )
            