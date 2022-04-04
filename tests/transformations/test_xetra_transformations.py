"""Test XetraETL Methods"""

from io import BytesIO
import os
import unittest
from unittest.mock import patch

import boto3
import pandas as pd
from moto import mock_s3

from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.transformations.xetra_transformations import XetraETL, XetraSourceConfig, XetraTargetConfig


class TestXetraETLMethods(unittest.TestCase):
    """
    Testing the XetraETL class
    """

    def setUp(self):
        """Setting up the environment"""
        # Mocking S3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.us-east-2.amazonaws.com'
        self.s3_bucket_name_src = 'src-bucket'
        self.s3_bucket_name_trg = 'trg-bucket'
        self.meta_key = 'meta_key'
        # create s3 access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # Create bucket on mocked S3
        self.s3 = boto3.resource(
            service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name_src,
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'us-east-2'
                              })
        self.s3.create_bucket(Bucket=self.s3_bucket_name_trg,
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'us-east-2'
                              })
        self.src_bucket = self.s3.Bucket(self.s3_bucket_name_src)
        self.trg_bucket = self.s3.Bucket(self.s3_bucket_name_trg)
        # Creat testing instances
        self.s3_src_bucket = S3BucketConnector(self.s3_access_key,
                                               self.s3_secret_key,
                                               self.s3_endpoint_url,
                                               self.s3_bucket_name_src)
        self.s3_trg_bucket = S3BucketConnector(self.s3_access_key,
                                               self.s3_secret_key,
                                               self.s3_endpoint_url,
                                               self.s3_bucket_name_trg)
        config_dict_src = {
            'src_first_extract_date': '2022-03-01',
            'src_columns': ['ISIN', 'Mnemonic', 'Date',
                            'Time', 'StartPrice', 'EndPrice',
                            'MinPrice', 'MaxPrice', 'TradedVolume'],
            'src_col_date': 'Date',
            'src_col_isin': 'ISIN',
            'src_col_time': 'Time',
            'src_col_start_price': 'StartPrice',
            'src_col_min_price': 'MinPrice',
            'src_col_max_price': 'MaxPrice',
            'src_col_traded_vol': 'TradedVolume'
        }
        config_dict_trg = {
            'trg_col_isin': 'isin',
            'trg_col_date': 'date',
            'trg_col_op_price': 'opening_price_eur',
            'trg_col_clos_price': 'closing_price_eur',
            'trg_col_min_price': 'minimum_price_eur',
            'trg_col_max_price': 'maximum_price_eur',
            'trg_col_daily_trad_vol': 'daily_traded_volume',
            'trg_col_ch_prev_clos': 'change_prev_closing_%',
            'trg_key': 'report1/xetra_daily_report1',
            'trg_key_date_format': '%Y%m%d_%H%M%S',
            'trg_format': 'parquet'
        }
        self.source_config = XetraSourceConfig(**config_dict_src)
        self.target_config = XetraTargetConfig(**config_dict_trg)
        # creating source files on mocked s3
        columns_src = ['ISIN', 'Mnemonic', 'Date', 'Time', 'StartPrice',
                       'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume']
        data = [
            ['AT0000A0E9W5', 'SANT', '2022-03-15',
                '12:00', 20.19, 18.45, 18.20, 20.33, 877],
            ['AT0000A0E9W5', 'SANT', '2022-03-16',
                '15:00', 18.27, 21.19, 18.27, 21.34, 987],
            ['AT0000A0E9W5', 'SANT', '2022-03-17',
                '13:00', 20.21, 18.27, 18.21, 20.42, 633],
            ['AT0000A0E9W5', 'SANT', '2022-03-17',
                '14:00', 18.27, 21.19, 18.27, 21.34, 455],
            ['AT0000A0E9W5', 'SANT', '2022-03-18',
                '07:00', 20.58, 19.27, 18.89, 20.58, 9066],
            ['AT0000A0E9W5', 'SANT', '2022-03-18',
                '08:00', 19.27, 21.14, 19.27, 21.14, 1220],
            ['AT0000A0E9W5', 'SANT', '2022-03-19',
                '07:00', 23.58, 23.58, 23.58, 23.58, 1035],
            ['AT0000A0E9W5', 'SANT', '2022-03-19',
                '08:00', 23.58, 24.22, 23.31, 24.34, 1028],
            ['AT0000A0E9W5', 'SANT', '2022-03-19', '09:00', 24.22, 22.21, 22.21, 25.01, 1523]]
        self.df_src = pd.DataFrame(data, columns=columns_src)
        self.s3_src_bucket.write_df_to_s3(
            self.df_src.loc[0:0], '2022-03-15/2022-03-15_BINS_XETR12.csv', 'csv')
        self.s3_src_bucket.write_df_to_s3(
            self.df_src.loc[1:1], '2022-03-16/2022-03-16_BINS_XETR13.csv', 'csv')
        self.s3_src_bucket.write_df_to_s3(
            self.df_src.loc[2:2], '2022-03-17/2022-03-17_BINS_XETR14.csv', 'csv')
        self.s3_src_bucket.write_df_to_s3(
            self.df_src.loc[3:3], '2022-03-17/2022-03-17_BINS_XETR15.csv', 'csv')
        self.s3_src_bucket.write_df_to_s3(
            self.df_src.loc[4:4], '2022-03-18/2022-03-18_BINS_XETR16.csv', 'csv')
        self.s3_src_bucket.write_df_to_s3(
            self.df_src.loc[5:5], '2022-03-18/2022-03-18_BINS_XETR17.csv', 'csv')
        self.s3_src_bucket.write_df_to_s3(
            self.df_src.loc[6:6], '2022-03-19/2022-03-19_BINS_XETR07.csv', 'csv')
        self.s3_src_bucket.write_df_to_s3(
            self.df_src.loc[7:7], '2022-03-19/2022-03-19_BINS_XETR08.csv', 'csv')
        self.s3_src_bucket.write_df_to_s3(
            self.df_src.loc[8:8], '2022-03-19/2022-03-19_BINS_XETR09.csv', 'csv')
        columns_report = ['ISIN', 'Date', 'opening_price_eur', 'closing_price_eur',
                          'minimum_price_eur', 'maximum_price_eur', 'daily_traded_volume',
                          'change_prev_closing_%']
        data_report = [['AT0000A0E9W5', '2022-03-17', 20.21, 18.27, 18.21, 21.34, 1088, 10.62],
                       ['AT0000A0E9W5', '2022-03-18', 20.58,
                           19.27, 18.89, 21.14, 10286, 1.83],
                       ['AT0000A0E9W5', '2022-03-19', 23.58, 24.22, 22.21, 25.01, 3586, 14.58]]
        self.df_report = pd.DataFrame(data_report, columns=columns_report)

    def tearDown(self):
        """Executing after unit test"""
        # Mocking S3 connection stopped
        self.mock_s3.stop()

    def test_extact_no_files(self):
        """
        Tests the extract method when there are no files to be extracted
        """
        # Test init
        extract_date = '2200-01-02'
        extract_date_list = []
        # Method execution
        with patch.object(MetaProcess, 'return_date_list',
                          return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_src_bucket, self.s3_trg_bucket,
                                 self.meta_key, self.source_config, self.target_config)
            df_return = xetra_etl.extract()
        # Test after method execution
        self.assertTrue(df_return.empty)

    def test_extract_files(self):
        """
        Tests the extract method when there are good files to be extracted
        """
        # Expected results
        df_exp = self.df_src.loc[1:8].reset_index(drop=True)
        # Test Init
        extract_date = '2022-03-17'
        extract_date_list = ['2022-03-16', '2022-03-17',
                             '2022-03-18', '2022-03-19', '2022-03-20']
        # Method execution
        with patch.object(MetaProcess, 'return_date_list',
                          return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_src_bucket, self.s3_trg_bucket,
                                 self.meta_key, self.source_config, self.target_config)
            df_result = xetra_etl.extract()
        self.assertTrue(df_exp.equals(df_result))

    def test_transform_report1_emptydf(self):
        """
        Tests the transform_report1 method with an empty DataFrame as an input argument
        """
        # Expected results
        log_exp = 'The dataframe is empty. No transformations will be applied!'
        # Test init
        extract_date = '2022-03-17'
        extract_date_list = ['2022-03-16', '2022-03-17', '2022-03-18']
        df_input = pd.DataFrame()
        # Method execution
        with patch.object(MetaProcess, 'return_date_list',
                          return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_src_bucket, self.s3_trg_bucket,
                                 self.meta_key, self.source_config, self.target_config)
            with self.assertLogs() as logm:
                df_result = xetra_etl.transform_report1(df_input)
                # Log test after method execution
                self.assertIn(log_exp, logm.output[0])
        # Test after method execution
        self.assertTrue(df_result.empty)

    def test_transform_report1_ok(self):
        """
        Tests the transform_report1 method with a DataFrame as an input argument
        """
        # Expected results
        log1_exp = 'Applying transformations to Xetra source data for report 1 started...'
        log2_exp = 'Applying transformations to Xetra source data finished...'
        df_exp = self.df_report
        # Test init
        extract_date = '2022-03-17'
        extract_date_list = ['2022-03-16',
                             '2022-03-17', '2022-03-18', '2022-03-19']
        df_input = self.df_src.loc[1:8].reset_index(drop=True)
        # Method execution
        with patch.object(MetaProcess, 'return_date_list',
                          return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_src_bucket, self.s3_trg_bucket,
                                 self.meta_key, self.source_config, self.target_config)
            with self.assertLogs() as logm:
                df_result = xetra_etl.transform_report1(df_input)
                # Log test after method execution
                self.assertIn(log1_exp, logm.output[0])
                self.assertIn(log2_exp, logm.output[1])
        # Test after method execution
        self.assertTrue(df_exp.equals(df_result))

    def test_load(self):
        """
        Tests the load method
        """
        # Expected results
        log1_exp = 'Xetra target data successfully written.'
        log2_exp = 'Xetra meta file successfully updated.'
        df_exp = self.df_report
        meta_exp = ['2022-03-17', '2022-03-18', '2022-03-19']
        # Test init
        extract_date = '2022-03-17'
        extract_date_list = ['2022-03-17', '2022-03-18', '2022-03-19']
        df_input = self.df_report
        # Method execution
        with patch.object(MetaProcess, 'return_date_list',
                          return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_src_bucket, self.s3_trg_bucket,
                                 self.meta_key, self.source_config, self.target_config)
            with self.assertLogs() as logm:
                xetra_etl.load(df_input)
                # Log test after method execution
                self.assertIn(log1_exp, logm.output[1])
                self.assertIn(log2_exp, logm.output[4])
        # Test after method execution
        trg_file = self.s3_trg_bucket.list_files_in_prefix(
            self.target_config.trg_key)[0]
        data = self.trg_bucket.Object(key=trg_file).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertTrue(df_exp.equals(df_result))
        meta_file = self.s3_trg_bucket.list_files_in_prefix(self.meta_key)[0]
        df_meta_result = self.s3_trg_bucket.read_csv_to_df(meta_file)
        self.assertEqual(list(df_meta_result['source_date']), meta_exp)
        # Cleanup after test
        self.trg_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': trg_file
                    },
                    {
                        'Key': trg_file
                    }
                ]
            }
        )

    def test_etl_report1(self):
        """
        Tests the etl_report1 method
        """
        # Expected results
        df_exp = self.df_report
        meta_exp = ['2022-03-17', '2022-03-18', '2022-03-19']
        # Test init
        extract_date = '2022-03-17'
        extract_date_list = ['2022-03-16',
                             '2022-03-17', '2022-03-18', '2022-03-19']
        # Method execution
        with patch.object(MetaProcess, 'return_date_list',
                          return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_src_bucket, self.s3_trg_bucket,
                                 self.meta_key, self.source_config, self.target_config)
            xetra_etl.etl_report1()
        # Test after method execution
        trg_file = self.s3_trg_bucket.list_files_in_prefix(
            self.target_config.trg_key)[0]
        data = self.trg_bucket.Object(key=trg_file).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertTrue(df_exp.equals(df_result))
        meta_file = self.s3_trg_bucket.list_files_in_prefix(self.meta_key)[0]
        df_meta_result = self.s3_trg_bucket.read_csv_to_df(meta_file)
        self.assertEqual(list(df_meta_result['source_date']), meta_exp)
        # Cleanup after test
        self.trg_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': trg_file
                    },
                    {
                        'Key': trg_file
                    }
                ]
            }
        )


if __name__ == '__main__':
    unittest.main()
