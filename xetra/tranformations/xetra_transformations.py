""" 
Xetra ETL Component
"""
import logging 

from typing import NamedTuple
import pandas as pd
from xetra.common.s3 import S3BucketConnector

class XetraSourceConfig(NamedTuple):
    """
    Class for source configuration data

    Params:
        src_first_extract_date (str): determines the date for extracting the source
        src_columns (list): source column names
        src_col_date (str): column name for date in source
        src_col_isin (str): column name for isin source
        src_col_time (str): column name for time in source
        src_col_start_price (str): column name for starting price in source 
        src_col_min_price (str): column name for minimum price in source
        src_col_max_price (str): column name for maximum price in source 
        src_col_trade_vol (str): column name for traded volumn in source 
    """
    
    src_first_extract_date: str 
    src_columns: list
    src_col_date: str 
    src_col_isin: str 
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_trade_vol: str
    
class XetraTargetConfig(NamedTuple):
    """
    Class for target configuration data

    Params:
        trg_col_isin (str): column name for isin in target
        trg_col_date (str): column name for date in target
        trg_col_op_price (str): column name for opening price in target
        trg_col_clos_price (str): column name for closing price in target
        trg_col_min_price (str): column name for minimum price in target
        trg_col_max_price (str): column name for maximum price in target
        trg_daily_traded_volume (str): column names for daily traded volume in target
        trg_key (str): basic key for target file
        trg_key_date_format (str): date format of the target file key
        trg_format (str): file format of the target file
    """
    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_daily_traded_volume: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str
    
class XetraETL():
    """
    Reads the Xetra data, transforms and wrties the transformed to target
    """
    
    def __init__(self, s3_bucket_src: S3BucketConnector, s3_bucket_trg: S3BucketConnector, meta_key: str, src_args: XetraSourceConfig, trg_args: XetraTargetConfig):
        """
        Class constructor for XetraTransformer

        Params:
            s3_bucket_src (S3BucketConnector): connection to source S3 bucket
            s3_bucket_trg (S3BucketConnector): connection to target S3 bucket 
            meta_key (str): used as self.meta_key -> key of meta file
            src_args (XetraSourceConfig): NamedTuple class with source configuration data
            trg_args (XetraTargetConfig): NamedTuple class with target configuration data
        """
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_date = 
        self.extract_date_list = 
        self.meta_update_list = 
        
    def extract(self):
        """
        Read the source data and concatenates them to one Pandas DataFrame
        
        Returns:
            data_frame (pd.DataFrame): Pandas DataFrame with the extracted data
        """
        self._logger.info('Extracting Xetra source files started...')
        files = [key for date in self.extract_date_list for key in self.s3_bucket_src.list_files_in_prefix(date)]
        if not files:
            data_frame = pd.DataFrame()
        else:
            data_frame = pd.concat([self.s3_bucket_src.read_csv_to_df(file) for file in files], ignore_index=True)
        self._logger('Extracting Xetra source files finished')
        return data_frame
    
    def transform_report1(self):
        pass
    
    def load(self):
        pass
    
    def etl_report1(self):
        pass
    
    