""" Xetra ETL Component"""
from datetime import datetime
import logging


from typing import NamedTuple
import pandas as pd
from xetra.common.meta_process import MetaProcess
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
    src_col_end_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str


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
    trg_col_daily_trad_vol: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str


class XetraETL():
    """
    Reads the Xetra data, transforms and wrties the transformed to target
    """

    def __init__(self, s3_bucket_src: S3BucketConnector,
                 s3_bucket_trg: S3BucketConnector,
                 meta_key: str,
                 src_args: XetraSourceConfig,
                 trg_args: XetraTargetConfig):
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
        self.extract_date, self.extract_date_list = MetaProcess.return_date_list(
            self.src_args.src_first_extract_date, self.meta_key, self.s3_bucket_trg)
        self.meta_update_list = [
            date for date in self.extract_date_list if date >= self.extract_date]

    def extract(self):
        """
        Read the source data and concatenates them to one Pandas DataFrame

        Returns:
            data_frame (pd.DataFrame): Pandas DataFrame with the extracted data
        """
        self._logger.info('Extracting Xetra source files started...')
        files = [
            key for date in self.extract_date_list
            for key in self.s3_bucket_src.list_files_in_prefix(date)]
        if not files:
            data_frame = pd.DataFrame()
        else:
            data_frame = pd.concat([self.s3_bucket_src.read_csv_to_df(
                file) for file in files], ignore_index=True)
        self._logger.info('Extracting Xetra source files finished')
        return data_frame


    def transform_report1(self, data_frame: pd.DataFrame):
        """
        Applies the necessary transformations to create report 1

        Params:
            data_frame (pd.DataFrame): Pandas DataFrame as input

        Returns:
            data_frame = transformed pandas dataframe
        """
        if data_frame.empty:
            self._logger.info(
                'The dataframe is empty. No transformations will be applied!')
            return data_frame
        self._logger.info(
            'Applying transformations to Xetra source data for report 1 started...')
        # Filtering necessary source columns
        data_frame = data_frame.loc[:, self.src_args.src_columns]
        # Removing rows with missing values
        data_frame.dropna(inplace=True)
        # Calculating opening price per ISIN and day
        data_frame[self.trg_args.trg_col_op_price] = data_frame.sort_values(by=[self.src_args.src_col_time]).groupby([
            self.src_args.src_col_isin,
            self.src_args.src_col_date])[self.src_args.src_col_start_price].transform('first')
        # Calculating closing price per ISIN and day
        data_frame[self.trg_args.trg_col_clos_price] = data_frame.sort_values(by=[self.src_args.src_col_time]).groupby([
            self.src_args.src_col_isin,
            self.src_args.src_col_date])[self.src_args.src_col_end_price].transform('last')
        # Renaming columns
        data_frame.rename(columns={
            self.src_args.src_col_min_price: self.trg_args.trg_col_min_price,
            self.src_args.src_col_max_price: self.trg_args.trg_col_max_price,
            self.src_args.src_col_traded_vol: self.trg_args.trg_col_daily_trad_vol,
        }, inplace=True)
        # Aggregating per ISIN and day -> opening price, closing price, min
        # price, max price, traded volume
        data_frame = data_frame.groupby([
            self.src_args.src_col_isin,
            self.src_args.src_col_date], as_index=False).agg({
                self.trg_args.trg_col_op_price: 'min',
                self.trg_args.trg_col_clos_price: 'max',
                self.trg_args.trg_col_min_price: 'min',
                self.trg_args.trg_col_max_price: 'max',
                self.trg_args.trg_col_daily_trad_vol: 'sum',
            })
        # % Change of current day's closing price compared to the previous trading day's closing price
        data_frame[self.trg_args.trg_col_ch_prev_clos] = data_frame.sort_values(
            by=[self.src_args.src_col_date]).groupby([self.src_args.src_col_isin])[self.trg_args.trg_col_op_price].shift(1)
        data_frame[self.trg_args.trg_col_ch_prev_clos] = (
            data_frame[self.trg_args.trg_col_op_price] - data_frame[self.trg_args.trg_col_ch_prev_clos]) / data_frame[self.trg_args.trg_col_ch_prev_clos] * 100
        # Rounding to 2 decimal places
        data_frame = data_frame.round(decimals=2)
        # Removing the day before extract_date
        data_frame = data_frame[data_frame.Date >=
                                self.extract_date].reset_index(drop=True)
        self._logger.info(
            'Applying transformations to Xetra source data finished...')
        return data_frame

    def load(self, data_frame: pd.DataFrame):
        """
        Saves a Pandas Dataframe to the target

        Params:
            data_frame (pd.DataFrame): dataframe to load
        """
        # Creating target key
        target_key = (
            f'{self.trg_args.trg_key}'
            f'{datetime.today().strftime(self.trg_args.trg_key_date_format)}'
            f'{self.trg_args.trg_format}'
        )
        # Writing to target
        self.s3_bucket_trg.write_df_to_s3(
            data_frame, target_key, self.trg_args.trg_format)
        self._logger.info('Xetra target data successfully written.')
        # Updating meta file
        MetaProcess.update_meta_file(
            self.meta_update_list, self.meta_key, self.s3_bucket_trg)
        self._logger.info('Xetra meta file successfully updated.')
        return True


    def etl_report1(self):
        """
        Extract, transform and load to create report 1
        """

        # Extrcation
        data_frame = self.extract()
        # Transformation
        data_frame = self.transform_report1(data_frame)
        # Load
        self.load(data_frame)
        return True
