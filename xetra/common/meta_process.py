"""
Methods for processing the meta file
"""

import collections
from dataclasses import dataclass
from datetime import datetime
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongFormatException, WrongMetaFileException
from xetra.common.s3 import S3BucketConnector
import pandas as pd

class MetaProcess():
    """
    Class for working with the meta file
    """
    
    @staticmethod
    def update_meta_file(extract_date_list: list, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """Updating the meta file with the processed Xetra dates and todays date as procesed date

        Params:
            extract_date_list (list): list of dates that are extracted from the source
            meta_key (str): key of the meta file on the S3 bucket
            s3_bucket_meta (S3BucketConnector): S3BucketConnector for the bucket with the meta file
        """
        #Creating an empty DataFrame using the meta file column names
        df_new = pd.DataFrame(columns = [
            MetaProcessFormat.META_SOURCE_DATE_COL.value,
            MetaProcessFormat.META_PROCESS_COL.value])
        # Filling the date column with the extract_date_list
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        # Filling the processed column
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        try:
            # If meta file exists -> union DataFrame of old and new meta data created
            df_old = s3_bucket_meta.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all = pd.concat([df_old, df_new])
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # No meta file exists -> only the new data is used
            df_all = df_new
        # Writing to S3
        s3_bucket_meta.write_df_to_s3(df_all, meta_key, MetaProcessFormat.META_FILE_FORMAT.value)
        return True
    
    @staticmethod
    def return_date_list():
        pass