from .logger import Step, Status, extra_fields
from datetime import datetime
from typing import Generator
import polars as pl
import awswrangler as wr
import traceback

class Extract:

    def __init__(self, logger_session:any, aws_session:any, bucket:str) -> None:
        self.logger = logger_session
        self.awsSession = aws_session
        self.bucket = bucket
    
    def get_raw_data(self, tables:list):
        self.logger.info('Start extraction', extra=extra_fields(Step.EXTRACT_INIT, Status.PROCESSING))

        for table in tables:
            self.logger.info(f'Capturing data from table -> {table}', extra=extra_fields(Step.READ,Status.PROCESSING, table))
            try:
                df = self.read_csv_with_encoding(f'data/{table}.csv', ['utf-8', 'latin-1'])
                self.logger.info(f'Complete data capture from table -> {table}', extra=extra_fields(Step.READ,Status.COMPLETED, table, len(df.rows())))
                self.__upload_raw_data(df, table)
            except Exception as e:
                self.logger.error(f'Failure to read data from table {table}: {traceback.format_exc()}', extra=extra_fields(Step.READ, Status.FAILURE, table))
                raise e
            
        self.logger.info('Completed extraction', extra=extra_fields(Step.EXTRACT_END, Status.COMPLETED))
    
    def read_csv_with_encoding(self, file_path: str, encodings: list):
        for encoding in encodings:
            try:
                df = pl.read_csv(file_path, encoding=encoding, separator=';', ignore_errors=True)
                return df
            except UnicodeDecodeError:
                pass
            except Exception as e:
                raise e
        raise Exception(f'Failed to read CSV file: {file_path} with available encodings')


    def __upload_raw_data(self, df:pl.DataFrame, table_name:str) -> None:

        dt_str = datetime.now().strftime('%Y%m%d%H')
        path = f"s3://{self.bucket}/raw/{table_name}/{table_name}_{dt_str}.parquet"

        self.logger.info(f'Uploading raw data to s3: {path}', extra=extra_fields(Step.UPLOAD, Status.PROCESSING, table_name))

        try:
            wr.s3.to_parquet(
                df=df.to_pandas(),
                path=path,
                boto3_session=self.awsSession
            )
        except Exception as e:
            self.logger.error(f'Failed to upload raw data to s3 {table_name}: {traceback.format_exc()}', extra=extra_fields(Step.UPLOAD, Status.FAILURE, table_name, len(df.rows())))
            raise e
        
        self.logger.info(f'Completed upload raw data to s3.', extra=extra_fields(Step.UPLOAD, Status.COMPLETED, table_name, len(df.rows())))