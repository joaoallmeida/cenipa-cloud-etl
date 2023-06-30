import json
import polars as pl
import awswrangler as wr
import traceback

from .logger import Step, Status, extra_fields
from .utils import Utils
from datetime import datetime


class Refined:

    def __init__(self, logger_session:any, aws_session:any, bucket:str) -> None:
        self.logger = logger_session
        self.awsSession = aws_session
        self.bucket = bucket

    def refine_data(self, tables:list, dt_ref:str=None):
        self.logger.info('Start refined', extra=extra_fields(Step.REFINED_INIT, Status.PROCESSING))

        refined_event = json.loads(open('tests/refined_event.json','r').read())

        for table in tables:
            try:
                self.logger.info(f'Refining data -> {table}', extra=extra_fields(Step.REFINE,Status.PROCESSING, table))
                
                df = self.__read_raw_file(table, dt_ref)
                df = Utils.str_title_case(df, refined_event[table]['columns_str'])
                df = Utils.str_datetime(df, refined_event[table]['columns_date'])

                self.logger.info(f'completed data refinement table -> {table}', extra=extra_fields(Step.REFINE,Status.COMPLETED, table, len(df.rows())))
                self.__upload_refined_data(df, table, dt_ref)
            except Exception as e:
                self.logger.error(f'Failure to refined table {table}: {e}', extra=extra_fields(Step.REFINE,Status.FAILURE, table))
                raise e

        self.logger.info('Completed refined', extra=extra_fields(Step.REFINED_END, Status.COMPLETED))

    def __read_raw_file(self, table_name:str, dt_ref:str=None) -> pl.DataFrame:
        try:
            dt_str = datetime.now().strftime('%Y%m%d%H') if dt_ref is None else dt_ref
            path = f"s3://{self.bucket}/raw/{table_name}/{table_name}_{dt_str}.parquet"

            df = wr.s3.read_parquet(
                path=path,
                boto3_session=self.awsSession
            )
        except Exception as e:
            raise e
        return pl.from_pandas(df)

    def __upload_refined_data(self, df:pl.DataFrame, table_name:str, dt_ref:str=None) -> None:

        dt_str = datetime.now().strftime('%Y%m%d%H') if dt_ref is None else dt_ref
        path = f"s3://{self.bucket}/refined/{table_name}/{table_name}_{dt_str}.parquet"

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
