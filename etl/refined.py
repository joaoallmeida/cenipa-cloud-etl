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

    def __upload_refined_data(self, df:pl.DataFrame, table_name:str, dt_ref:str=None) -> None:

        dt_str = datetime.now().strftime('%Y%m%d') if dt_ref is None else dt_ref
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

    def refine_data(self, tables:list, dt_ref:str=None):
        self.logger.info('Start refined', extra=extra_fields(Step.REFINED_INIT, Status.PROCESSING))

        refined_event = json.loads(open('events/refined_event.json','r').read())

        for table in tables:
            try:
                self.logger.info(f'Refining data -> {table}', extra=extra_fields(Step.REFINE,Status.PROCESSING, table))
                
                dt_str = datetime.now().strftime('%Y%m%d') if dt_ref is None else dt_ref
                path = f"s3://{self.bucket}/raw/{table}/{table}_{dt_str}.parquet"
                
                df = pl.from_pandas(wr.s3.read_parquet( path=path, boto3_session=self.awsSession ))
                df = Utils.str_cols(df, refined_event[table]['columns_str'], True)
                df = Utils.str_cols(df, refined_event[table]['columns_str_special'])
                df = Utils.str_datetime(df, refined_event[table]['columns_date'])
                df = Utils.drop_cols(df, refined_event[table]['columns_drop'])
                df = Utils.rename_cols(df, refined_event[table]['columns_rename'])

                self.logger.info(f'completed data refinement table -> {table}', extra=extra_fields(Step.REFINE,Status.COMPLETED, table, len(df.rows())))
                self.__upload_refined_data(df, table, dt_ref)
                
            except Exception as e:
                self.logger.error(f'Failure to refined table {table}: {e}', extra=extra_fields(Step.REFINE,Status.FAILURE, table))
                raise e

        self.logger.info('Completed refined', extra=extra_fields(Step.REFINED_END, Status.COMPLETED))


