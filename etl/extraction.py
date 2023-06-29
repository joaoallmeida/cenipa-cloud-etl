from .logger import Step, Status, extra_fields
from datetime import datetime
import polars as pl
import awswrangler as wr
import traceback

class Extract:

    def __init__(self, logger_session:any, aws_session:any) -> None:
        self.logger = logger_session
        self.awsSession = aws_session
    
    def get_raw_data(self, bucket:str ,tables:list):
        self.logger.info('Start extraction', extra=extra_fields(Step.EXTRACT_INIT, Status.PROCESSING))

        for table in tables:
            self.logger.info(f'Capturing data from table -> {table}', extra=extra_fields(Step.READ,Status.PROCESSING, table))
            try:
                df = pl.read_csv(f'data/{table}.csv', encoding='utf-8', separator=';', ignore_errors=True)
            except UnicodeDecodeError:
                df = pl.read_csv(f'data/{table}.csv', encoding='latin-1', separator=';', ignore_errors=True)
            except Exception as e:
                self.logger.error(f'Failure to read data from table {table}: {traceback.format_exc()}', extra=extra_fields(Step.READ, Status.FAILURE, table))
                raise e
        
            self.logger.info(f'Complete data capture from table -> {table}', extra=extra_fields(Step.READ,Status.COMPLETED, table, len(df.rows())))
            self.upload_raw_data(df, bucket, table)

            yield df
            
        self.logger.info('Completed extraction', extra=extra_fields(Step.EXTRACT_END, Status.COMPLETED))

    def upload_raw_data(self, df:pl.DataFrame, bucket:str, table_name:str) -> None:

        dt_str = datetime.now().strftime('%Y%m%d%H%M')
        path = f"s3://{bucket}/raw/{table_name}/{table_name}_{dt_str}.parquet"

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