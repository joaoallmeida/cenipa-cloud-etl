from .logger import Step, Status, extra_fields
from .utils import Utils
from datetime import datetime, timedelta
import awswrangler as wr
import polars as pl
import getpass
import socket
import polars as pl
import awswrangler as wr
import traceback
import pymysql

class Load:

    def __init__(self, logger_session:any, aws_session:any, bucket:str) -> None:
        self.logger = logger_session
        self.aws_session = aws_session
        self.bucket = bucket

        self.conn = wr.mysql.connect('rds_mysql', boto3_session=self.aws_session)
        self.uri = Utils.get_glue_connection(self.aws_session, 'rds_mysql')

        pymysql.install_as_MySQLdb()

    def check_new_rows_and_changes(self, source:pl.DataFrame, table_name:str, dt_ref:str=None):

        self.logger.info(f'Reading table target: {table_name}.', extra=extra_fields(Step.READ, Status.PROCESSING, table=table_name))

        try:
            target = pl.read_database(f'SELECT * FROM {table_name}', self.uri).drop(['criado_em','criado_por','atualizado_em','atualizado_por'])
            insert = source.filter(~source.to_series().is_in(target.to_series()))

            if insert.is_empty():
                self.logger.info(f'Not found new rows to table {table_name}!', extra=extra_fields(Step.INSERT, Status.COMPLETED, table=table_name))
            else:
                self.logger.info(f'Found news rows to table {table_name}: {len(insert.rows())}', extra=extra_fields(Step.INSERT, Status.PROCESSING, table=table_name, lines=len(insert.rows())))
                insert = insert.with_columns(
                                    pl.lit(datetime.now()).alias('criado_em'),
                                    pl.lit(f'{getpass.getuser()}@{socket.gethostname()}').alias('criado_por'),
                                    pl.lit(datetime.now()).alias('atualizado_em'),
                                    pl.lit(f'{getpass.getuser()}@{socket.gethostname()}').alias('atualizado_por')
                                )
                insert.write_database( table_name=f'{table_name}', connection_uri=self.uri, if_exists='append', engine='sqlalchemy' )
                self.logger.info(f'Completed insert new rows into table {table_name}.', extra=extra_fields(Step.INSERT, Status.COMPLETED, table=table_name, lines=len(insert.rows())))

                target = pl.read_database(f'SELECT * FROM {table_name}', self.uri).drop(['criado_em','criado_por','atualizado_em','atualizado_por'])

            diff = source == target
            changes = source.filter(~pl.all(diff))

            if changes.is_empty():
                self.logger.info(f'Not found changes to table {table_name}!',  extra=extra_fields(Step.UPDATE, Status.COMPLETED))
            else:
                self.logger.info(f'Found changes to table {table_name}: {len(changes.rows())}', extra=extra_fields(Step.UPDATE, Status.PROCESSING, lines=len(changes.rows())))
                Utils.update_into_mysql('codigo_ocorrencia', changes, table_name, self.conn)
                self.logger.info(f'Completed updated table {table_name}.', extra=extra_fields(Step.UPDATE, Status.PROCESSING, lines=len(changes.rows())))

        except Exception as e:
            raise e
    
    def create_ddl(self):

        self.logger.info('Executing DDL DW Tables', extra=extra_fields(Step.CREATE, Status.PROCESSING))

        try:
            with open('etl/SQL/DDL.sql','r') as file:
                query_statemant = file.read().split(';')
                with self.conn.cursor() as cursor:
                    for query in query_statemant:
                        if len(query) >0:
                            cursor.execute(query)
                cursor.close()
                self.conn.commit()
                
        except Exception as e:
            self.logger.error(f'Failure to create DDL Tables: {traceback.format_exc()}', extra=extra_fields(Step.CREATE, Status.FAILURE))
            self.conn.rollback()
            raise e
        
        self.logger.info('Completed DDL DW Tables', extra=extra_fields(Step.CREATE, Status.COMPLETED))
        

    def create_starschema(self, table_name:str, dt_ref:str=None):

        self.logger.info(f'Creating {table_name.title()}.',  extra=extra_fields(Step.CREATE, Status.PROCESSING, table_name.title()))

        try:

            dt_str = datetime.now().strftime('%Y%m%d') if dt_ref is None else dt_ref
            folder_s3 = f"s3://{self.bucket}/refined/{table_name[4:]}/{table_name[4:]}_{dt_str}.parquet"

            df = pl.from_pandas(wr.s3.read_parquet( path=folder_s3, boto3_session=self.aws_session ))   
            self.check_new_rows_and_changes(df, table_name, dt_ref)

            self.logger.info(f'Complete creation table {table_name.title()}.', extra=extra_fields(Step.CREATE, Status.COMPLETED, table_name.title(), len(df.rows())))

        except Exception as e:
            self.logger.error(f'Erro to create table {table_name} into MySQL: {e}', extra=extra_fields(Step.CREATE, Status.FAILURE, table_name.title()))
            raise e

    def load_starschema_model(self, tables:list, dt_ref:str=None, create_ddl:bool=False):

        self.logger.info("Start Load Data.", extra=extra_fields(Step.LOAD_INIT, Status.PROCESSING))

        if create_ddl:
            self.create_ddl()

        for table in tables:
            self.create_starschema(table, dt_ref)

        self.logger.info("Complete Load Data.", extra=extra_fields(Step.LOAD_END, Status.COMPLETED))
