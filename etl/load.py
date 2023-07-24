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

        # dt_str = (datetime.now() - timedelta(1)).strftime('%Y%m%d') if dt_ref is None else dt_ref
        # s3_path = f"s3://{self.bucket}/refined/{table_name}/{table_name}_{dt_str}.parquet"

        self.logger.info(f'Reading table target: {table_name}.', extra=extra_fields(Step.READ, Status.PROCESSING, table=table_name))

        try:
            # target = pl.from_pandas(wr.s3.read_parquet( path=s3_path, boto3_session=aws_session ))
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
                # target = target.extend(insert)

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
        
    def create_dim_aeronave(self, dt_ref:str=None):

        self.logger.info('Creating Dim Aeronave.',  extra=extra_fields(Step.CREATE, Status.PROCESSING, table='Dim Aeronave'))

        dt_str = datetime.now().strftime('%Y%m%d') if dt_ref is None else dt_ref
        path_aeronave = f"s3://{self.bucket}/refined/aeronave/aeronave_{dt_str}.parquet"

        dim_aeronave_df = pl.from_pandas(wr.s3.read_parquet( path=path_aeronave, boto3_session=self.aws_session ))
        self.check_new_rows_and_changes(dim_aeronave_df, 'dim_aeronave', dt_ref)

        self.logger.info('Complete creation table Dim Aeronave.', extra=extra_fields(Step.CREATE, Status.COMPLETED, table='Dim Aeronave'))

    def create_dim_ocorrencia_tipo(self, dt_ref:str=None):
        self.logger.info('Creating Dim Ocorrencia Tipo.',  extra=extra_fields(Step.CREATE, Status.PROCESSING, table='Dim Aeronave'))
        
        dt_str = datetime.now().strftime('%Y%m%d') if dt_ref is None else dt_ref
        path_ocorrencia_tipo = f"s3://{self.bucket}/refined/ocorrencia_tipo/ocorrencia_tipo_{dt_str}.parquet"

        dim_ocorrencia_tipo_df = pl.from_pandas(wr.s3.read_parquet( path=path_ocorrencia_tipo, boto3_session=self.aws_session ))      
        self.check_new_rows_and_changes(dim_ocorrencia_tipo_df, 'dim_ocorrencia_tipo', dt_ref)

        self.logger.info('Complete creation table Dim Ocorrencia Tipo.', extra=extra_fields(Step.CREATE, Status.COMPLETED, table='Dim Ocorrencia Tipo'))

    def create_dim_recomendacao(self, dt_ref:str=None):

        self.logger.info('Creating Dim Recomendacao.',  extra=extra_fields(Step.CREATE, Status.PROCESSING, table='Dim Recomendacao'))
        
        dt_str = datetime.now().strftime('%Y%m%d') if dt_ref is None else dt_ref
        path_recomendacao = f"s3://{self.bucket}/refined/recomendacao/recomendacao_{dt_str}.parquet"

        dim_recomendacao_df = pl.from_pandas(wr.s3.read_parquet( path=path_recomendacao, boto3_session=self.aws_session ))
        self.check_new_rows_and_changes(dim_recomendacao_df, 'dim_recomendacao', dt_ref)

        self.logger.info('Complete creation table Dim Recomendacao.', extra=extra_fields(Step.CREATE, Status.COMPLETED, table='Dim Recomendacao'))

    def create_dim_fator_contribuinte(self, dt_ref:str=None):

        self.logger.info('Creating Dim Fator Contribuinte.',  extra=extra_fields(Step.CREATE, Status.PROCESSING, table='Dim Fator Contribuinte'))
        
        dt_str = datetime.now().strftime('%Y%m%d') if dt_ref is None else dt_ref
        path_fator_contribuinte = f"s3://{self.bucket}/refined/fator_contribuinte/fator_contribuinte_{dt_str}.parquet"

        dim_fator_contribuinte_df = pl.from_pandas(wr.s3.read_parquet( path=path_fator_contribuinte, boto3_session=self.aws_session ))
        self.check_new_rows_and_changes(dim_fator_contribuinte_df, 'dim_fator_contribuinte', dt_ref)

        self.logger.info('Complete creation table Dim Fator Contribuinte.', extra=extra_fields(Step.CREATE, Status.COMPLETED, table='Dim Fator Contribuinte'))

    def create_fat_ocorrencia(self, dt_ref:str=None):
        
        self.logger.info('Creating Fat Ocorrencia.',  extra=extra_fields(Step.CREATE, Status.PROCESSING, table='Fat Ocorrencia'))
        
        dt_str = datetime.now().strftime('%Y%m%d') if dt_ref is None else dt_ref
        path_ocorrencia = f"s3://{self.bucket}/refined/ocorrencia/ocorrencia_{dt_str}.parquet"

        fat_ocorrencia_df = pl.from_pandas(wr.s3.read_parquet( path=path_ocorrencia, boto3_session=self.aws_session ))   
        self.check_new_rows_and_changes(fat_ocorrencia_df, 'fat_ocorrencia', dt_ref)

        self.logger.info('Complete creation table Fat Ocorrencia.', extra=extra_fields(Step.CREATE, Status.COMPLETED, table='Fat Ocorrencia'))

    def load_starschema_model(self, tables:list, dt_ref:str=None, create_ddl:bool=False):

        self.logger.info("Start Load Data.", extra=extra_fields(Step.LOAD_INIT, Status.PROCESSING))

        if create_ddl:
            self.create_ddl()

        for table in tables:

            if table == 'fat_ocorrencia':
                self.create_fat_ocorrencia(dt_ref)
            elif table == 'dim_ocorrencia_tipo':
                self.create_dim_ocorrencia_tipo(dt_ref)
            elif table == 'dim_aeronave':
                self.create_dim_aeronave(dt_ref)
            elif table == 'dim_recomendacao':
                self.create_dim_recomendacao(dt_ref)
            elif table == 'dim_fator_contribuinte':
                self.create_dim_fator_contribuinte(dt_ref) 
            else:
                self.logger.warning(f'Not fount table: {table}', extra=extra_fields(Step.CREATE, Status.ERROR))

        self.logger.info("Complete Load Data.", extra=extra_fields(Step.LOAD_END, Status.COMPLETED))