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
import configparser

class Load:

    def __init__(self, logger_session:any, aws_session:any, bucket:str) -> None:
        self.logger = logger_session
        self.aws_session = aws_session
        self.bucket = bucket

        config = configparser.ConfigParser()
        config.read('config/credentials.ini')
        self.url_conn = config['AWS']['mysql_url']


    def create_dim_aeronave(self, dt_ref:str=None):

        self.logger.info('Creating Dim Aeronave.',  extra=extra_fields(Step.CREATE, Status.PROCESSING, table='Dim Aeronave'))

        dt_str = datetime.now().strftime('%Y%m%d') if dt_ref is None else dt_ref
        dt_str_yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d') if dt_ref is None else dt_ref

        path_aeronave = f"s3://cenipa.etl.com.br/refined/aeronave/aeronave_{dt_str}.parquet"
        path_aeronave_yesterday = f"s3://cenipa.etl.com.br/refined/aeronave/aeronave_{dt_str_yesterday}.parquet"
        
        dim_aeronave_df = pl.from_pandas(wr.s3.read_parquet( path=path_aeronave, boto3_session=self.aws_session ))
        df_target = pl.from_pandas(wr.s3.read_parquet(path_aeronave_yesterday, boto3_session=self.aws_session))
            
        if dim_aeronave_df.frame_equal(df_target):
            self.logger.info('Not found changes in aeronave data.', extra=extra_fields(Step.CREATE, Status.COMPLETED, table='Dim Aeronave', lines=len(dim_aeronave_df.rows())))
        else:     
            self.logger.info('Insert data into MySQL table.', extra=extra_fields(Step.INSERT, Status.COMPLETED, table='Dim Aeronave', lines=len(dim_aeronave_df.rows())))
            
            dim_aeronave_df = dim_aeronave_df.filter(~pl.col('codigo_ocorrencia').is_in(df_target.select('codigo_ocorrencia').to_series()))
            dim_aeronave_df = dim_aeronave_df.with_columns(
                                                pl.lit(datetime.now()).alias('criado_em'),
                                                pl.lit(f'{getpass.getuser()}@{socket.gethostname()}').alias('criado_por')    
                                            )
            
            dim_aeronave_df.write_database(table_name='dim_aeronave', connection_uri=self.url_conn, if_exists='append', engine='sqlalchemy' )
            
            self.logger.info('Complete insert into MySQL into table Dim Aeronave.', extra=extra_fields(Step.INSERT, Status.COMPLETED, table='Dim Aeronave', lines=len(dim_aeronave_df.rows())))
        
        self.logger.info('Complete creation table Dim Aeronave.', extra=extra_fields(Step.CREATE, Status.COMPLETED, table='Dim Aeronave', lines=len(dim_aeronave_df.rows())))
        

    def create_dim_ocorrencia_tipo(self, dt_ref:str=None):
        self.logger.info('Creating Dim Ocorrencia Tipo.',  extra=extra_fields(Step.CREATE, Status.PROCESSING, table='Dim Aeronave'))
        
        dt_str = datetime.now().strftime('%Y%m%d') if dt_ref is None else dt_ref
        dt_str_yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d') if dt_ref is None else dt_ref
        path_ocorrencia_tipo = f"s3://cenipa.etl.com.br/refined/ocorrencia_tipo/ocorrencia_tipo_{dt_str}.parquet"
        path_ocorrencia_tipo_yesterday = f"s3://cenipa.etl.com.br/refined/ocorrencia_tipo/ocorrencia_tipo_{dt_str_yesterday}.parquet"

        dim_ocorrencia_tipo_df = pl.from_pandas(wr.s3.read_parquet( path=path_ocorrencia_tipo, boto3_session=self.aws_session ))
        df_target = pl.from_pandas(wr.s3.read_parquet(path_ocorrencia_tipo_yesterday, boto3_session=self.aws_session))
            
        if dim_ocorrencia_tipo_df.frame_equal(df_target):
            self.logger.info('Not found changes in ocorrencia_tipo data.', extra=extra_fields(Step.CREATE, Status.COMPLETED, table='Dim Ocorrencia Tipo', lines=len(dim_ocorrencia_tipo_df.rows())))
        else:
            dim_ocorrencia_tipo_df = dim_ocorrencia_tipo_df.filter(~pl.col('codigo_ocorrencia').is_in(df_target.select('codigo_ocorrencia').to_series()))
            dim_ocorrencia_tipo_df = dim_ocorrencia_tipo_df.with_columns(
                                                pl.lit(datetime.now()).alias('criado_em'),
                                                pl.lit(f'{getpass.getuser()}@{socket.gethostname()}').alias('criado_por')    
                                            )

            dim_ocorrencia_tipo_df.write_database(table_name='dim_ocorrencia_tipo', connection_uri=self.url_conn, if_exists='replace', engine='sqlalchemy' )
            self.logger.info('Complete insert into MySQL into table Dim Ocorrencia Tipo.', extra=extra_fields(Step.INSERT, Status.COMPLETED, table='Dim Ocorrencia Tipo', lines=len(dim_ocorrencia_tipo_df.rows())))

        self.logger.info('Complete creation table Dim Ocorrencia Tipo.', extra=extra_fields(Step.CREATE, Status.COMPLETED, table='Dim Ocorrencia Tipo', lines=len(dim_ocorrencia_tipo_df.rows())))

    def create_dim_recomendacao(self, dt_ref:str=None):

        self.logger.info('Creating Dim Recomendacao.',  extra=extra_fields(Step.CREATE, Status.PROCESSING, table='Dim Recomendacao'))
        
        dt_str = datetime.now().strftime('%Y%m%d') if dt_ref is None else dt_ref
        dt_str_yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d') if dt_ref is None else dt_ref
        path_recomendacao = f"s3://cenipa.etl.com.br/refined/recomendacao/recomendacao_{dt_str}.parquet"
        path_recomendacao_yesterday = f"s3://cenipa.etl.com.br/refined/recomendacao/recomendacao_{dt_str_yesterday}.parquet"
        
        dim_recomendacao_df = pl.from_pandas(wr.s3.read_parquet( path=path_recomendacao, boto3_session=self.aws_session ))
        df_target = pl.from_pandas(wr.s3.read_parquet(path_recomendacao_yesterday, boto3_session=self.aws_session))
        
        if dim_recomendacao_df.frame_equal(df_target):
            self.logger.info('Not found changes in ocorrencia_tipo data.', extra=extra_fields(Step.CREATE, Status.COMPLETED, table='Dim Ocorrencia Tipo', lines=len(dim_recomendacao_df.rows())))
        else:
            dim_recomendacao_df = dim_recomendacao_df.filter(~pl.col('codigo_ocorrencia').is_in(df_target.select('codigo_ocorrencia').to_series()))
            dim_recomendacao_df = dim_recomendacao_df.with_columns(
                                                pl.lit(datetime.now()).alias('criado_em'),
                                                pl.lit(f'{getpass.getuser()}@{socket.gethostname()}').alias('criado_por')    
                                            )

            self.logger.info('Complete insert into MySQL into table Dim Recomendacao.', extra=extra_fields(Step.INSERT, Status.COMPLETED, table='Dim Recomendacao', lines=len(dim_recomendacao_df.rows())))
            dim_recomendacao_df.write_database(table_name='dim_recomendacao', connection_uri=self.url_conn, if_exists='replace', engine='sqlalchemy' )

        self.logger.info('Complete creation table Dim Recomendacao.', extra=extra_fields(Step.CREATE, Status.COMPLETED, table='Dim Recomendacao', lines=len(dim_recomendacao_df.rows())))

    def create_fat_ocorrencia(self, dt_ref:str=None):
        
        self.logger.info('Creating Fat Ocorrencia.',  extra=extra_fields(Step.CREATE, Status.PROCESSING, table='Fat Ocorrencia'))
        
        dt_str = datetime.now().strftime('%Y%m%d') if dt_ref is None else dt_ref
        dt_str_yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d') if dt_ref is None else dt_ref
        path_ocorrencia = f"s3://cenipa.etl.com.br/refined/ocorrencia/ocorrencia_{dt_str}.parquet"
        path_ocorrencia_yesterday = f"s3://cenipa.etl.com.br/refined/ocorrencia/ocorrencia_{dt_str_yesterday}.parquet"
        
        fat_ocorrencia_df = pl.from_pandas(wr.s3.read_parquet( path=path_ocorrencia, boto3_session=self.aws_session ))
        df_target = pl.from_pandas(wr.s3.read_parquet(path_ocorrencia_yesterday, boto3_session=self.aws_session))
        
        if fat_ocorrencia_df.frame_equal(df_target):
            self.logger.info('Not found changes in ocorrencia_tipo data.', extra=extra_fields(Step.CREATE, Status.COMPLETED, table='Dim Ocorrencia Tipo', lines=len(fat_ocorrencia_df.rows())))
        else:
            fat_ocorrencia_df = fat_ocorrencia_df.filter(~pl.col('codigo_ocorrencia').is_in(df_target.select('codigo_ocorrencia').to_series()))
            fat_ocorrencia_df = fat_ocorrencia_df.with_columns(
                                                pl.lit(datetime.now()).alias('criado_em'),
                                                pl.lit(f'{getpass.getuser()}@{socket.gethostname()}').alias('criado_por')    
                                            )
            
            fat_ocorrencia_df.write_database(table_name='fat_ocorrencia', connection_uri=self.url_conn, if_exists='replace', engine='sqlalchemy' )
            self.logger.info('Complete insert into MySQL into table Fat Ocorrencia.', extra=extra_fields(Step.INSERT, Status.COMPLETED, table='Fat Ocorrencia', lines=len(fat_ocorrencia_df.rows())))
        
        self.logger.info('Complete creation table Fat Ocorrencia.', extra=extra_fields(Step.CREATE, Status.COMPLETED, table='Fat Ocorrencia', lines=len(fat_ocorrencia_df.rows())))

    def load_starschema_model(self, tables:list, dt_ref:str=None):

        self.logger.info("Start Load Data.", extra=extra_fields(Step.LOAD_INIT, Status.PROCESSING))

        for table in tables:

            if table == 'fat_ocorrencia':
                self.create_fat_ocorrencia(dt_ref)
            elif table == 'dim_ocorrencia_tipo':
                self.create_dim_ocorrencia_tipo(dt_ref)
            elif table == 'dim_aeronave':
                self.create_dim_aeronave(dt_ref)
            elif table == 'dim_recomendacao':
                self.create_dim_recomendacao(dt_ref)
            else:
                self.logger.warning(f'Not fount table: {table}', extra=extra_fields(Step.CREATE, Status.ERROR))

        self.logger.info("Complete Load Data.", extra=extra_fields(Step.LOAD_END, Status.COMPLETED))