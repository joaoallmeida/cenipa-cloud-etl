from datetime import datetime, timedelta
import polars as pl
import awswrangler as wr
import getpass
import socket

class Utils:

    @staticmethod
    def get_glue_connection(aws_session:any, conn_name:str) -> str:
        try:
            glue = aws_session.client('glue')
            connection = glue.get_connection(Name=conn_name)['Connection']['ConnectionProperties']

            host = connection['JDBC_CONNECTION_URL'].split('/')[1]
            uri = f"mysql://{connection['USERNAME']}:{connection['PASSWORD']}@{host}/dw"
        except Exception as e:
            raise e
        return uri

    @staticmethod
    def str_cols(df:pl.DataFrame, cols:list, str_case:bool=False) -> pl.DataFrame:
        try:
            df = df.to_pandas()
            for col in cols:
                    if str_case:
                        df[col] = df[col].str.replace("***",'Outros').str.title()
                    else:
                        df[col] = (df[col].str.replace(r'\t', '')
                                        .str.replace(r'\\', '')
                                        .str.replace(r'*', '')
                                        .str.normalize('NFKD')
                                        .str.encode('ascii', errors='ignore')
                                        .str.decode('utf-8'))
                    
        except Exception as e:
            raise e
        return pl.from_pandas(df)
    
    @staticmethod
    def str_datetime(df:pl.DataFrame, cols:list) -> pl.DataFrame:
        for col in cols:
            try:
                df = df.with_columns(
                    pl.when( pl.col(col).str.contains('/') == True )
                    .then( pl.col(col).str.to_datetime("%d/%m/%Y", strict=False, time_unit='ns'))
                    .otherwise( pl.col(col).str.to_datetime("%Y-%m-%d", strict=False, time_unit='ns'))
                )
            except Exception as e:
                raise e
        return df
    
    @staticmethod
    def drop_cols(df:pl.DataFrame, cols:list) -> pl.DataFrame:
        try:
            df = df.drop(cols)
        except Exception as e:
            raise e
        return df

    @staticmethod
    def rename_cols(df:pl.DataFrame, cols:dict) -> pl.DataFrame:
        try:
            df = df.rename(cols)
        except Exception as e:
            raise e
        return df
    
    @staticmethod
    def get_changes(df:pl.DataFrame, s3_path:str, aws_session:any):
        try:
            df_target = pl.from_pandas(wr.s3.read_parquet(s3_path, boto3_session=aws_session))
            
            if df.frame_equal(df_target):
                df = df_target    
            else:
                df = df_target.filter(~pl.col('codigo_ocorrencia2').is_in(df.select('codigo_ocorrencia').to_series()))
            return df
            
        except Exception as e:
            raise e
    
    
    @staticmethod
    def update_into_mysql(pk:str, df:pl.DataFrame, table_name:str, conn:any):
        rows = df.to_dicts()
        for data in rows:
            sql01 = ",".join([f"{c} = '{data[c]}'" if isinstance(data[c], str) else f"{c} = {data[c]}" for c in df.columns if c != pk])
            # sql01 = ",".join(f'{c} = %s' for c in df.columns if c != pk) 
            update_statemant = f"UPDATE {table_name} SET {sql01}, atualizado_em='{datetime.now()}', atualizado_por='{getpass.getuser()}@{socket.gethostname()}' WHERE {pk} = {data[pk]}"

            with conn.cursor() as cursor:
                cursor.execute(update_statemant)
            cursor.close()
            conn.commit()