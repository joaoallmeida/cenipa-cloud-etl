from datetime import datetime, timedelta
import polars as pl
import awswrangler as wr
import getpass
import socket

class Utils:

    @staticmethod
    def str_cols(df:pl.DataType, cols:list) -> pl.DataFrame:
        for col in cols:
            try:
                df = df.with_columns(
                    pl.col(col).str.replace(r"[***]",'Outros').str.to_titlecase()
                )
            except Exception as e:
                raise e
        return df
    
    @staticmethod
    def str_datetime(df:pl.DataType, cols:list) -> pl.DataFrame:
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
    def drop_cols(df:pl.DataType, cols:list) -> pl.DataFrame:
        try:
            df = df.drop(cols)
        except Exception as e:
            raise e
        return df

    @staticmethod
    def rename_cols(df:pl.DataType, cols:dict) -> pl.DataFrame:
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