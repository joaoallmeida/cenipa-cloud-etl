import polars as pl

class Utils:

    @staticmethod
    def str_title_case(df:pl.DataType, cols:list) -> pl.DataFrame:
        for col in cols:
            try:
                df = df.with_columns(
                    pl.col(col).str.to_titlecase()
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
    def get_changes(self,df,table,dbconn):

        try:
            
            df_target = pl.read_database .read_sql_table(table, dbconn)
            changes = df[~df.apply(tuple,axis=1).isin(df_target.apply(tuple,axis=1))]
            insert = changes[~changes['id'].isin(df_target['id'])]
            # modified = changes[changes['id'].isin(df_target['id'])]
            
        except Exception as e:
            raise e
        
        return insert