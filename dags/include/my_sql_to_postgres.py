def My_SQL_to_Postgres(**kwargs):
    # os.chdir('include')
    
    # load_dotenv('enviroment_variables.env')
    from datetime import datetime, timedelta
    import time
    from os import environ
    from base64 import b64decode
    from datetime import datetime
    from logging import getLogger, INFO, WARN
    from os import environ
    from sys import exit as sys_exit
    from os import environ
    from pandas import read_sql_table
    from sqlalchemy import create_engine, types
    from sqlalchemy.exc import SQLAlchemyError
    from psycopg2.extensions import register_adapter
    from psycopg2.extras import Json
    from psycopg2 import connect
    import logging
    from dotenv import load_dotenv
    import os
    import requests
    from pandas import read_sql
     
    pg_host =  os.getenv('PG_HOST_STAGING')
    pg_user = os.getenv('PG_USERNAME_WRITE_STAGING')
    pg_password = os.getenv('PG_PASSWORD_WRITE_STAGING')
    pg_database = os.getenv('PG_DATABASE')
    pg_schema = kwargs['pg_schema']  
    pg_tables_to_use = kwargs['pg_tables_to_use']
    delta_load = kwargs['delta_load']
    pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)
    chunk_size = 1000 

    mysql_host =  os.getenv('MYSQL_HOST')
    mysql_port =  os.getenv('MYSQL_PORT')
    mysql_user = os.getenv('MYSQL_USERNAME')
    mysql_password = os.getenv('MYSQL_PASSWORD')
    mysql_schema = kwargs['mysql_schema'] 
    mysql_tables_to_copy = kwargs['mysql_tables_to_copy']
    chunksize_to_use = kwargs['chunksize_to_use']
    mysql_connect_string = f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_schema}"
    mysql_engine = create_engine(f"{mysql_connect_string}", echo=False)

   
    if delta_load == False:
        df = read_sql_table(    mysql_tables_to_copy,
                                con=mysql_engine,
                               chunksize=chunksize_to_use
                            )
        pg_conn_args = dict(
                                host=pg_host,
                                user=pg_user,
                                password=pg_password,
                                database=pg_database,
                            )
        connection = connect(**pg_conn_args)
        cur = connection.cursor()
       
        cur.execute(f"DROP TABLE if exists {pg_schema}.{pg_tables_to_use} ;")
        connection.commit()
        print("Table {}.{}, emptied before adding updated data.".format(pg_schema, pg_tables_to_use))
    else :
         df = read_sql("""select * from {}.{} 
                        where   created_at >= current_date - INTERVAL DAYOFWEEK(current_date)+6 DAY
                            AND created_at < curdate() - INTERVAL DAYOFWEEK(curdate())-1 DAY""".format(mysql_schema,mysql_tables_to_copy),
                      con=mysql_engine
                    , chunksize=chunksize_to_use)
        

    

    for i, df_chunk in enumerate(df):
        print(i, df_chunk.shape)
        if not df_chunk.empty:
            # TODO fix this and make dtype flexible ( dict(column,table))
            # col_dtype = {dtype_column: types.JSON} if pg_table == dtype_table else None
            df_chunk["_updated_at"] = datetime.now()
            df_chunk.to_sql(pg_tables_to_use,
                            dtype={'raw_values': types.JSON,
                                   'data': types.JSON},
                            con=pg_engine,
                            chunksize=chunksize_to_use,
                            if_exists="append",
                            method="multi",
                            schema=pg_schema,
                            index=False
                            )
