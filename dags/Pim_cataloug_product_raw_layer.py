from datetime import datetime, timedelta
import airflow
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator



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
logging.getLogger().setLevel(logging.INFO)

def My_SQL_to_Postgres(**kwargs):
    os.chdir('include')
    
    load_dotenv('enviroment_variables.env')
    pg_host =  os.getenv('PG_HOST')
    pg_database = os.getenv('PG_DATABASE')
    pg_user = os.getenv('PG_USERNAME_WRITE')
    pg_password = os.getenv('PG_PASSWORD_WRITE')
    pg_schema = kwargs['pg_schema']  
    
    pg_tables_to_use = kwargs['pg_tables_to_use']

    pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)
    chunk_size = 1000 

    mysql_host =  os.getenv('MYSQL_HOST')
    mysql_port =  os.getenv('MYSQL_PORT')
    mysql_user = os.getenv('MYSQL_USERNAME')
    mysql_password = os.getenv('MYSQL_PASSWORD')
    mysql_schema = kwargs['mysql_schema'] 
    mysql_tables_to_copy = kwargs['mysql_tables_to_copy']
    mysql_connect_string = f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_schema}"
    mysql_engine = create_engine(f"{mysql_connect_string}", echo=False)

   

    df = read_sql_table(mysql_tables_to_copy,
                        con=mysql_engine, chunksize=10000)

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

    
    #logging.getLogger().setLevel(logging.INFO)
    print("Table {}.{}, emptied before adding updated data.".format(pg_schema, pg_tables_to_use))



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
                            chunksize=10000,
                            if_exists="append",
                            method="multi",
                            schema=pg_schema
                            )


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


def dbt_run():
    myToken = os.getenv('dbt_token')
    myUrl = 'https://cloud.getdbt.com/api/v2/accounts/1335/jobs/2833/run/'

    #string  = {'Authorization': 'token {}'.format(myToken),'cause' :'Kick Off From Testing Script'}
    head = {'Authorization': 'token {}'.format(myToken)}
    body = {'cause': 'Kick Off From Pim_cataloug_product_raw_layer Script'}
    r = requests.post(myUrl, headers=head, data=body)
    r_dictionary = r.json()
    print(r.text)


with DAG(
    dag_id="Pim_cataloug_product_raw_layer",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 */23 * * *",
    concurrency=100
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    data_dog_log = DummyOperator(task_id='data_dog_log', retries=3)
    copy_PIM_CATALOUG_PRODUCT_from_mySQL = PythonOperator(task_id='copy_PIM_CATALOUG_PRODUCT_from_mySQL', python_callable=My_SQL_to_Postgres,
                                                          op_kwargs={'pg_schema': 'junk_tables'
                                                                    , 'pg_tables_to_use': 'cp_pim_catalog_product'
                                                                    , 'mysql_tables_to_copy': 'pim_catalog_product'
                                                                    , 'mysql_schema': 'akeneo'}, retries=5)
    copy_PIM_CATALOUG_PRODUCT_model_from_mySQL = PythonOperator(task_id='copy_PIM_CATALOUG_PRODUCT_model_from_mySQL', python_callable=My_SQL_to_Postgres,
                                                          op_kwargs={'pg_schema': 'junk_tables'
                                                                    , 'pg_tables_to_use': 'cp_pim_catalog_product_model'
                                                                    , 'mysql_tables_to_copy': 'pim_catalog_product_model'
                                                                    , 'mysql_schema': 'akeneo'}, retries=5)
    copy_GFGH_DATA_from_mySQL = PythonOperator(task_id='copy_GFGH_DATA_from_mySQL', python_callable=My_SQL_to_Postgres,
                                                          op_kwargs={'pg_schema': 'junk_tables'
                                                                    , 'pg_tables_to_use': 'gfgh_data'
                                                                    , 'mysql_tables_to_copy': 'product'
                                                                    , 'mysql_schema': 'gfghdata'}, retries=5)
    dbt_job_raw_layers = PythonOperator(
        task_id='dbt_job_raw_layers', python_callable=dbt_run) 
    # run_All_SKUs =                       PostgresOperator(
    #                                                             task_id="run_All_SKUs",
    #                                                             postgres_conn_id="post_gres_prod",
    #                                                             sql="custom/ALL_SKUS.sql",
    #                                                         )
data_dog_log >> copy_PIM_CATALOUG_PRODUCT_from_mySQL >>copy_PIM_CATALOUG_PRODUCT_model_from_mySQL >> copy_GFGH_DATA_from_mySQL #>> dbt_job_raw_layers#>>run_All_SKUs 