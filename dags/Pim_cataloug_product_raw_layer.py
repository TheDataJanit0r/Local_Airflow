
import airflow
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


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

from include.dbt_run_raw_layer import dbt_run_raw_layers
from include.dbt_run_all_layers import dbt_run_all_layers
from include.my_sql_to_postgres import My_SQL_to_Postgres






default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'backfill' :False,
    'catchup':False
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
    start_date=datetime.now(),#datetime.today() - timedelta(days=1),
    schedule_interval="0 18-23/4 * * *",
    concurrency=100,
   
    catchup=False
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    data_dog_log = DummyOperator(task_id='data_dog_log', retries=3)
    copy_PIM_CATALOUG_PRODUCT_from_mySQL = PythonOperator(task_id='copy_PIM_CATALOUG_PRODUCT_from_mySQL', python_callable=My_SQL_to_Postgres,
                                                          op_kwargs={'pg_schema': 'from_pim'
                                                                    , 'pg_tables_to_use': 'cp_pim_catalog_product'
                                                                    , 'mysql_tables_to_copy': 'pim_catalog_product'
                                                                    , 'mysql_schema': 'akeneo'}, retries=5)
    copy_PIM_CATALOUG_PRODUCT_model_from_mySQL = PythonOperator(task_id='copy_PIM_CATALOUG_PRODUCT_model_from_mySQL', python_callable=My_SQL_to_Postgres,
                                                          op_kwargs={'pg_schema': 'from_pim'
                                                                    , 'pg_tables_to_use': 'cp_pim_catalog_product_model'
                                                                    , 'mysql_tables_to_copy': 'pim_catalog_product_model'
                                                                    , 'mysql_schema': 'akeneo'}, retries=5)
    copy_GFGH_DATA_from_mySQL = PythonOperator(task_id='copy_GFGH_DATA_from_mySQL', python_callable=My_SQL_to_Postgres,
                                                          op_kwargs={'pg_schema': 'from_pim'
                                                                    , 'pg_tables_to_use': 'gfgh_data'
                                                                    , 'mysql_tables_to_copy': 'product'
                                                                    , 'mysql_schema': 'gfghdata'}, retries=5)
    dbt_job_raw_layers = PythonOperator(
                                        task_id='dbt_job_raw_layers'
                                        , python_callable=dbt_run_raw_layers,
                                        trigger_rule='all_done'
                                        ) 
    data_dog_log_final = DummyOperator(task_id='data_dog_log_final', retries=3)
  
data_dog_log >> [copy_PIM_CATALOUG_PRODUCT_from_mySQL,copy_PIM_CATALOUG_PRODUCT_model_from_mySQL , copy_GFGH_DATA_from_mySQL] 
[copy_PIM_CATALOUG_PRODUCT_from_mySQL,copy_PIM_CATALOUG_PRODUCT_model_from_mySQL , copy_GFGH_DATA_from_mySQL] >> dbt_job_raw_layers
dbt_job_raw_layers>>data_dog_log_final