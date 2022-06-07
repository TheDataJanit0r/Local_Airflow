
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

from cProfile import run
from datetime import datetime, timedelta
import json
from os import environ
from base64 import b64decode
from datetime import datetime
from logging import getLogger, INFO, WARN
from os import environ
from sys import exit as sys_exit
from sqlalchemy import create_engine
import io
import psycopg2
import csv
from pandas import read_sql_table
from psycopg2.extensions import register_adapter
from psycopg2.extras import Json
from psycopg2 import connect
import pandas as pd
import warnings
import psycopg2
import csv
import io
#from tkinter.messagebox import QUESTION
import mysql.connector
import pandas as pd
import os
import numpy as np
import time
import io
import csv
import requests
import os 
from dotenv import load_dotenv, find_dotenv
from include.delta_load_all_skus import run_delta_load
from include.full_load_all_skus import run_full_load
from include.full_load_all_skus_to_sql import run_full_load_to_sql
from include.dbt_run_raw_layer import dbt_run_raw_layers
from include.dbt_run_all_layers import dbt_run_all_layers
from include.my_sql_to_postgres import My_SQL_to_Postgres
# Logging

def branch_on():

    # os.chdir('include')
    # load_dotenv('enviroment_variables.env')
    pg_host =  os.getenv('PG_HOST_STAGING')
    pg_user = os.getenv('PG_USERNAME_WRITE_STAGING')
    pg_password = os.getenv('PG_PASSWORD_WRITE_STAGING')



    pg_database = os.getenv('PG_DATABASE')
    pg_schema = os.getenv('PG_RAW_SCHEMA')
    pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)

    merchants_active= pd.read_sql_table('merchants_all', con=pg_engine,schema=os.getenv('PG_RAW_SCHEMA'))
    merchants_active = merchants_active[~merchants_active["merchant_key"].str.contains('test',na=False)]
    merchants_active = merchants_active[merchants_active["merchant_key"]!='trinkkontor']
    merchants_active = merchants_active[merchants_active["merchant_key"]!='trinkkontor_trr']



    merchants_active_count= pd.read_sql_table('current_merchant_active_count', con=pg_engine,schema=pg_schema)

    if (merchants_active['merchant_key'].size != merchants_active_count.loc[0,'merchant_count']):
        merchants_active_count.loc[0,'merchant_count'] = merchants_active['merchant_key'].size
        pg_tables_to_use ='current_merchant_active_count'
        merchants_active_count.to_sql(pg_tables_to_use,pg_engine,schema=pg_schema, if_exists='replace',index=False)
        print("changed the count")
        return['run_full_load']
    else :
        return['run_delta_load']

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


with DAG(
    dag_id="PIM_Pipeline",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 */4 * * *",
    concurrency=100
    ,catchup=False
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    data_dog_log 	=  BashOperator(
                                                            task_id='Started_All_SKUs_DAG',
                                                            bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
                                                            dag=dag,
                                                            
                                                            )
    full_load = PythonOperator(
                                task_id='run_full_load'
                              , python_callable=run_full_load
                              , dag=dag
                              ,trigger_rule="none_failed"
                              , retries=5)

    delta_load = PythonOperator(
                                 task_id='run_delta_load'
                                ,python_callable=run_delta_load
                                ,dag=dag
                                ,trigger_rule="none_failed"
                                ,retries=5
                                )
    branch_operator = BranchPythonOperator (
                                                task_id='choose_delta_or_full_load',
                                                python_callable=branch_on
                                            )
    
    copy_PIM_CATALOUG_PRODUCT_from_mySQL = PythonOperator(task_id='copy_PIM_CATALOUG_PRODUCT_from_mySQL'
                                                        , python_callable=My_SQL_to_Postgres,
                                                          op_kwargs={'pg_schema': 'from_pim'
                                                                    , 'pg_tables_to_use': 'cp_pim_catalog_product'
                                                                    , 'mysql_tables_to_copy': 'pim_catalog_product'
                                                                    , 'mysql_schema': 'akeneo'
                                                                    , 'unique_column':'id'
                                                                    , 'delta_load' :'UPSERT'
                                                                    , 'timestamp_column':'updated'
                                                                    , 'look_back_period' : 0
                                                                    , 'chunksize_to_use':10000}
                                                        , retries=5)
    copy_PIM_CATALOUG_PRODUCT_model_from_mySQL = PythonOperator(task_id='copy_PIM_CATALOUG_PRODUCT_model_from_mySQL'
                                                        , python_callable=My_SQL_to_Postgres,
                                                          op_kwargs={'pg_schema': 'from_pim'
                                                                    , 'pg_tables_to_use': 'cp_pim_catalog_product_model'
                                                                    , 'mysql_tables_to_copy': 'pim_catalog_product_model'
                                                                    , 'mysql_schema': 'akeneo'
                                                                    , 'timestamp_column':'updated'
                                                                    , 'unique_column':'id'
                                                                    , 'delta_load' :'UPSERT'
                                                                    , 'look_back_period' : 0
                                                                     ,'chunksize_to_use':10000}
                                                         , retries=5)
    copy_GFGH_DATA_from_mySQL = PythonOperator(task_id='copy_GFGH_DATA_from_mySQL'
                                                        , python_callable=My_SQL_to_Postgres,
                                                          op_kwargs={'pg_schema': 'from_pim'
                                                                    , 'pg_tables_to_use': 'cp_gfgh_product'
                                                                    , 'mysql_tables_to_copy': 'product'
                                                                    , 'timestamp_column':'updated_at'
                                                                    , 'mysql_schema': 'gfghdata'
                                                                    , 'unique_column':'id'
                                                                    , 'delta_load' :'UPSERT'
                                                                    , 'look_back_period' : 0
                                                                    , 'chunksize_to_use':10000}
                                                        , retries=5)
    dbt_job_raw_layers = PythonOperator(
                                        task_id='dbt_job_raw_layers'
                                        , python_callable=dbt_run_raw_layers,
                                        trigger_rule='all_success'
                                        ) 
    dbt_job_all_layers = PythonOperator(
                                        task_id='dbt_run_all_layers'
                                        , python_callable=dbt_run_all_layers,
                                        trigger_rule='all_success'
                                        ) 
    data_dog_log_final = BashOperator(
                                        task_id='Finished_All_SKUs_fully',
                                        bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
                                        dag=dag,
                                        trigger_rule="none_failed"
                                    )
    data_dog_log_middle = BashOperator(
                                                            task_id='Finished_delta_or_full_load',
                                                            bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
                                                            dag=dag,
                                                            trigger_rule="none_failed"
                                                            )
    data_dog_log_middle_2 = BashOperator(
                                                                task_id='Finished_Copying_tables_from_MySQL',
                                                                bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
                                                                dag=dag,
                                                                trigger_rule="none_failed"
                                                            )


data_dog_log >> branch_operator >>[full_load,delta_load] >>data_dog_log_middle
data_dog_log_middle>>[copy_PIM_CATALOUG_PRODUCT_model_from_mySQL,copy_GFGH_DATA_from_mySQL,copy_PIM_CATALOUG_PRODUCT_from_mySQL] >>data_dog_log_middle_2
data_dog_log_middle_2>>[dbt_job_raw_layers,dbt_job_all_layers] >>data_dog_log_final