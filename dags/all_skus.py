
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

from include.dbt_run_raw_layer import dbt_run_raw_layers
# Logging

def branch_on():
    print('Current Directory is '+os.getcwd())
    os.chdir('include')
    print('Current Directory is '+os.getcwd())
    
    load_dotenv('enviroment_variables.env')
    print(os.environ.items())
    pg_host =  os.getenv('PG_HOST')
    pg_database = os.getenv('PG_DATABASE')

    pg_user = os.getenv('PG_USERNAME_WRITE')
    pg_database =os.getenv('PG_DATABASE')

    pg_password = os.getenv('PG_PASSWORD_WRITE')
    pg_schema = os.getenv('PG_RAW_SCHEMA')
    pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    print(f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}")
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)

    merchants_active= pd.read_sql_table(os.getenv('PG_MERCHANTS_ACTIVE'), con=pg_engine,schema=os.getenv('PG_RAW_SCHEMA'))
    merchants_active = merchants_active[~merchants_active["merchant_key"].str.contains('test',na=False)]
    merchants_active = merchants_active[merchants_active["merchant_key"]!='trinkkontor']
    merchants_active = merchants_active[merchants_active["merchant_key"]!='trinkkontor_trr']



    merchants_active_count= pd.read_sql_table('current_merchant_active_count', con=pg_engine,schema=pg_schema)

    if (merchants_active.size > merchants_active_count.loc[0,'merchant_count']):
        merchants_active_count.loc[0,'merchant_count'] = len(merchants_active)
        pg_tables_to_use ='current_merchant_active_count'
        #merchants_active_count.to_sql(pg_tables_to_use,pg_engine,schema=pg_schema, if_exists='replace',index=False)
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
    dag_id="all_skus_branch",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 */23 * * *",
    concurrency=100
    ,catchup=False
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    data_dog_log 	= DummyOperator(task_id='data_dog_log', retries=3)

    
    # dbt_job_run_raw_layers  = PythonOperator( task_id='dbt_job_run_raw_layers'
    #                                         , python_callable=dbt_run_raw_layer
    #                                         , trigger_rule='none_failed')
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
    branch_operator = BranchPythonOperator(
                                                task_id='choose_delta_or_full_load',
                                                python_callable=branch_on
                                                )
    dbt_job_run_raw_layers  = PythonOperator(task_id='dbt_job_run_raw_layers', python_callable=dbt_run_raw_layers)
    data_dog_log_final = DummyOperator(task_id='data_dog_log_final', retries=3)
data_dog_log >> branch_operator >>[full_load,delta_load] >>dbt_job_run_raw_layers >>data_dog_log_final


    