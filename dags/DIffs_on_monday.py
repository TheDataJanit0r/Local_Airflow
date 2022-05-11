from datetime import datetime, timedelta



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
from sqlalchemy import create_engine
from dotenv import load_dotenv
import requests
from include.dbt_run_all_layers import dbt_run_all_layers

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

from include.monday_api import run_monday_api

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
    dag_id="diffs_monday",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 0 * * 1",
    concurrency=100,
    catchup=False
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    data_dog_log 	=  BashOperator        (
                                                task_id='Started_Diffs_DAG',
                                                bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
                                                dag=dag,
                                            
                                            )
    copy_GFGH_DATA_cp_import = PythonOperator(            task_id='copy_GFGH_DATA_cp_import'
                                                        , python_callable=My_SQL_to_Postgres,
                                                          op_kwargs={'pg_schema': 'from_gfgh'
                                                                    , 'pg_tables_to_use': 'cp_import'
                                                                    , 'mysql_tables_to_copy': 'product_import'
                                                                    , 'mysql_schema': 'gfghdata'
                                                                    ,'delta_load' :True
                                                                    ,'chunksize_to_use':10000}
                                                        , retries=5)
    copy_cp_product_import = PythonOperator(              task_id='copy_GFGH_DATA_cp_product_import'
                                                        , python_callable=My_SQL_to_Postgres,
                                                          op_kwargs={'pg_schema': 'from_gfgh'
                                                                    , 'pg_tables_to_use': 'cp_product_import'
                                                                    , 'mysql_tables_to_copy': 'product'
                                                                    , 'mysql_schema': 'gfghdata'
                                                                    ,'delta_load' :True
                                                                    ,'chunksize_to_use':10000}
                                                        , retries=5)

    
data_dog_log >> copy_GFGH_DATA_cp_import >>copy_cp_product_import# >>dbt_job_run_all_layers>>data_dog_log_final


    