from datetime import datetime, timedelta

import os
import time


from include.dbt_run_all_layers import dbt_run_all_layers

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

from include.monday_api import run_monday_api
from include.gedat import run_gedat
from airflow.models import Variable

   
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
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
    dag_id="every_monday_routins",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 0 * * 1",
    concurrency=100,
     catchup=False
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    data_dog_log 	=  BashOperator        (
                                            task_id='Started_Monday_Routines_DAG',
                                            bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
                                            dag=dag,
                                            
                                            )
    run_monday_api	= PythonOperator(
                                            task_id='run_monday_api'
                                            , python_callable=run_monday_api
                                            , retries=5
                                            )
    get_gedat_results	= PythonOperator(
                                            task_id='get_gedat_results'
                                            , python_callable=run_gedat
                                            , retries=5
                                            )
    data_dog_log_finished 	=  BashOperator        (
                                            task_id='Finished_Monday_Routines_DAG',
                                            bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
                                            dag=dag,
                                            
                                            )
data_dog_log >> run_monday_api >>get_gedat_results >>data_dog_log_finished


    