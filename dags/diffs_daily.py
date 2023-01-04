 # pyright: reportMissingImports=false

from datetime import datetime, timedelta
import os
import time
from include.dbt_run_all_layers import dbt_run_all_layers

import airflow 
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash import BashOperator 
from include.monday_api import run_monday_api

from include.dbt_run_all_layers import dbt_run_all_layers
from include.my_sql_to_postgres import My_SQL_to_Postgres
from include.dbt_run_diffs import dbt_run_diffs
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
    dag_id="diffs_daily",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 7 * * *",
    concurrency=100,
    catchup=False
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    data_dog_log = BashOperator(
        task_id='Started_Diffs_DAG',
        bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
        dag=dag,

    )
    copy_GFGH_DATA_cp_import = PythonOperator(task_id='copy_GFGH_DATA_cp_import', python_callable=My_SQL_to_Postgres,
                                              op_kwargs={  'pg_schema': 'from_pim'
                                                         , 'pg_tables_to_use': 'cp_gfgh_import'
                                                         , 'mysql_tables_to_copy': 'import'
                                                         , 'mysql_schema': 'gfghdata'
                                                         , 'delta_load': 'FULL_RELOAD'
                                                         , 'unique_column': 'id'
                                                         , 'timestamp_column': 'created_at'
                                                         , 'look_back_period': 0
                                                         , 'chunksize_to_use': 10000}
                                                , retries=5
                                              )
    copy_cp_gfgh_product_import = PythonOperator(task_id='copy_cp_gfgh_product_import', python_callable=My_SQL_to_Postgres,
                                                 op_kwargs={'pg_schema': 'from_pim'
                                                         , 'pg_tables_to_use': 'cp_gfgh_product_import'
                                                         , 'mysql_tables_to_copy': 'product_import'
                                                         , 'mysql_schema': 'gfghdata'
                                                         , 'delta_load': 'INSERT_NEW_ROWS_DROP_OLD_TABLE'
                                                         , 'unique_column': 'NOT_NEEDED'
                                                         , 'timestamp_column': 'created_at'
                                                         , 'look_back_period': 30 #### 22 because i want the last 3 weeks cuz it's enough
                                                         , 'chunksize_to_use': 10000}
                                                , retries=5
                                                 )

    dbt_run_diffs_node = PythonOperator(
                                        task_id='dbt_run_diffs'
                                        , python_callable=dbt_run_diffs,
                                        trigger_rule='all_success'
                                        ) 
  
    data_dog_log_final = BashOperator(

        task_id='finished_Diffs_DAG',
        bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
        dag=dag,

    )


data_dog_log >>copy_cp_gfgh_product_import >> copy_GFGH_DATA_cp_import >>dbt_run_diffs_node>> data_dog_log_final
