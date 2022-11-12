from dag_tasks.upload_to_db import load_trajectory_data
from dag_tasks.db_connect import start_DB_session
from dag_tasks.create_tables import create_tables

from airflow.decorators import dag
from datetime import timedelta, datetime

import os
from pathlib import Path


# DEFAULT ARGUMENTS FOR DAGs
default_args = {'owner': 'janerose', 'email': 'janerosenyams@gmail.com', 'email_on_failure': True, 'email_on_retry': True, 'depends_on_past': False, 'retries': 1,
                'start_date': datetime(2022, 9, 11), 'retry_delay': timedelta(minutes=2)}


@dag(dag_id="csv_data_to_db", default_args=default_args, description='read data from CSV files and upload data to table in database', schedule=None, catchup=False, dagrun_timeout=timedelta(minutes=5), tags=['postgres_db'])
def save_data_dag():
    dir_name = "/home/njogu-janerose/10-Academy-Repo/traffic-dwh-tech-stack"
    # TODO: CHANGE FILE PATH AS NECESSARY
    file_path = dir_name+"/data/20181024_d5_0830_0900.csv"
    # start session and save data to Postgres Database
    # executing each task
    create_tables(start_DB_session())>>load_trajectory_data(start_DB_session(), file_path)


data_to_db_dag = save_data_dag()