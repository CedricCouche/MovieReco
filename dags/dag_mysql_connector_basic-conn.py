# -------------------------------------- #
# Imports
# -------------------------------------- #

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import datetime

# import pandas as pd
# import numpy as np
# import requests
# import string

import mysql.connector

# import sqlalchemy
# from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, create_engine, text, inspect
# from sqlalchemy_utils import database_exists, create_database

# -------------------------------------- #
# DAG
# -------------------------------------- #

my_dag = DAG(
    dag_id='MySQL-Connector-basic-conn',
    description='MySQL-Connector-basic-conn',
    tags=['TEST'],
    schedule_interval=datetime.timedelta(minutes=30),
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
)

# -------------------------------------- #
# Global variables
# -------------------------------------- #


processed_filenames = ['title.basics_reduced.zip', 
                        'title_basics_processed.zip',
                        'imdb_content.csv.zip',
                        'api_data.zip']

path_raw_data = '/app/raw_data/'
path_processed_data = '/app/processed_data/'

#mysql_url = 'container_mysql:3306'
mysql_user = 'root'
mysql_password = 'my-secret-pw'
database_name = 'db_movie'
mysql_host = 'container_mysql'


# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #

def load_mysql_connector(source_path):
    """
    description
    """

    print('load_mysql started')


    # MySQLconnection set-up
    connection = mysql.connector.connect(
        user=mysql_user,
        password=mysql_password,
        host=mysql_host,
        database=database_name,
        ssl_disabled=True
    )

    cursor = connection.cursor()

    # Query
    query = """
        SELECT * FROM table_api LIMIT 5
    """

    cursor.execute(query)

    result = []
    for i, data in enumerate(cursor):
        result.append(data)

    print(result)

    # Close connnection
    cursor.close()
    connection.close()


    print('load_mysql done')
    return 0

# -------------------------------------- #
# TASKS
# -------------------------------------- #

task1 = PythonOperator(
    task_id='load_mysql_connector',
    python_callable=load_mysql_connector,
    op_kwargs={'source_path':path_processed_data + processed_filenames[3]},
    dag=my_dag
)



# -------------------------------------- #
# TASKS DEPENDANCIES
# -------------------------------------- #


