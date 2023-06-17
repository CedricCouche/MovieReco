# -------------------------------------- #
# Imports
# -------------------------------------- #

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import datetime

import pandas as pd
import numpy as np
import requests
import string

import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, create_engine, text, inspect
from sqlalchemy_utils import database_exists, create_database

# -------------------------------------- #
# DAG
# -------------------------------------- #

my_dag = DAG(
    dag_id='Load_MySQL_01',
    description='Load_MySQL_01',
    tags=['DB'],
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

mysql_url = 'container_mysql:3306'
mysql_user = 'root'
mysql_password = 'my-secret-pw'
database_name = 'db_movie'


# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #



def load_mysql_pandas(source_path):
    """
    This function load data from a local file and store it in MySQL database
    """
    print('load_mysql started')

    # Creating the URL connection
    connection_url = 'mysql://{user}:{password}@{url}/{database}'.format(
        user=mysql_user,
        password=mysql_password,
        url=mysql_url,
        database = database_name
        )

    engine = create_engine(connection_url)
    inspector = inspect(engine)

  
    # Load data from .csv
    column_list = [
        'tconst', 'titleType', 'primaryTitle','startYear','runtimeMinutes', 'genres', 'runtimeCategory', 'yearCategory','combined_features']
    dict_types = {'tconst':object,'titleType':object, 'primaryTitle':object, 'startYear':int, 'runtimeMinutes':int, 'genres':object, 'runtimeCategory':object, 'yearCategory':object, 'combined_features':object}

    df = pd.read_csv(source_path, usecols= column_list, dtype=dict_types, compression = 'zip', sep = ',')

    print('pandas loaded')


    # Store data in MySQL DB
    df.to_sql('table_api', engine, if_exists='replace', index=False)


    print('load_mysql done')

    return 0


# -------------------------------------- #
# TASKS
# -------------------------------------- #

task1 = PythonOperator(
    task_id='load_mysql',
    python_callable=load_mysql_pandas,
    op_kwargs={'source_path':path_processed_data + processed_filenames[3]},
    dag=my_dag
)


# -------------------------------------- #
# TASKS DEPENDANCIES
# -------------------------------------- #


