# -------------------------------------- #
# Imports
# -------------------------------------- #

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import datetime
import pandas as pd
import requests
import numpy as np
import string

import sqlalchemy
from sqlalchemy import create_engine, inspect
from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, text 
from sqlalchemy_utils import database_exists, create_database


# -------------------------------------- #
# DAG
# -------------------------------------- #

my_dag = DAG(
    dag_id='get_new_data_D01',
    description='download data from IMDB',
    tags=['download', 'Process_D'],
    schedule_interval=datetime.timedelta(hours=12),
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
)

# -------------------------------------- #
# Global variables
# -------------------------------------- #


# imdb_base_url =  'https://datasets.imdbws.com/'
# imdb_files_names = ['title.basics.tsv.gz', 
#                     'name.basics.tsv.gz', 
#                     'title.akas.tsv.gz', 
#                     'title.crew.tsv.gz', 
#                     'title.episode.tsv.gz', 
#                     'title.principals.tsv.gz', 
#                     'title.ratings.tsv.gz']


# processed_filenames = ['title_basics.csv.zip', 
#                     'name_basics.csv.zip', 
#                     'title_akas.csv.zip', 
#                     'title_crew.csv.zip', 
#                     'title_episode.csv.zip', 
#                     'title_principals.csv.zip', 
#                     'title_ratings.csv.zip',
#                     'merged_content.csv.zip',
#                     'api_table.csv.zip']

# path_raw_data = '/app/raw_data/'


imdb_base_url       = Variable.get("imdb_base_url")
imdb_files_names    = Variable.get("imdb_files_names", deserialize_json=True)["list"]
processed_filenames = Variable.get("processed_filenames", deserialize_json=True)["list"]
path_raw_data       = Variable.get("path_raw_data")



# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #


def download():

    for i in range(len(imdb_files_names)):

        url = imdb_base_url + imdb_files_names[i]
        destination_path = path_raw_data + imdb_files_names[i]

        r = requests.get(url, allow_redirects=True)
        open(destination_path, 'wb').write(r.content)

    return 0


# -------------------------------------- #
# TASKS
# -------------------------------------- #

task1 = PythonOperator(
    task_id='download',
    python_callable=download,
    dag=my_dag
)


# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #




