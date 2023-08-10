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

import dask as dd
#import dask.dataframe as dd

import sqlalchemy
from sqlalchemy import create_engine, inspect
from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, text 
from sqlalchemy_utils import database_exists, create_database


# -------------------------------------- #
# DAG
# -------------------------------------- #

my_dag = DAG(
    dag_id='A03_Test-Dask_v1',
    description='Preprocess',
    tags=['Pre-Process', 'Process_A'],
    schedule_interval=datetime.timedelta(minutes=10),
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
)

# -------------------------------------- #
# Global variables
# -------------------------------------- #


# imdb_base_url       = Variable.get("imdb_base_url")
# imdb_files_names    = Variable.get("imdb_files_names", deserialize_json=True)["list"]
imdb_base_url       = Variable.get("imdb", deserialize_json=True)["base_url"]
imdb_files_names    = Variable.get("imdb", deserialize_json=True)["file_names"]
processed_filenames = Variable.get("processed_filenames", deserialize_json=True)["list"]

path_raw_data       = Variable.get("path_raw_data")
path_processed_data = Variable.get("path_processed_data")
path_reco_data      = Variable.get("path_reco_data")

mysql_url           = Variable.get("mysql", deserialize_json=True)["url"]
mysql_user          = Variable.get("mysql", deserialize_json=True)["user"]
mysql_password      = Variable.get("mysql", deserialize_json=True)["password"]
database_name       = Variable.get("mysql", deserialize_json=True)["database_name"]


# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #


def test_dask(batch_nb, source_path):
        """
        description : return a list of movies to process

        Arguments : 
            - nb_movies : batch size of movies to process

        """
        print('process started')

        dict_types = {'tconst':str, 
                    'titleType':str, 
                    'primaryTitle':str ,
                    'originalTitle':str, 
                    'isAdult':float, 
                    'startYear':float, 
                    'endYear':float, 
                    'runtimeMinutes':str, 
                    'genres':str
                    }


        # Load
        df = pd.read_csv(source_path,
                 compression='gzip', 
                 sep='\t', 
                 dtype= dict_types,
                 na_values=['\\N', 'nan', 'NA', ' nan','  nan', '   nan']
                 ) 
        
        df.to_parquet(path_raw_data+'title_basics.parquet.gzip',  compression='gzip')

        # DF Size
        print('Initial size :', df.info(memory_usage='deep'))

        # Limitation of existing films in title basics
        df = df[df['titleType'] == 'movie']
        df = df[df['isAdult'] == 0]


        print('reduced size :', df.info(memory_usage='deep'))

        # Deletion to save RAM
        del df

        ddf = dd.read_parquet(path_raw_data+'title_basics.parquet.gzip', compression='gzip') 

        print('parquet size :', ddf.info(memory_usage='deep'))

        return 0



# -------------------------------------- #
# TASKS
# -------------------------------------- #


task01 = PythonOperator(
    task_id='test_dask',
    python_callable=test_dask,
    op_kwargs={'batch_nb':100, 'source_path':path_raw_data + imdb_files_names[0]},
    dag=my_dag
)




# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #




