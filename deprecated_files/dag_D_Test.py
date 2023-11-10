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
    dag_id='Test_D02',
    description='Test',
    tags=['Pre-Process', 'Process_D'],
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


def movie_to_process(batch_nb, source_path):
        """
        description : return a list of movies to process

        Arguments : 
            - nb_movies : batch size of movies to process

        """
        print('process started')

        # Connection to MySQL
        connection_url = 'mysql://{user}:{password}@{url}/{database}'.format(
            user=mysql_user,
            password=mysql_password,
            url=mysql_url,
            database = database_name
            )

        engine = create_engine(connection_url)
        conn = engine.connect()
        inspector = inspect(engine)

        # Load
        query = """ SELECT tconst FROM imdb_titlebasics; """
        df_existing_tconst = pd.read_sql(query, engine)
        list_movies = df_existing_tconst['tconst'].tolist()
        
        print('len list_movies:', len(list_movies))

        # Load
        df = pd.read_csv(source_path,
                 compression='gzip', 
                 sep='\t', 
                 usecols= ['tconst'],
                 dtype= {'tconst':object}
                 ) 

        # Limitation of existing films in title basics
        df = df[~df['tconst'].isin(list_movies)]
        df = df.head(batch_nb)

  
        # Store data in MySQL DB
        df.to_sql('title_to_process', engine, if_exists='replace', index=False)

        # Deletion of df to save memory
        df = pd.DataFrame()
        df_existing_tconst = pd.DataFrame()
        del df
        del df_existing_tconst

        # Closing MySQL connection
        conn.close()
        engine.dispose()
        print('process done')

        return 0




# -------------------------------------- #
# TASKS
# -------------------------------------- #


task0 = PythonOperator(
    task_id='movie_to_process',
    python_callable=movie_to_process,
    op_kwargs={'batch_nb':100, 'source_path':path_raw_data + imdb_files_names[0]},
    dag=my_dag
)



# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #




