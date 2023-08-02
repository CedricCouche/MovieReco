# -------------------------------------- #
# Imports
# -------------------------------------- #

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import datetime

import pandas as pd
# import requests
import numpy as np
# import string

import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, inspect
from sqlalchemy_utils import database_exists, create_database

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity


# -------------------------------------- #
# DAG
# -------------------------------------- #


my_dag = DAG(
    dag_id='A01_Tests_v01',
    description='A01_Tests_v01',
    tags=['model', 'Process_A'],
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


mysql_url      = Variable.get("mysql", deserialize_json=True)["url"]
mysql_user     = Variable.get("mysql", deserialize_json=True)["user"]
mysql_password = Variable.get("mysql", deserialize_json=True)["password"]
database_name  = Variable.get("mysql", deserialize_json=True)["database_name"]


# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #

def Test_SQLAlchemy_A(top_n):
        """
        This function build the combined feature that will be used for cosine similarity
        """

        print('Process started')

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
        query = """ SELECT * FROM imdb_content LIMIT 20; """
        df = pd.read_sql(sql=query, con=conn)

        df = df.iloc[0]
        
        # Store in MySQL DB
            # with engine.connect() as connection:
             
            #     with connection.begin() as transaction:
                    
            #         try:
            #             ins = 'INSERT INTO test_a VALUES ()'         
            #             connection.execute(ins, dict_movie)  

            #         except:
            #             transaction.rollback()
            #         raise
  
            #         else:
            #             transaction.commit()
            

        # TEMP
        df.to_csv('/app/processed_data/test_a.csv', index=False)


        
        # Close MySQL connection and engine
        conn.close()
        engine.dispose()

        print('Process done')
        return 0


# -------------------------------------- #
# TASKS
# -------------------------------------- #


task1 = PythonOperator(
    task_id='Test_SQLAlchemy',
    python_callable=Test_SQLAlchemy_A,
    op_kwargs={'top_n':11},
    dag=my_dag
)


# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #




