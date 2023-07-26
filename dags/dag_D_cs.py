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
    dag_id='cosine-similarity_D',
    description='cosine-similarity_D',
    tags=['model', 'Process_D'],
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

def cosine_similarity_A(top_n):
        """
        This function build the combined feature that will be used for cosine similarity
        """

        print('cosine_similarity started')

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
        query = """ SELECT * FROM imdb_content LIMIT 1000; """
        df = pd.read_sql(sql=query, con=conn)


        # Feature build
        list_cols = ['primaryTitle','titleType', 'genres', 'runtimeCategory', 'yearCategory','directors_id', 'writers_id']
        df['combined_features'] = df[list_cols].apply(lambda x: ' '.join(x), axis=1)

        #cols_to_drop = ['primaryTitle','titleType', 'genres', 'runtimeCategory', 'yearCategory', 'startYear', 'runtimeMinutes']
        #df = df.drop(columns=cols_to_drop, axis=1)
        df = df[['tconst', 'combined_features']]

        # Tokenization
        cv = CountVectorizer()
        count_matrix = cv.fit_transform(df["combined_features"])
        print(count_matrix.shape)

        # Initialisation of dictionary
        dict_movie = {}

        # Loop : for each movie, we compute CS against all movies
        for index in range(len(df)):

            target_movie_id = df.iloc[index]['tconst']
            count_matrix_target = count_matrix[index]

            # Cosine Similarity computation
            cosine_sim = cosine_similarity(count_matrix, count_matrix_target)

            # List of recommanded movies
            similar_movies = list(enumerate(cosine_sim))
            sorted_similar_movies = sorted(similar_movies, key=lambda x:x[1], reverse=True)[:top_n]

            list_tconst = []
            list_score = []

            for e in range(len(sorted_similar_movies)):

                movie_index = sorted_similar_movies[e][0]
                movie_tconst = df.iloc[movie_index]['tconst']
                list_tconst.append(movie_tconst)

                movie_score = sorted_similar_movies[e][1][0]
                list_score.append(movie_score)

            # Note : target_movie_id appears in recommanded films, will be in position 1, be could be also in position 2 (if we're unlucky)
            redundant_movie_index = list_tconst.index(target_movie_id)
            list_tconst.pop(redundant_movie_index) 
            list_score.pop(redundant_movie_index) 
            list_tconst.insert(0,target_movie_id) # We add target_movie_id in first position

            dict_movie[index]= list_tconst + list_score

            # Clean-up to save RAM
            list_tconst = []
            list_score = []
            

        # DataFrame build
        top_labels   = ['top' + str(i) for i in range(1,top_n,1)]
        score_labels = ['score' + str(i) for i in range(1,top_n,1)]
        col_labels   = top_labels + score_labels
        col_labels.insert(0, 'tconst')

        df_score = pd.DataFrame.from_dict(data=dict_movie, orient='index', columns=col_labels)

        # Store in MySQL DB
        df_score.to_sql('score_cs', engine, if_exists='replace', index=False)

        # Clean-up to save RAM
        dict_movie = {}
        df_score = pd.DataFrame()
        del df_score
        
        # Close MySQL connection and engine
        conn.close()
        engine.dispose()

        print('cosine_similarity done')
        return 0



def cosine_similarity_B(top_n):
        """
        This function build the combined feature that will be used for cosine similarity
        """

        print('cosine_similarity started')

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
        query = """ SELECT * FROM imdb_content LIMIT 15; """
        df = pd.read_sql(sql=query, con=conn)


        # Feature build
        list_cols = ['primaryTitle','titleType', 'genres', 'runtimeCategory', 'yearCategory']
        df['combined_features'] = df[list_cols].apply(lambda x: ' '.join(x), axis=1)

        cols_to_drop = ['primaryTitle','titleType', 'genres', 'runtimeCategory', 'yearCategory', 'startYear', 'runtimeMinutes']
        df = df.drop(columns=cols_to_drop, axis=1)

        # Tokenization
        cv = CountVectorizer()
        count_matrix = cv.fit_transform(df["combined_features"])
        print(count_matrix.shape)

        # Initialisation of dictionary
        dict_movie = {}

        # Loop : for each movie, we compute CS against all movies
        for index in range(len(df)):

            target_movie_id = df.iloc[index]['tconst']
            count_matrix_target = count_matrix[index]

            # Cosine Similarity computation
            cosine_sim = cosine_similarity(count_matrix, count_matrix_target)

            # List of recommanded movies
            similar_movies = list(enumerate(cosine_sim))
            sorted_similar_movies = sorted(similar_movies, key=lambda x:x[1], reverse=True)[:top_n]

            list_tconst = []

            for e in range(len(sorted_similar_movies)):
                movie_index = sorted_similar_movies[e][0]
                movie_score = sorted_similar_movies[e][1][0]
                movie_id = df.iloc[movie_index]['tconst']
                list_tconst.append(movie_id)

            # Note : target_movie_id appears in recommanded films, will be in position 1, be could be also in position 2 (if we're unlucky)
            list_tconst.remove(target_movie_id) # We remove target_movie_id from recommanded list

            dict_movie[index]= [target_movie_id, list_tconst]

        # DataFrame build
        col_labels = ['tconst', 'movie_list']
        df_score = pd.DataFrame.from_dict(data=dict_movie, orient='index', columns=col_labels)
        print(df_score.head(3))

        # Store in MySQL DB
        df_score.to_sql('score_cs', engine, if_exists='replace', index=False)

        # Clean-up to save RAM
        dict_movie = {}
        df_score = pd.DataFrame()
        del df_score
        
        # Close MySQL connection and engine
        conn.close()
        engine.dispose()

        print('cosine_similarity done')
        return 0



# -------------------------------------- #
# TASKS
# -------------------------------------- #


task1 = PythonOperator(
    task_id='cosine_similarity',
    python_callable=cosine_similarity_A,
    op_kwargs={'top_n':11},
    dag=my_dag
)


# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #




