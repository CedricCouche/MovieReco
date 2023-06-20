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
# import numpy as np
# import string

import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, create_engine, text, inspect
from sqlalchemy_utils import database_exists, create_database

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity


# -------------------------------------- #
# DAG
# -------------------------------------- #

my_dag = DAG(
    dag_id='cosine-similarity_02',
    description='cosine-similarity_02',
    tags=['model', 'Process_C'],
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

mysql_url = 'container_mysql:3306'
mysql_user = 'root'
mysql_password = 'my-secret-pw'
database_name = 'db_movie'

# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #


def cosine_similarity(top_n):
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
        query = """ SELECT * FROM imdb_content LIMIT 10; """
        df = pd.read_sql(query, engine)


        # Feature build
        list_cols = ['primaryTitle','titleType', 'genres', 'runtimeCategory', 'yearCategory']
        df['combined_features'] = df[list_cols].apply(lambda x: ' '.join(x), axis=1)

        df = df.drop(columns=list_cols, axis=1)

        # Tokenization
        cv = CountVectorizer()
        count_matrix = cv.fit_transform(df["combined_features"])

        # Cosine Similarity computation
        cosine_sim = cosine_similarity(count_matrix)

        # # Initialisation of df_score
        # df_score = pd.DataFrame(columns=['tconst','similar_movies'])

        # # Loop : for each movie, we compute CS against all movies
        # for movie_index in range(len(cosine_sim)):

        #     print(movie_index)
        #     target_movie_id = df.iloc[movie_index]['tconst']
        #     print(target_movie_id)

        #     # Movie Recommandation
        #     similar_movies = list(enumerate(cosine_sim[movie_index]))
        #     sorted_similar_movies = sorted(similar_movies, key=lambda x:x[1], reverse=True)[:top_n]

        #     list_index = []
        #     for e in range(len(sorted_similar_movies)):
        #         movie_index = sorted_similar_movies[e][0]
        #         list_index.append(movie_index)
            
        #     # Retrieve info on recommended movies
        #     list_titles = df.iloc[list_index]['tconst'].tolist()
        #     # movie_reco = df.iloc[list_index]
        #     # list_titles = movie_reco['tconst'].tolist()
        #     print(list_titles)

        #     df_movie = pd.DataFrame()
        #     df_movie['tconst'] = target_movie_id
        #     df_movie['similar_movies']= list_titles

        #     df_score = pd.concat([df_score, df_movie])


        # # SQL Table : creation if not existing
        # if not 'score_cs' in inspector.get_table_names():
        #     meta = MetaData()

        #     score_cs = Table(
        #     'score_cs', meta, 
        #     Column('tconst', String(15), primary_key=True), 
        #     Column('similar_movies', String(255))
        #     ) 

        #     meta.create_all(engine)

        # # Store data in MySQL DB
        # # df_score.to_sql('score_cs', engine, if_exists='replace', index=False)

        # # Save in CSV
        # df_score.to_csv('/app/reco_data/cs_score.csv', index=False)


        conn.close()

        print('cosine_similarity done')

        return 0


# -------------------------------------- #
# TASKS
# -------------------------------------- #


task1 = PythonOperator(
    task_id='cosine_similarity',
    python_callable=cosine_similarity,
    op_kwargs={'top_n':10},
    dag=my_dag
)


# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #




