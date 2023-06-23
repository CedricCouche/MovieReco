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
    dag_id='download_C03',
    description='download_C03',
    tags=['download', 'Pre-Process', 'Process_C'],
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


imdb_base_url =  'https://datasets.imdbws.com/'
imdb_files_names = ['title.basics.tsv.gz', 
                    'name.basics.tsv.gz', 
                    'title.akas.tsv.gz', 
                    'title.crew.tsv.gz', 
                    'title.episode.tsv.gz', 
                    'title.principals.tsv.gz', 
                    'title.ratings.tsv.gz']


processed_filenames = ['title_basics.csv.zip', 
                    'name_basics.csv.zip', 
                    'title_akas.csv.zip', 
                    'title_crew.csv.zip', 
                    'title_episode.csv.zip', 
                    'title_principals.csv.zip', 
                    'title_ratings.csv.zip',
                    'merged_content.csv.zip',
                    'api_table.csv.zip']

path_raw_data = '/app/raw_data/'
path_processed_data = '/app/processed_data/'
path_reco_data = '/app/reco_data/'

mysql_url = 'container_mysql:3306'
mysql_user = 'root'
mysql_password = 'my-secret-pw'
database_name = 'db_movie'

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


def process_title_basics(source_path):
        """
        This function clean the file title_basics, discretise some fields and reduce the size of the dataset

        Original fields from imdb : 
            - tconst (string) - alphanumeric unique identifier of the title
            - titleType (string) – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
            - primaryTitle (string) – the more popular title / the title used by the filmmakers on promotional materials at the point of release
            - originalTitle (string) - original title, in the original language
            - isAdult (boolean) - 0: non-adult title; 1: adult title
            - startYear (YYYY) – represents the release year of a title. In the case of TV Series, it is the series start year
            - endYear (YYYY) – TV Series end year. "\\N" for all other title types
            - runtimeMinutes – primary runtime of the title, in minutes
            - genres (string array) – includes up to three genres associated with the title
        """

        print('process_title_basics started')

        # Load
        column_list = ['tconst', 'titleType', 'primaryTitle', 'startYear', 'runtimeMinutes', 'genres', 'isAdult']
        dict_types = {'tconst':object, 'titleType':object, 'primaryTitle':object, 'startYear':object, 'runtimeMinutes':object, 'genres':object, 'isAdult':object}

        df = pd.read_csv(source_path,
                 compression='gzip', 
                 sep='\t', 
                 usecols= column_list,
                 dtype=dict_types,
                 na_values=['\\N', 'nan', 'NA', ' nan','  nan', '   nan']
                 ) 


        # Drop of rows containing NANs
        df = df.dropna(how='any', axis=0, subset=['startYear', 'runtimeMinutes', 'genres','isAdult'])

        # Drop of rows containing errors
        runtime_errors = ['Reality-TV','Talk-Show','Documentary','Game-Show','Animation,Comedy,Family','Game-Show,Reality-TV']
        df = df[~df['runtimeMinutes'].isin(runtime_errors)] # '~' sign allows to reverse the logic of isin()

        # Format change
        df['startYear']      = df['startYear'].astype('float')
        df['runtimeMinutes'] = df['runtimeMinutes'].astype('float')
        df['isAdult']        = df['isAdult'].astype('float')

        df['startYear']      = df['startYear'].apply(np.int64)
        df['runtimeMinutes'] = df['runtimeMinutes'].apply(np.int64)
        df['isAdult']        = df['isAdult'].apply(np.int64)

        df['startYear']      = df['startYear'].astype('int')
        df['runtimeMinutes'] = df['runtimeMinutes'].astype('int')
        df['isAdult']        = df['isAdult'].astype('int')


        # Limitation of the data set size
        df = df[df['startYear']>2018.0]
        df = df[df['titleType']=='movie']
        df = df[df['isAdult']==0]

        df = df.drop(columns=['isAdult'], axis=1)


        # Discretisation of runtime

        generic_labels = list(string.ascii_uppercase)

        bins_runtime = [0, 10, 20, 30, 45, 60, 120, 150, 180, 9999]
        df['runtimeCategory'] = pd.cut(x = df['runtimeMinutes'],
                                                bins = bins_runtime,
                                                labels = generic_labels[:len(bins_runtime)-1],
                                                include_lowest=True)

        df['runtimeCategory'] = df['runtimeCategory'].astype(str)


        # Discretisation of startYear
        df['startYear'] = df['startYear'].astype(int)

        bins_years = [1850, 1900, 1930, 1950, 1960, 1970, 1980, 1990, 2000, 2010, 2020, 2030]
        df['yearCategory'] = pd.cut(x = df['startYear'],
                                                bins = bins_years,
                                                labels = generic_labels[:len(bins_years)-1],
                                                include_lowest=True)

        df['yearCategory'] = df['yearCategory'].astype(str)


        # Connection to MySQL
        connection_url = 'mysql://{user}:{password}@{url}/{database}'.format(
            user=mysql_user,
            password=mysql_password,
            url=mysql_url,
            database = database_name
            )

        engine = create_engine(connection_url)
        conn = engine.connect()
        # inspector = inspect(engine)

        # # SQL Table : creation if not existing
        # if not 'imdb_titlebasics' in inspector.get_table_names():
        #     meta = MetaData()

        #     imdb_titlebasics = Table(
        #     'imdb_titlebasics', meta, 
        #     Column('tconst', String(15), primary_key=True), 
        #     Column('titleType', String(150)), 
        #     Column('primaryTitle', String(150)),
        #     Column('startYear', Integer),
        #     Column('runtimeMinutes', Integer),
        #     Column('genres',  String(150)),
        #     Column('runtimeCategory',  String(2)),
        #     Column('yearCategory',  String(2))
        #     ) 

        #     meta.create_all(engine)


        # Store data in MySQL DB
        df.to_sql('imdb_titlebasics', engine, if_exists='replace', index=False)

        # Save in CSV
        # df.to_csv(destination_path, index=False, compression="zip")


        # Deletion of df to save memory
        df = pd.DataFrame()
        del df

        conn.close()
        engine.dispose()
        print('process_title_basics done')

        return 0


def process_name_basics(source_path):
        # """
        # description
        # """
        # print('process started')

        # # Load
        # df = pd.read_csv(source_path,
        #             compression='gzip', 
        #             sep='\t', 
        #             na_values=['\\N', 'nan', 'NA', ' nan','  nan', '   nan']
        #             ) 

        # # Save
        # df.to_csv(destination_path, index=False, compression="zip")

        # # Deletion of df to save memory
        # df = pd.DataFrame()
        # del df

        # print('process done')

        return 0


def process_title_akas(source_path):
        # """
        # description
        # """
        # print('process started')

        # # Load
        # df = pd.read_csv(source_path,
        #             compression='gzip', 
        #             sep='\t', 
        #             na_values=['\\N', 'nan', 'NA', ' nan','  nan', '   nan']
        #             ) 

        # # Save
        # df.to_csv(destination_path, index=False, compression="zip")

        # # Deletion of df to save memory
        # df = pd.DataFrame()
        # del df

        # print('process done')


        return 0


def process_title_crew(source_path):
        """
        description : load & pre-process data

        Original fields from imdb : 
            - tconst (string) - alphanumeric unique identifier of the title
            - directors (array of nconsts) - director(s) of the given title
            - writers (array of nconsts) – writer(s) of the given title

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
                    na_values=['\\N', 'nan', 'NA', ' nan','  nan', '   nan']
                    ) 

        df = df.rename({'directors':'directors_id', 'writers':'writers_id'}, axis=1)

        print('columns :', df.columns)

        # Limitation of existing films in title basics
        df = df[df['tconst'].isin(list_movies)]

        # Clean-up
        

        # # SQL Table : creation if not existing
        # if not 'imdb_titlecrew' in inspector.get_table_names():
        #     meta = MetaData()

        #     imdb_titlecrew = Table(
        #     'imdb_titlecrew', meta, 
        #     Column('tconst', String(15), primary_key=True), 
        #     Column('directors_id', String(255)), 
        #     Column('writers_id', String(255))
        #     ) 
        #     meta.create_all(engine)


        # Store data in MySQL DB
        df.to_sql('imdb_titlecrew', engine, if_exists='replace', index=False)

        # Deletion of df to save memory
        df = pd.DataFrame()
        df_existing_tconst = pd.DataFrame()
        del df
        del df_existing_tconst

        conn.close()
        engine.dispose()
        print('process done')

        return 0


def process_title_episode(source_path):
        # """
        # description
        # """
        # print('process started')

        # # Load
        # df = pd.read_csv(source_path,
        #             compression='gzip', 
        #             sep='\t', 
        #             na_values=['\\N', 'nan', 'NA', ' nan','  nan', '   nan']
        #             ) 

        # # Save
        # df.to_csv(destination_path, index=False, compression="zip")

        # # Deletion of df to save memory
        # df = pd.DataFrame()
        # del df

        # print('process done')


        return 0


def process_title_principal(source_path):
        # """
        # description
        # """
        # print('process started')

        # # Load
        # df = pd.read_csv(source_path,
        #             compression='gzip', 
        #             sep='\t', 
        #             na_values=['\\N', 'nan', 'NA', ' nan','  nan', '   nan']
        #             ) 

        # # Save
        # df.to_csv(destination_path, index=False, compression="zip")

        # # Deletion of df to save memory
        # df = pd.DataFrame()
        # del df

        # print('process done')


        return 0


def process_title_rating(source_path):
        # """
        # description
        # """
        # print('process started')

        # # Load
        # df = pd.read_csv(source_path,
        #             compression='gzip', 
        #             sep='\t', 
        #             na_values=['\\N', 'nan', 'NA', ' nan','  nan', '   nan']
        #             ) 

        # # Save
        # df.to_csv(destination_path, index=False, compression="zip")

        # # Deletion of df to save memory
        # df = pd.DataFrame()
        # del df

        # print('process done')


        return 0


def merge_content(source_path):
        """
        Merge of processed tables
        """
        print('merge_content started')

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
        query = """ SELECT * FROM imdb_titlebasics; """
        df_imdb_titlebasics = pd.read_sql(query, engine)

        query = """ SELECT * FROM imdb_titlecrew; """
        df_imdb_titlecrew = pd.read_sql(query, engine)


        # Merge
        df_merged = df_imdb_titlebasics.merge(right=df_imdb_titlecrew, left_on='tconst', right_on='tconst', how='inner')
        #df_merged = df_imdb_titlebasics

        # Temporary : NANs clean-up
        df_merged = df_merged.dropna(how='any', axis=0)


        # # SQL Table : creation if not existing
        # if not 'imdb_content' in inspector.get_table_names():
        #     meta = MetaData()

        #     imdb_content = Table(
        #     'imdb_content', meta, 
        #     Column('tconst', String(15), primary_key=True), 
        #     Column('titleType', String(150)), 
        #     Column('primaryTitle', String(150)),
        #     Column('startYear', Integer),
        #     Column('runtimeMinutes', Integer),
        #     Column('genres',  String(150)),
        #     Column('runtimeCategory',  String(2)),
        #     Column('yearCategory',  String(2))
        #     ) 

        #     meta.create_all(engine)

        # Store in MySQL DB
        df_merged.to_sql('imdb_content', engine, if_exists='replace', index=False)

        # Save Local
        df_merged.to_csv('/app/processed_data/imdb_content.csv.zip', index=False, compression="zip")

        conn.close()
        engine.dispose()
        print('merge_content done')
        return 0


# -------------------------------------- #
# TASKS
# -------------------------------------- #

task1 = PythonOperator(
    task_id='download',
    python_callable=download,
    dag=my_dag
)

task2 = PythonOperator(
    task_id='process_title_basics',
    python_callable=process_title_basics,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[0]},
    dag=my_dag
)

task3 = PythonOperator(
    task_id='process_name_basics',
    python_callable=process_name_basics,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[1]},
    dag=my_dag
)

task4 = PythonOperator(
    task_id='process_title_akas',
    python_callable=process_title_akas,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[2]},
    dag=my_dag
)

task5 = PythonOperator(
    task_id='process_title_crew',
    python_callable=process_title_crew,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[3]},
    dag=my_dag
)

task6 = PythonOperator(
    task_id='process_title_episode',
    python_callable=process_title_episode,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[4]},
    dag=my_dag
)

task7 = PythonOperator(
    task_id='process_title_principal',
    python_callable=process_title_principal,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[5]},
    dag=my_dag
)

task8 = PythonOperator(
    task_id='process_title_rating',
    python_callable=process_title_rating,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[6]},
    dag=my_dag
)

task9 = PythonOperator(
    task_id='merge_content',
    python_callable=merge_content,
    op_kwargs={'source_path':path_processed_data + processed_filenames[0]},
    trigger_rule=TriggerRule.ALL_DONE,
    dag=my_dag
)



# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #

task1 >> task2
task2 >> [task3, task4, task5, task6, task7, task8]
[task3, task4, task5, task6, task7, task8] >> task9


