# -------------------------------------- #
# Imports
# -------------------------------------- #

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import datetime

import pandas as pd
import requests
import numpy as np
import string

import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, create_engine, text, inspect
from sqlalchemy_utils import database_exists, create_database


# -------------------------------------- #
# DAG
# -------------------------------------- #

my_dag = DAG(
    dag_id='download_C01',
    description='download_C01',
    tags=['download', 'Process_C'],
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


def process_title_basics(source_path, destination_path):
        """
        This function clean the file title_basics, discretise some fields and reduce the size of the dataset
        """
        print('process started')

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
        df = df[df['startYear']>2010.0]
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


        # Save in CSV
        df# .to_csv(destination_path, index=False, compression="zip")

        print('process done')


        # Connection to MySQL
        connection_url = 'mysql://{user}:{password}@{url}/{database}'.format(
            user=mysql_user,
            password=mysql_password,
            url=mysql_url,
            database = database_name
            )

        engine = create_engine(connection_url)
        inspector = inspect(engine)

        # SQL Table : creation if not existing
        if not 'test_c' in inspector.get_table_names():
            meta = MetaData()

            test_c = Table(
            'test_c', meta, 
            Column('tconst', String(15), primary_key=True), 
            Column('titleType', String(150)), 
            Column('primaryTitle', String(150)),
            Column('startYear', Integer),
            Column('runtimeMinutes', Integer),
            Column('genres',  String(150)),
            Column('runtimeCategory',  String(2)),
            Column('yearCategory',  String(2)),
            Column('combined_features',  String(255))
            ) 

            meta.create_all(engine)
            print('table created')


        # Store data in MySQL DB
        df.to_sql('test_c', engine, if_exists='replace', index=False)


        # check
        query = """ SELECT * FROM test_c LIMIT 5; """
        df_check = pd.read_sql(query, engine)
        print(df_check.head(5))


        # Deletion of df to save memory
        df = pd.DataFrame()
        del df

        print('load in mysql done')

        return 0


def process_name_basics(source_path, destination_path):
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


def process_title_akas(source_path, destination_path):
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


def process_title_crew(source_path, destination_path):
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


def process_title_episode(source_path, destination_path):
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


def process_title_principal(source_path, destination_path):
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


def process_title_rating(source_path, destination_path):
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


def merge_content(source_path, destination_path):
        """
        Merge of processed tables
        """
        print('merge started')

        # Load
        column_list = ['tconst', 'titleType', 'primaryTitle', 'startYear', 'runtimeMinutes', 'genres', 'runtimeCategory', 'yearCategory']
        dict_types = {'tconst':object, 'titleType':object, 'primaryTitle':object, 'startYear':int, 'runtimeMinutes':int, 'genres':object, 'runtimeCategory':object, 'yearCategory':object}

        title_basics = pd.read_csv(source_path,
            usecols= column_list,
            compression='zip',
            sep= ',',
            dtype=dict_types)

        # Merge
        imdb_content = title_basics


        # Temporary : NANs clean-up
        imdb_content = imdb_content.dropna(how='any', axis=0)

        # Save
        imdb_content.to_csv(destination_path, index=False, compression="zip")

        print('merge done')
        return 0


def feature_build(source_path, destination_path):
        """
        This function build the combined feature that will be used for cosine similarity
        """

        print('combined features started')

        # Load
        column_list = [
            'tconst', 
            'titleType', 
            'primaryTitle', 
            'startYear', 
            'runtimeMinutes', 
            'genres', 
            'runtimeCategory', 
            'yearCategory']
        dict_types = {
            'tconst':object, 
            'titleType':object, 
            'primaryTitle':object, 
            'startYear':int, 
            'runtimeMinutes':int, 
            'genres':object, 
            'runtimeCategory':object, 
            'yearCategory':object
            }

        df = pd.read_csv(source_path,
            usecols= column_list,
            compression='zip',
            sep= ',',
            dtype=dict_types)


        # Feature build
        list_cols = ['primaryTitle','titleType', 'genres', 'runtimeCategory', 'yearCategory']
        df['combined_features'] = df[list_cols].apply(lambda x: ' '.join(x), axis=1)

        # Save
        df.to_csv(destination_path, index=False, compression="zip")

        print('combined features done')

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
    op_kwargs={'source_path':path_raw_data + imdb_files_names[0], 'destination_path':path_processed_data + processed_filenames[0]},
    dag=my_dag
)

task3 = PythonOperator(
    task_id='process_name_basics',
    python_callable=process_name_basics,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[1], 'destination_path':path_processed_data + processed_filenames[1]},
    dag=my_dag
)

task4 = PythonOperator(
    task_id='process_title_akas',
    python_callable=process_title_akas,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[2], 'destination_path':path_processed_data + processed_filenames[2]},
    dag=my_dag
)

task5 = PythonOperator(
    task_id='process_title_crew',
    python_callable=process_title_crew,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[3], 'destination_path':path_processed_data + processed_filenames[3]},
    dag=my_dag
)

task6 = PythonOperator(
    task_id='process_title_episode',
    python_callable=process_title_episode,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[4], 'destination_path':path_processed_data + processed_filenames[4]},
    dag=my_dag
)

task7 = PythonOperator(
    task_id='process_title_principal',
    python_callable=process_title_principal,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[5], 'destination_path':path_processed_data + processed_filenames[5]},
    dag=my_dag
)

task8 = PythonOperator(
    task_id='process_title_rating',
    python_callable=process_title_rating,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[6], 'destination_path':path_processed_data + processed_filenames[6]},
    dag=my_dag
)

task9 = PythonOperator(
    task_id='merge_content',
    python_callable=merge_content,
    op_kwargs={'source_path':path_processed_data + processed_filenames[0], 'destination_path':path_processed_data + processed_filenames[7]},
    dag=my_dag
)

task10 = PythonOperator(
    task_id='feature_build',
    python_callable=feature_build,
    op_kwargs={'source_path':path_processed_data + processed_filenames[7], 'destination_path':path_processed_data + processed_filenames[8]},
    dag=my_dag
)


# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #

task1 >> [task2, task3, task4, task5, task6, task7, task8]
[task2, task3, task4, task5, task6, task7, task8] >> task9
task9 >> task10

