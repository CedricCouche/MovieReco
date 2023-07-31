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
    dag_id='D01_Initial_Load_v02',
    description='Initial_Load',
    tags=['Initialisation', 'Process_D'],
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
        df = df[df['startYear']<1950.0]
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

        # Store data in MySQL DB
        df.to_sql('imdb_titlebasics', engine, if_exists='replace', index=False)

        # Save in CSV
        # df.to_csv(destination_path, index=False, compression="zip")


        # Deletion of df to save memory
        df = pd.DataFrame()
        del df

        # Closing MySQL connection
        conn.close()
        engine.dispose()
        print('process_title_basics done')

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
        

        # Store data in MySQL DB
        df.to_sql('imdb_titlecrew', engine, if_exists='replace', index=False)

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


def nconst_to_process():
        """
        description : return a list of nconst to process
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

        # Load
        query = """ SELECT directors_id, writers_id FROM imdb_titlecrew; """
        df_titlecrew = pd.read_sql(query, engine)
        
        # List of nconst to load
        df_directors = pd.DataFrame()
        df_writers = pd.DataFrame()
        df_directors['nconst'] = df_titlecrew['directors_id']
        df_writers['nconst']  = df_titlecrew['writers_id']

        df = pd.concat([df_directors, df_writers], axis=0)
        df = df.drop_duplicates(ignore_index=True)
        df = df['nconst']

        list_nconst_A = df_titlecrew['directors_id'].tolist() + df_titlecrew['writers_id'].tolist()
        set_nconst_A = set(list_nconst_A)
        df_test = pd.DataFrame(set_nconst_A,columns=['nconst'])
        print(df_test.head(3))

        # Temp
        print('Nb df_directors:', df_directors.shape[0])
        print('Nb df_writers:', df_writers.shape[0])
        print('Nb nconst to add:', df.shape[0])

        # Store data in MySQL DB
        df.to_sql('nconst_to_process', engine, if_exists='replace', index=False)

        # Deletion of df to save memory
        df              = pd.DataFrame()
        df_titlecrew    = pd.DataFrame()
        df_directors    = pd.DataFrame()
        df_writers      = pd.DataFrame()
        del df
        del df_titlecrew
        del df_directors
        del df_writers
  
        # Closing MySQL connection
        conn.close() 
        engine.dispose()
        print('process done')

        return 0


def process_name_basics(source_path):
        """
        description

        Original fields from imdb : 
            - nconst (string) - alphanumeric unique identifier of the name/person
            - primaryName (string)– name by which the person is most often credited
            - birthYear – in YYYY format
            - deathYear – in YYYY format if applicable, else '\\N'
            - primaryProfession (array of strings)– the top-3 professions of the person
            - knownForTitles (array of tconsts) – titles the person is known for
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

        # Load list of movies to process
        query = """ SELECT nconst FROM nconst_to_process; """
        df_nconst = pd.read_sql(query, engine)
        nconst_to_process = df_nconst['nconst'].tolist()

        # Load
        column_list = ['nconst', 'primaryName']
        dict_types = {'nconst':object, 'primaryName':object}

        df = pd.read_csv(source_path,
                    compression='gzip', 
                    sep='\t', 
                    usecols= column_list,
                    dtype=dict_types,
                    na_values=['\\N', 'nan', 'NA', ' nan','  nan', '   nan']
                    ) 

        print('original columns : ', df.columns)
        print('Number of persons: ', df.shape[0])

        # Limitation of existing films in title basics
        df = df[df['nconst'].isin(nconst_to_process)]
        df = df[['nconst', 'primaryName']]

        # Clean-up
        

        # Store data in MySQL DB
        df.to_sql('imdb_namebasics', engine, if_exists='replace', index=False)

        # Deletion of df to save memory
        df = pd.DataFrame()
        df_nconst = pd.DataFrame()
        del df
        del df_nconst 

        # Closing MySQL connection
        conn.close()
        engine.dispose()
        print('process done')

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
        """
        description
        
        Original fields from imdb : 
            tconst (string) - alphanumeric unique identifier of the title
            averageRating – weighted average of all the individual user ratings
            numVotes - number of votes the title has received

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


        # Limitation of existing films in title basics
        df = df[df['tconst'].isin(list_movies)]

        # Clean-up
        

        # Store data in MySQL DB
        df.to_sql('imdb_titleratings', engine, if_exists='replace', index=False)

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


        # Load
        query = """ SELECT * FROM imdb_titlebasics; """
        df_imdb_titlebasics = pd.read_sql(query, engine)

        query = """ SELECT * FROM imdb_titlecrew; """
        df_imdb_titlecrew = pd.read_sql(query, engine)

        query = """ SELECT * FROM imdb_titleratings; """
        df_imdb_titleratings = pd.read_sql(query, engine)

        # Merge
        df_imdb_content = df_imdb_titlebasics.merge(right=df_imdb_titlecrew, left_on='tconst', right_on='tconst', how='inner')
        df_imdb_content = df_imdb_content.merge(right=df_imdb_titleratings, left_on='tconst', right_on='tconst', how='inner')

        # Temporary : NANs clean-up
        df_imdb_content = df_imdb_content.dropna(how='any', axis=0)


        # Store in MySQL DB
        df_imdb_content.to_sql('imdb_content', engine, if_exists='replace', index=False)

        # Save Local (For testing purpose)
        df_imdb_content.to_csv('/app/processed_data/imdb_content.csv.zip', index=False, compression="zip")

        # Deletion of df to save memory
        df_imdb_titlebasics = pd.DataFrame()
        df_imdb_titlecrew   = pd.DataFrame()
        df_imdb_titleratings= pd.DataFrame()
        df_imdb_content     = pd.DataFrame()

        del df_imdb_titlebasics
        del df_imdb_titlecrew
        del df_imdb_titleratings
        del df_imdb_content

        # Closing MySQL connection
        conn.close()
        engine.dispose()

        print('merge_content done')
        
        return 0


# -------------------------------------- #
# TASKS
# -------------------------------------- #

task01 = PythonOperator(
    task_id='download',
    python_callable=download,
    dag=my_dag
)

task02 = PythonOperator(
    task_id='process_title_basics',
    python_callable=process_title_basics,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[0]},
    dag=my_dag
)

task03 = PythonOperator(
    task_id='process_title_crew',
    python_callable=process_title_crew,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[3]},
    dag=my_dag
)

task04 = PythonOperator(
    task_id='nconst_to_process',
    python_callable=nconst_to_process,
    dag=my_dag
)

task05 = PythonOperator(
    task_id='process_name_basics',
    python_callable=process_name_basics,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[1]},
    dag=my_dag
)

task06 = PythonOperator(
    task_id='process_title_akas',
    python_callable=process_title_akas,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[2]},
    dag=my_dag
)

task07 = PythonOperator(
    task_id='process_title_episode',
    python_callable=process_title_episode,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[4]},
    dag=my_dag
)

task08 = PythonOperator(
    task_id='process_title_principal',
    python_callable=process_title_principal,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[5]},
    dag=my_dag
)

task09 = PythonOperator(
    task_id='process_title_rating',
    python_callable=process_title_rating,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[6]},
    dag=my_dag
)

task10 = PythonOperator(
    task_id='merge_content',
    python_callable=merge_content,
    op_kwargs={'source_path':path_processed_data + processed_filenames[0]},
    trigger_rule=TriggerRule.ALL_DONE,
    dag=my_dag
)



# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #

task01 >> task02
task02 >> task03
task03 >> task04
task04 >> [task05, task06, task07, task08, task09]
[task05, task06, task07, task08, task09] >> task10


