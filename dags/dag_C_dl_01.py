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
from sqlalchemy import create_engine, text
from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, inspect
from sqlalchemy_utils import database_exists, create_database


from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split

from tensorflow.keras.layers import Input, Dense
from tensorflow.keras.models import Model
from sklearn.metrics import classification_report,confusion_matrix


# -------------------------------------- #
# DAG
# -------------------------------------- #


my_dag = DAG(
    dag_id='histGBC_01',
    description='histGBC_01',
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


# mysql_url = 'container_mysql:3306'
# mysql_user = 'root'
# mysql_password = 'my-secret-pw'
# database_name = 'db_movie'

mysql_url       = Variable.get("mysql_url")
mysql_user      = Variable.get("mysql_user")
mysql_password  = Variable.get("mysql_pw")
database_name   = Variable.get("database_name")


# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #


def dl_A(top_n):
        """
        This function build the combined feature that will be used for cosine similarity
        """

        print('Model dl started')

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
        list_cols = ['primaryTitle','titleType', 'genres', 'runtimeCategory', 'yearCategory', 'directors_id ', 'writers_id']
        df['combined_features'] = df[list_cols].apply(lambda x: ' '.join(x), axis=1)

        cols_to_drop = ['primaryTitle','titleType', 'genres', 'runtimeCategory', 'yearCategory', 'startYear', 'runtimeMinutes']
        df = df.drop(columns=cols_to_drop, axis=1)

        # Tokenization
        cv = CountVectorizer()
        count_matrix = cv.fit_transform(df["combined_features"])


        # Test / Train split
        inputs = Input(shape = (10), name = "Input")

        dense1 = Dense(units = 10, activation = "tanh", name = "Dense_1")
        dense2 = Dense(units = 8, activation = "tanh", name = "Dense_2")
        dense3 = Dense(units = 6, activation = "tanh", name = "Dense_3")
        dense4 = Dense(units = 3, activation = "softmax", name = "Dense_4")

        x=dense1(inputs)
        x=dense2(x)
        x=dense3(x)
        outputs=dense4(x)

        model = Model(inputs = inputs, outputs = outputs)
        model.summary()


        model.compile(loss = "sparse_categorical_crossentropy",
              optimizer = "adam",
              metrics = ["accuracy"])

        model.fit(X_train,y_train,epochs=500,batch_size=32,validation_split=0.1)

        y_pred = model.predict(X_test)

        test_pred = model.predict(X_test)
        y_test_class = y_test
        y_pred_class = np.argmax(test_pred,axis=1)

        
        print(classification_report(y_test_class,y_pred_class))
        print(confusion_matrix(y_test_class,y_pred_class))

        # Store data in MySQL DB
        df_score.to_sql('score_histGBC', engine, if_exists='replace', index=False)

        conn.close()
        engine.dispose()

        print('Model dl done')
        return 0




# -------------------------------------- #
# TASKS
# -------------------------------------- #


task1 = PythonOperator(
    task_id='dl',
    python_callable=dl_A,
    op_kwargs={'top_n':10},
    dag=my_dag
)


# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #




