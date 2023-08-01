import streamlit as st
import requests

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, inspect
from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, text 


st.markdown('# MySQL Info')


# Variables

mysql = {"database_name": "db_movie", 
            "password": "my-secret-pw", 
            "url": "container_mysql:3306", 
            "user": "root" }


# Connection to MySQL
connection_url = 'mysql://{user}:{password}@{url}/{database}'.format(
    user        = mysql['user'],
    password    = mysql['password'],
    url         = mysql['url'],
    database    = mysql['database_name']
    )

engine = create_engine(connection_url)
conn = engine.connect()
inspector = inspect(engine)

st.markdown('## DataBase Info')


list_tables = inspector.get_table_names()
st.write('list of tables : ', list_tables)


st.markdown('## Tables Info')


st.markdown('### imdb_content')

query = """ SELECT * FROM imdb_content LIMIT 10; """
df_imdb_content = pd.read_sql(query, engine)
st.table(df_imdb_content)