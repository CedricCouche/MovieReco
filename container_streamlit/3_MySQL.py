import streamlit as st
import requests


st.markdown('# MySQL Info')


# Variables

mysql = { "database_name": "db_movie", 
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