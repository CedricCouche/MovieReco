
# ---------- Imports ---------- #

from fastapi import FastAPI, status, Header, Response, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from fastapi.responses import JSONResponse
import os
import time

from fastapi import Depends, Security
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from passlib.context import CryptContext
from fastapi.security.api_key import APIKeyHeader, APIKey

import pandas as pd
import zipfile as zipfile


import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, create_engine, text, inspect, select
from sqlalchemy_utils import database_exists, create_database


# ---------- MySQL Connection ---------- #


mysql_url = 'container_mysql:3306'
mysql_user = os.environ.get('MYSQL_USER')
mysql_password = os.environ.get('MYSQL_ROOT_PASSWORD')
database_name = os.environ.get('MYSQL_DATABASE')
table_users = os.environ.get('MYSQL_TABLE_USERS')
table_movies =  os.environ.get('MYSQL_TABLE_MOVIES')


# Creating the URL connection
connection_url = 'mysql://{user}:{password}@{url}/{database}'.format(
    user=mysql_user,
    password=mysql_password,
    url=mysql_url,
    database = database_name
)




# ---------- Function definition ---------- #


def convert_df_to_json(df):
    return Response(df.to_json(orient="records"), media_type="application/json")


# ---------- Load data for recommandation ---------- #


# # Load data from MySQL
# stmt = 'SELECT tconst, combined_features FROM {table};'.format(table=table_movies)
# df = pd.read_sql(sql=text(stmt), con=conn)


# # Load users
# stmt = 'SELECT * FROM {table};'.format(table=table_users)
# df_users = pd.read_sql(sql=text(stmt), con=conn)


# ---------- Pydantic class ---------- #


class User(BaseModel):
    user_id: str
    username: str
    role:str
    password: str
    email: str

class Movie(BaseModel):
    tconst: str
    titleType: str
    primaryTitle: str
    startYear:int
    runtimeMinutes:int
    genres: str
    runtimeCategory: str
    yearCategory: str
    combined_features: str


# ---------- API INITIALISATION ---------- #


api = FastAPI(
    title="Movie recommendation",
    description="Content based Movie recommendation",
    version="1.5.9",
    openapi_tags=[
              {'name':'Info', 'description':'Info'},
              {'name':'MovieReco','description':'Get recommendation'}, 
              {'name':'Admin', 'description':'Staff only'} 
             ]
)


# ---------- SECURITY : ADMIN ---------- #




# ---------- SECURITY : USERS ---------- #




# ---------- API Routes ---------- #


@api.get('/status', tags=['Info']) 
async def get_status(): 
    return 200


@api.get('/welcome',  name="Return a list of users", response_model=User, tags=['Info'])
async def get_users():
    """ 
    Return the list of users
    """
    # Creating MySQL connection
    engine = create_engine(connection_url, echo=True)
    conn = engine.connect()
    inspector = inspect(engine)


    stmt = 'SELECT * FROM {table};'.format(table=table_users)

    with engine.connect() as connection:
        results = connection.execute(text(stmt))

    results = [
        User(
            user_id=i[0],
            username=i[1],
            role=i[2],
            password=i[3],
            email=i[4]
            ) for i in results.fetchall()]
    
    # Closing MySQL connection
    conn.close()
    engine.dispose()
    
    return results[1]



@api.get('/get-film-info/{tconst:str}', name="Return information on a film" , response_model=Movie, tags=['Info'])
async def list_genres(tconst):
    """ 
    Return information on a film
    """

    # Creating MySQL connection
    engine = create_engine(connection_url, echo=True)
    conn = engine.connect()
    inspector = inspect(engine)


    stmt = 'SELECT * FROM {table} WHERE tconst = {tconst};'.format(table=table_movies, tconst=tconst)

    with engine.connect() as connection:
        results = connection.execute(text(stmt))

    results = [
        Movie(
            tconst=i[0],
            titleType=i[1],
            primaryTitle=i[2],
            startYear=i[3],
            runtimeMinutes=i[4],
            genres=i[5],
            runtimeCategory=i[6],
            yearCategory=i[7],
            combined_features=i[8]
            ) for i in results.fetchall()]

    if len(results) == 0:
        raise HTTPException(
            status_code=404,
            detail='Unknown movie')
    else:
        # Closing MySQL connection
        conn.close()
        engine.dispose()
                
        return results[0]


@api.get('/get_reco_cs/{movie_user_title:str}', name="Return a list of similar movies" , tags=['Recommandation'])
async def get_recommendation(movie_user_title:str):
    """ 
    Return a list of similar movies
    """

    list_titles = ['good', 'bad', 'ugly']

    return list_titles



@api.get('/get-films-list/{number:int}', name="get-films-list" , tags=['Info'])
async def list_films(number:int):
    """ 
    get a list of films
    """
    # Creating MySQL connection
    engine = create_engine(connection_url, echo=True)
    conn = engine.connect()
    inspector = inspect(engine)

    stmt = 'SELECT * FROM {table} LIMIT {number};'.format(table=table_movies, number=number)

    with engine.connect() as connection:
        results = connection.execute(text(stmt))

    results = [
        Movie(
            tconst=i[0],
            titleType=i[1],
            primaryTitle=i[2],
            startYear=i[3],
            runtimeMinutes=i[4],
            genres=i[5],
            runtimeCategory=i[6],
            yearCategory=i[7],
            combined_features=i[8]
            ) for i in results.fetchall()]

    
    # Closing MySQL connection
    conn.close()
    engine.dispose()

    return results



