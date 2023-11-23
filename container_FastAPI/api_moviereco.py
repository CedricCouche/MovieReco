# -------------------------------------- #
#                Imports                 #
# -------------------------------------- #

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


# -------------------------------------- #
#           MySQL Connection             #
# -------------------------------------- #


mysql_url = os.environ.get('MYSQL_URL')
mysql_user = os.environ.get('MYSQL_USER')
mysql_password = os.environ.get('MYSQL_ROOT_PASSWORD')
database_name = os.environ.get('MYSQL_DATABASE')
table_users = os.environ.get('MYSQL_TABLE_USERS')
table_movies = os.environ.get('MYSQL_TABLE_MOVIES')
table_recocs = os.environ.get('MYSQL_TABLE_CS')


# Creating the URL connection
connection_url = 'mysql://{user}:{password}@{url}/{database}'.format(
    user = mysql_user,
    password = mysql_password,
    url = mysql_url,
    database = database_name
)

# -------------------------------------- #
#         Function definition            #
# -------------------------------------- #

def convert_df_to_json(df):
    return Response(df.to_json(orient="records"), media_type="application/json")


# -------------------------------------- #
#            Pydantic class              #
# -------------------------------------- #

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
    directors_id: str
    writers_id: str
    averageRating: float
    numVotes: int

class RecoCS(BaseModel):
    tconst: str
    top1: str
    top2: str
    top3: str
    top4: str
    top5: str
    top6: str
    top7: str
    top8: str
    top9: str
    top10: str
    score1: float
    score2: float
    score3: float
    score4: float
    score5: float
    score6: float
    score7: float
    score8: float
    score9: float
    score10: float

class RecoTest(BaseModel):
    tconst: str
    titles: list[str]
    scores: list[float]

# -------------------------------------- #
#           API INITIALISATION           #
# -------------------------------------- #

api = FastAPI(
    title="Movie recommendation",
    description="Content based Movie recommendation",
    version="1.0.0",
    openapi_tags=[
              {'name':'Info', 'description':'Info'},
              {'name':'Recommandation','description':'Get recommendation'}, 
              {'name':'Admin', 'description':'Staff only'} 
             ]
)

# -------------------------------------- #
#            SECURITY : ADMIN            #
# -------------------------------------- #

API_KEY = os.environ.get('API_KEY')
API_KEY_NAME = os.environ.get('API_KEY_NAME')


api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    if api_key_header == API_KEY:
        return api_key_header
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Could not validate credentials"
    )


# -------------------------------------- #
#            SECURITY : USERS            #
# -------------------------------------- #

security = HTTPBasic()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

users_b = {
    "Admin": {
        "username": "admin",
        "name": "Admin",
        'role' : ['admin'],
        "hashed_password": pwd_context.hash('Admin'),
    },
    "Streamlit": {
        "username": "Streamlit",
        "name": "Streamlit",
        'role' : ['user'],
        "hashed_password": pwd_context.hash('Streamlit'),
    },
    "Dominique": {
        "username": "Dominique",
        "name": "Dominique",
        'role' : ['user'],
        "hashed_password": pwd_context.hash('Dominique'),
    },
    "Diane": {
        "username": "Diane",
        "name": "Diane",
        'role' : ['user'],
        "hashed_password": pwd_context.hash('Diane'),
    }
}

def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    username = credentials.username
    if not(users_b.get(username)) or not(pwd_context.verify(credentials.password, users_b[username]['hashed_password'])):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect ID or password",
            headers={"WWW-Authenticate": "Basic"},
        )
        

# -------------------------------------- #
#               API Routes               #
# -------------------------------------- #


@api.get('/status', tags=['Info']) 
async def get_status(): 
    return 200


@api.get('/welcome',  name="Return a list of users", response_model=User, tags=['Info'])
async def get_users(username: str = Depends(get_current_user)):
    """ 
    Return the list of users
    """
    # Creating MySQL connection
    engine = create_engine(connection_url, echo=True)
    conn = engine.connect()

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
async def get_filminfo(tconst, username: str = Depends(get_current_user)):
    """ 
    Return information on a film
    """

    # Creating MySQL connection
    engine = create_engine(connection_url, echo=True)
    conn = engine.connect()

    stmt = 'SELECT * FROM {table} WHERE tconst = {tconst};'.format(table=table_movies, tconst=text(tconst))

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
            directors_id=i[8],
            writers_id=i[9],
            averageRating=i[10],
            numVotes=i[11]
            ) for i in results.fetchall()]

    if len(results) == 0:
        # Closing MySQL connection
        conn.close()
        engine.dispose()

        # Error handling
        raise HTTPException(
            status_code=404,
            detail='Unknown movie')
    else:
        # Closing MySQL connection
        conn.close()
        engine.dispose()
                
        return results[0]


@api.get('/get_reco_cs01/{tconst:str}', name="Return a list of similar movies" , tags=['Recommandation'])
async def get_recommendation(tconst:str, username: str = Depends(get_current_user)):
    """ 
    Return a list of similar movies
    """

    # Creating MySQL connection
    engine = create_engine(connection_url, echo=True)
    conn = engine.connect()

    #stmt = 'SELECT * FROM {table} WHERE tconst={tconst};'.format(table=table_recocs, tconst=text(tconst))
    stmt = 'SELECT * FROM {table} WHERE tconst={tconst};'.format(table=table_recocs, tconst=str(tconst))

    with engine.connect() as connection:
        results = connection.execute(text(stmt))

        results = [
            RecoCS(
                tconst=i[0],
                top1=i[1],
                top2=i[2],
                top3=i[3],
                top4=i[4],
                top5=i[5],
                top6=i[6],
                top7=i[7],
                top8=i[8],
                top9=i[9],
                top10=i[10],
                score1=i[11],
                score2=i[12],
                score3=i[13],
                score4=i[14],
                score5=i[15],
                score6=i[16],
                score7=i[17],
                score8=i[18],
                score9=i[19],
                score10=i[20]
                ) for i in results.fetchall()]

    if len(results) == 0:
        # Closing MySQL connection
        conn.close()
        engine.dispose()

        # Error handling
        raise HTTPException(
            status_code=404,
            detail='Unknown movie')
    else:
        # Closing MySQL connection
        conn.close()
        engine.dispose()
                
        return results[0]


@api.get('/get_reco_cs02/{tconst:str}', name="Return a list of similar movies" , tags=['Recommandation'])
async def get_recommendation(tconst:str, username: str = Depends(get_current_user)):
    """ 
    Return a list of similar movies
    """

    # Creating MySQL connection
    engine = create_engine(connection_url, echo=True)
    conn = engine.connect()

    stmt = 'SELECT * FROM {table} WHERE tconst={tconst};'.format(table=table_recocs, tconst=str(tconst))

    with engine.connect() as connection:
        results = connection.execute(text(stmt))

        results = [
            RecoTest(
                tconst = i[0],
                titles = [ i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10] ],
                scores = [ i[11], i[12],i[13],i[14],i[15],i[16],i[17],i[18],i[19],i[20] ]
                ) for i in results.fetchall()]

    if len(results) == 0:
        # Closing MySQL connection
        conn.close()
        engine.dispose()

        # Error handling
        raise HTTPException(
            status_code=404,
            detail='Unknown movie')
    else:
        # Closing MySQL connection
        conn.close()
        engine.dispose()
    
        return results[0]


@api.get('/get-films-list/{number:int}', name="get-films-list" , tags=['Info'])
async def list_films(number:int, username: str = Depends(get_current_user)):
    """ 
    get a list of films
    """
    # Creating MySQL connection
    engine = create_engine(connection_url, echo=True)
    conn = engine.connect()

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
            directors_id=i[8],
            writers_id=i[9],
            averageRating=i[10],
            numVotes=i[11]
            ) for i in results.fetchall()]


    # Closing MySQL connection
    conn.close()
    engine.dispose()

    return results



