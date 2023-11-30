# Projet Movie-Recommandation

Version : 1.1.0

## What is Movie-Recommandation project ?


The main goal is to deploy a solution using MLOps, such as Docker and Airflow.
The solution provide a list of 10 recommanded movies, based on one movie provided by the user.
Several models will be implemented. The first ot this model is cosine similarity.
Approach choosen is content-based, so recommanded movies are choosen on similarites on a defined intrisec characteristic.
Main goal is to implement MLOps technologies, not to get the most relevant recommandation.


## Architecture

The solution is contained in a Airflow docker-compose.
Airflow is used as a pipeline from download to pre-processing and as last recommandation computation.
Data and recommandations are hosted in a MySQL container.
A FastAPI container provide a secured access to recommandations.
At last, a streamlit container is available as front to users.


## Data

- [https://www.imdb.com/interfaces/](https://www.imdb.com/interfaces/)

For this project, and due to limited computer ressources, we used the table contained in the file title.basics.tsv.gz, title.crew.tsv.gz and title.ratings.tsv.gz. 

## DAGs

There are 4 DAGs :
- 00 Initial Load : this dags covers download of file and preprocess of a limited number of them, to initialise MySQL tables. to be triggered only at startup.
- 01 Recurrent download : download of IMDB files, triggered every days
- 02 Recurrent Preprocess : Preprocess of new movies by batch, triggered every days
- 03 Model 1 cosine similarity : computation of score for all movies available in MySQL, triggered every days

DAG00 Initial Load
![DAG00 - Initial Load](https://github.com/CedricCouche/MovieReco/blob/main/images/dag_initial-load.png)

DAG02 PreProcess
![DAG02 - PreProcess](https://github.com/CedricCouche/MovieReco/blob/main/images/dag_movie-to-process.png)

DAG03 Cosine-Similarity
![DAG03 - Cosine-Similarity](https://github.com/CedricCouche/MovieReco/blob/main/images/dag_cosine-similarity.png)


## Models


### Cosine Similarity

Our first model, cosine similarity, was choose for its ease of implementation and computation. It has also the advantage to be computed within a dags, so it is ligther to process for API.

Several fields are concatenated to build a single field, that will be tokenized to form our vector to computed against other movies.
Fields used to create this combined features : ['primaryTitle','titleType', 'genres', 'runtimeCategory', 'yearCategory','directors_id', 'writers_id']


## How to install on distant machine (Ubuntu / Debian) ?

For information, OS used for this project is Ubuntu 22.04 (LTS)

Recommanded configuration : 2-core CPU and 16G of RAM at least.


#### Repository clone

``` 
git clone https://github.com/CedricCouche/MovieReco.git
```

#### General purpose Linux packages

Some linux packages a required

```
sudo apt update && apt upgrade
sudo apt install pip
```

#### Installation of Docker

To install Docker, please refers to tutorial provided by Docker : https://docs.docker.com/engine/install/

Other packages:
```
sudo apt install docker-compose
```


#### Linux packages installation for MySQL

SQLAlchemy python package requires  mysqlclient and  mysql-connector-python packages, but both packages requires to be built, and requires some additionnals packages to be installed on linux

```
sudo apt install build-essential libssl-dev python3-dev default-libmysqlclient-dev

# if last package is not found, try this one : 
sudo apt install libmysqlclient-dev

# Package to be interact direclty with MySQL
sudo apt install mysql-client-core-8.0
```


#### Python version set-up using PyEnv

To ensure consitency and reliability, python version is defined.
This project is based on python==3.11.05
To manage python version, we are using pyenv.


Please refers to pyenv official installation guide here: https://github.com/pyenv/pyenv#installation

Here are some additionnal packages to install & compile et python version:
```
sudo apt install build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev curl \
libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev
```

At last, we can download and define ou python version:
```cd
# Installation of Python 3.11.05
pyenv install 3.11.05
cd ~/MovieReco && pyenv local 3.11.05
```



#### Virtual environnement set-up

```
cd ~/MovieReco && python3 -m venv .venv
source .venv/bin/activate
```

#### Python Packages installation

``` bash
pip install -r requirements.txt
```


#### Permission changes

```
sudo chmod -R 777 logs/ raw_data/ processed_data/ db_dump/ api_logs/
```

#### Airflow initialisation

Airflow requires an initialisation phase.

```
sudo service docker start
sudo docker-compose up airflow-init
```

If everything went well, last line should display "mlops-movie-recommandation_airflow-init_1 exited with code 0"

#### Airflow start

Start of container in a detached mode
```
sudo docker-compose up -d
```

#### SSH Tunnel initialisation

- Airflow interface will be available on port 8080
- FastAPI is available on port 8000
- Streamlit App is available on port 8501

Tunnel SSH initialisation :
``` bash
# From your local machine :
ssh -i "your_key.pem" -L 8000:localhost:8000 -L 8080:localhost:8080 -L 8501:localhost:8501 user@server-ip-address

```

#### Airflow Connection

- login: airflow
- password: airflow


#### Airflow variables load

Variables are required to run DAGs, thy are store in the file dags_variables.json
In Airflow web-interface, go in menu Admin > Variables
Import the file dag_variables.json


#### Airflow Dags

Airflow is now ready, and in Dags tab you should see several dags.

The first to start is "initialisation", It will download files from IMDB and perform pre-process.
Once done, dag cosine similarity has to be executed.
dags download, preprocess and cosine similarity can be left active, as they are periodically triggers to refresh data & scores. 


## Screen Captures

Fast API :
![FastAPI](https://github.com/CedricCouche/MovieReco/blob/main/images/fastapi-doc.png)

Streamlit
![Streamlit](https://github.com/CedricCouche/MovieReco/blob/main/images/streamlit-recommendation.png)