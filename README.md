# Projet Movie-Recommandation


## What is Movie-Recommandation project ?

This project is realized in team during a MLOps training provided by [DataScientest](https://datascientest.com/).
The main goal is to deploy a solution using MLOps techniques we have learn.

The solution provide a list of 10 recommanded movies, based on one movie provided by the user.
Approach choosen by the team is content-based, so recommanded movies are choosen on similarites on a defined intrisec characteristic.
Similarity between movies are computed using a cosine similarity


## Architecture

The solution is contained in a Airflow docker-compose.
Airflow is used as a pipeline pre-processing, from download to data-base storage.
Data are hosted in a MySQL container.
A FastAPI container is containing cosine-similarity calculation, based on data retrieved from MySQL container.


## Data

- [https://www.imdb.com/interfaces/](https://www.imdb.com/interfaces/)

For this project, and due to limited computer ressources, we used the table contained in the file title.basics.tsv.gz

Our final table used for recommandation is the one below :
- tconst : movie id from IMDB
- titleType
- primaryTitle
- genres
- RuntimeCategory
- YearCategory 
- combined-features : this field concatenante several fields, and after a tokenization step, will become the vector for cosine-similarity computation



## How to install on distant machine (Ubuntu / Debian) ?

For information, OS used for this project is Ubuntu 22.04 (LTS)

Recommanded configuration : 2-core CPU and 16G of RAM


#### Repository clone

``` 
git clone https://github.com/CedricCouche/MovieReco.git
```

#### Python version set-up

This project is base on python==3.7.10

```
# Install de PIP (Prerequisite to install PyEnv)
sudo apt install pip

# Clone PyEnv repository
git clone https://github.com/pyenv/pyenv.git ~/.pyenv # pyenv est isntallé dans cedric

# compile :
cd ~/.pyenv && src/configure && make -C src

# Add commands to its .profile (often stored /home/user) : it can change from one linux distrib to anther, please check github for .bach_profile or others
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.profile
echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.profile
echo 'eval "$(pyenv init -)"' >> ~/.profile
exec "$SHELL"

sudo apt install build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev curl \
libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev
```

Reboot the distant machine

```
# Installation of Python 3.8.10
pyenv install 3.8.10
cd ~/MovieReco && pyenv local 3.8.10
```

#### Linux packages installation

SQLAlchemy python package requires  mysqlclient and  mysql-connector-python packages, but both packages requires to be built, and requires some additionnales packages to be installed on linux

```
sudo apt update && apt upgrade
sudo apt install build-essential libssl-dev
sudo apt install python3-dev
sudo apt install default-libmysqlclient-dev

# if last package is not found, try this one : 
sudo apt install libmysqlclient-dev

# Package to be interact direclty with MySQL
sudo apt install mysql-client-core-8.0

# at last, some remaining packages
sudo apt install pip docker-compose
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

login: airflow
password: airflow

#### Airflow variables load

Variables are required to run DAGs, thy are store in the file dags_variables.json
In Airflow web-interface, go in menu Admin > Variables
Import the file dag_variables.json




