FROM debian:11

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    default-libmysqlclient-dev \
    libssl-dev

RUN pip3 install mysqlclient \
    mysql-connector-python \
    sqlalchemy \
    SQLAlchemy-Utils

RUN pip install \
    streamlit \
    requests \
    numpy \
    pandas

RUN mkdir app
RUN cd app
WORKDIR /app

RUN mkdir assets pages utils

EXPOSE 8501

ADD 1_Main.py ./
ADD 2_Structure.py ./pages
ADD 3_Movie-Recommandation.py ./pages
ADD 4_MySQL.py ./pages

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

CMD streamlit run 1_Main.py
