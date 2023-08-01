FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip

RUN pip install \
    streamlit \
    requests \
    numpy \
    pandas \
    SQLAlchemy

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
