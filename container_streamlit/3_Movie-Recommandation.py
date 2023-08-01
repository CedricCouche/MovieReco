import streamlit as st
import requests

st.markdown('# Movie Recommandation')


st.markdown('Select a movie')

target_tconst = st.text_input('Movie tconst :', 'tt0001614')

st.markdown('## Cosine Similarity TOP 10 recommandation')


def get_reco_cs(tconst):

    endpoint = 'http://localhost:8000/get_reco_cs/{}'.format(str(tconst))
    response = requests.get(endpoint).json()
    movies_reco = response[1:12]
    movies_score = response[12:]

    return movies_reco, movies_score

def get_reco_cs2(tconst):

    endpoint = 'http://127.0.0.1:8000/get_reco_cs/{}'.format(str(tconst))
    response = requests.get(endpoint).json()
    movies_reco = response[1:12]
    movies_score = response[12:]

    return movies_reco, movies_score

def get_reco_cs3(tconst):

    endpoint = 'http://15.188.85.145:8000/get_reco_cs/{}'.format(str(tconst))
    response = requests.get(endpoint).json()
    movies_reco = response[1:12]
    movies_score = response[12:]

    return movies_reco, movies_score

def get_reco_cs4(tconst):

    endpoint = 'http://container_stream:8000/get_reco_cs/{}'.format(str(tconst))
    response = requests.get(endpoint).json()
    movies_reco = response[1:12]
    movies_score = response[12:]

    return movies_reco, movies_score


if st.button('Get Reco'):
    result = get_reco_cs(target_tconst)
    st.write('result: %s' % result[0])

if st.button('Get Reco2'):
    result = get_reco_cs2(target_tconst)
    st.write('result: %s' % result[0])

if st.button('Get Reco3'):
    result = get_reco_cs3(target_tconst)
    st.write('result: %s' % result[0])

if st.button('Get Reco4'):
    result = get_reco_cs4(target_tconst)
    st.write('result: %s' % result[0])


#st.write("Movies list : ", get_reco_cs(target_tconst)[0])
#st.write("Movies score : ", get_reco_cs(target_tconst)[1])


st.markdown('## Other Model (to come)')