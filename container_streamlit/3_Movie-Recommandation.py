import streamlit as st
import numpy as np
import requests

#### Variables ####

BaseURL = 'https://www.imdb.com/title/'



#### Functions ####

# Note : http://localhost:8000/get_reco_cs/%22tt0001614%22
# Note : http://localhost:8000/get_film-info/%22tt0001614%22

def get_film_info(tconst):

    endpoint = 'http://container_api:8000/get-film-info/{}'.format(str('%22'+tconst+'%22'))
    response = requests.get(endpoint).json()

    return response

def get_reco_cs01(tconst):

    endpoint = 'http://container_api:8000/get_reco_cs01/{}'.format(str('%22'+tconst+'%22'))
    response = requests.get(endpoint).json()

    return response


def get_reco_cs02(tconst):

    endpoint = 'http://container_api:8000/get_reco_cs02/{}'.format(str('%22'+tconst+'%22'))
    response = requests.get(endpoint).json()

    return response



#### Body ####

st.markdown('# Movie Recommandation')

st.markdown('Select a movie')

target_tconst = st.text_input('Movie tconst :', 'tt0000574')

if st.button('Get film info 01'):
    result = get_film_info(target_tconst)
    st.write('result: %s' % result)

if st.button('Get film info 02'):
    result = get_film_info(target_tconst)
    st.write("tconst : ", result["tconst"])
    st.write("titleType: ", result["titleType"])
    st.write("primaryTitle: ", result["primaryTitle"])
    st.write("startYear: ", result["startYear"])
    st.write("runtimeMinutes : ", result["runtimeMinutes"])
    st.write("genres : ", result["genres"])
    st.write("runtimeCategory : ", result["runtimeCategory"])
    st.write("yearCategory : ", result["yearCategory"])
    st.write("directors_id : ", result["directors_id"])
    st.write("writers_id : ", result["writers_id"])
    st.write("averageRating : ", result["averageRating"])
    st.write("numVotes : ", result["numVotes"])


st.markdown('## Cosine Similarity TOP 10 recommandation')


if st.button('Get Recommandation using Cosine Similarity (01)'):
    result = get_reco_cs01(target_tconst)
    st.write('result: %s' % result)


if st.button('Get Recommandation using Cosine Similarity (02)'):
    
    result = get_reco_cs02(target_tconst)
    
    filmInfo = get_film_info(target_tconst)
    title = filmInfo["primaryTitle"]
    year = str(filmInfo["startYear"])
    genres = filmInfo["genres"]
    st.write(" Recommandations for title : ", '[' +title+ ', ' +year+ ' (' +genres+ ')]('+BaseURL + target_tconst+ ')')
    
    for tconst, score in zip(result['titles'], result['scores']):
        filmInfo = get_film_info(tconst)
        title = filmInfo["primaryTitle"]
        year = str(filmInfo["startYear"])
        genres = filmInfo["genres"]
        st.write('Matching score : ' + str(np.round(score,2)) + " Movie : ", '[' +title+ ', ' +year+  ' (' +genres+ ')]('+BaseURL + tconst+ ')')


st.markdown('## Other Model (to come)')