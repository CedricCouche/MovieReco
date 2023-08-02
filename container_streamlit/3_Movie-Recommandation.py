import streamlit as st
import requests

st.markdown('# Movie Recommandation')


st.markdown('Select a movie')

target_tconst = st.text_input('Movie tconst :', 'tt0001614')

st.markdown('## Cosine Similarity TOP 10 recommandation')


def get_reco_cs(tconst):

    endpoint = 'http://container_api:8000/get_reco_cs/{}'.format(str(tconst))
    response = requests.get(endpoint).json()

    return response

def get_reco_cs2(tconst):

    endpoint = 'http://container_api:8000/get_reco_cs/{}'.format(tconst)
    response = requests.get(endpoint).json()

    return response

def get_reco_cs3(tconst):

    endpoint = 'http://container_api:8000/get_reco_cs/{}'.format(str('%22'+tconst+'%22'))
    response = requests.get(endpoint).json()

    return response

# http://localhost:8000/get_reco_cs/%22tt0001614%22

if st.button('Get Reco'):
    result = get_reco_cs(target_tconst)
    st.write('result: %s' % result)

if st.button('Get Reco2'):
    result = get_reco_cs2(target_tconst)
    st.write('result: %s' % result)

if st.button('Get Reco3'):
    result = get_reco_cs3(target_tconst)
    st.write('result: %s' % result)

#st.write("Movies list : ", get_reco_cs(target_tconst)[0])
#st.write("Movies score : ", get_reco_cs(target_tconst)[1])


st.markdown('## Other Model (to come)')