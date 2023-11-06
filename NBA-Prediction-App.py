import streamlit as st 
import joblib 
import requests
import pandas as pd 
import numpy as np
from sklearn import preprocessing
from PIL import Image
import pickle

st.sidebar.success('The Research Lab™')


 
def main():

    model_version = st.selectbox('Choose Model Version',
                            ('2021-2022 Model','2022-2023 Model'))

    url = "https://nba-prediction-api.onrender.com/wl_model?version=%27v1%27" if model_version == '2021-2022 Model' else "https://nba-prediction-api.onrender.com/wl_model?version=%27v2%27"
    response = requests.get(url)
    data = pd.read_json(response.json())
    st.session_state.model = pickle.loads(bytes.fromhex(data['model_object_hex'][0]))


    url = "https://nba-prediction-api.onrender.com/upcoming_games"
    response = requests.get(url)
    upcoming_games = pd.read_json(response.json(),orient='records')
    upcoming_games['GAME_DATE'] = upcoming_games['GAME_DATE_EST']


    url = "https://nba-prediction-api.onrender.com/model_features"
    response = requests.get(url)
    model_features = pd.read_json(response.json(),orient='records').drop('season',axis=1).dropna()


    url = "https://nba-prediction-api.onrender.com/test_data"
    response = requests.get(url)
    test_data = pd.read_json(response.json(),orient='records').dropna().sort_values('GAME_DATE',ascending=True)
    test_data = test_data[test_data['SEASON'] == '2022-23'] if model_version == '2021-2022 Model' else test_data[test_data['SEASON'] == '2023-24']


    def game_prediction(home_id,away_id):
        outcomes = ['AWAY TEAM','HOME TEAM']
        home_features = model_features[model_features['team_id'] == home_id].values[0][3:]
        away_features = model_features[model_features['team_id'] == away_id].values[0][3:]
        return outcomes[st.session_state.model.predict(np.array([*home_features,*away_features]).reshape(1,14))[0]]
        

    upcoming_games['MODEL_PREDICTION'] = upcoming_games.apply( lambda row: game_prediction(row['HOME_TEAM_ID'],row['AWAY_TEAM_ID']),axis=1)


    X_test = test_data[['HOME_LAST_GAME_OE', 'HOME_LAST_GAME_HOME_WIN_PCTG',
        'HOME_REST_DAY_CNT', 'HOME_LAST_GAME_AWAY_WIN_PCTG',
        'HOME_LAST_GAME_SEASON_WIN_PCTG',
        'HOME_LAST_GAME_ROLLING_SCORING_MARGIN', 'HOME_LAST_GAME_ROLLING_OE',
        'AWAY_LAST_GAME_OE', 'AWAY_LAST_GAME_HOME_WIN_PCTG',
        'AWAY_REST_DAY_CNT', 'AWAY_LAST_GAME_AWAY_WIN_PCTG',
        'AWAY_LAST_GAME_SEASON_WIN_PCTG',
        'AWAY_LAST_GAME_ROLLING_SCORING_MARGIN', 'AWAY_LAST_GAME_ROLLING_OE']]
    scaler = preprocessing.StandardScaler()
    scaler.fit(X_test)
    scaled_data = scaler.transform(X_test) 

    test_data['PREDICTION'] = st.session_state.model.predict(scaled_data)

    test_data['TRUE_POSITIVE_BOOL'] = ((test_data['PREDICTION'] == 1) & (test_data['HOME_W'] == 1))
    test_data['TRUE_NEGATIVE_BOOL'] = ((test_data['PREDICTION'] == 0) & (test_data['HOME_W'] == 0))
    test_data['FALSE_POSITIVE_BOOL'] = ((test_data['PREDICTION'] == 1) & (test_data['HOME_W'] == 0))
    test_data['FALSE_NEGATIVE_BOOL'] = ((test_data['PREDICTION'] == 0) & (test_data['HOME_W'] == 1))
    test_data['TRUE_POSITIVE_TOTAL_CNT'] = test_data['TRUE_POSITIVE_BOOL'].cumsum()
    test_data['TRUE_NEGATIVE_TOTAL_CNT'] = test_data['TRUE_NEGATIVE_BOOL'].cumsum()
    test_data['FALSE_POSITIVE_TOTAL_CNT'] = test_data['FALSE_POSITIVE_BOOL'].cumsum()
    test_data['FALSE_NEGATIVE_TOTAL_CNT'] = test_data['FALSE_NEGATIVE_BOOL'].cumsum()

    #PRECISION/ACCURACY
    test_data['ACCURACY'] = (test_data['TRUE_NEGATIVE_TOTAL_CNT'] + test_data['TRUE_POSITIVE_TOTAL_CNT'])/(test_data['TRUE_NEGATIVE_TOTAL_CNT'] + test_data['TRUE_POSITIVE_TOTAL_CNT'] + test_data['FALSE_NEGATIVE_TOTAL_CNT'] + test_data['FALSE_POSITIVE_TOTAL_CNT']) 
    test_data['PRECISION'] = test_data['TRUE_POSITIVE_TOTAL_CNT'] / (test_data['TRUE_POSITIVE_TOTAL_CNT'] + test_data['FALSE_POSITIVE_TOTAL_CNT']) 




    st.title('Live Performance')
    st.line_chart(test_data,x='GAME_DATE', y=['ACCURACY','PRECISION'])


    st.title('Upcoming Games')
    st.table(upcoming_games[['GAME_DATE','HOME_TEAM_NAME','AWAY_TEAM_NAME','MODEL_PREDICTION']])


try:
    main()
except:
    st.text("404 -  Try Again Later")
    image = Image.open('404_img.jpg')
    st.image(image)

