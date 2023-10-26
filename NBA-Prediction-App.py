import streamlit as st 
import joblib 
import requests
import pandas as pd 
import numpy as np
from sklearn import preprocessing

st.sidebar.success('The Research Lab™')

st.session_state.model = joblib.load('src/model/NBAHomeTeamWinLoss.pkl')
 


url = "https://nba-prediction-api.onrender.com/all"
response = requests.get(url)
historic_games = pd.read_json(response.json(),orient='records')

# Get most recent features for away @ team
historic_games['GAME_DATE'] = pd.to_datetime(historic_games['GAME_DATE'],infer_datetime_format=True, errors='coerce')

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


def game_prediction(home_id,away_id):
    outcomes = ['AWAY TEAM','HOME TEAM']
    home_features = model_features[model_features['team_id'] == home_id].values[0][3:]
    away_features = model_features[model_features['team_id'] == away_id].values[0][3:]
    return outcomes[st.session_state.model.predict(np.array([*home_features,*away_features]).reshape(1,14))[0]]
    

upcoming_games['MODEL_PREDICTION'] = upcoming_games.apply( lambda row: game_prediction(row['HOME_TEAM_ID'],row['AWAY_TEAM_ID']),axis=1)


X_test = test_data[['HOME_LAST_GAME_OE', 'HOME_LAST_GAME_HOME_WIN_PCTG',
       'HOME_NUM_REST_DAYS', 'HOME_LAST_GAME_AWAY_WIN_PCTG',
       'HOME_LAST_GAME_TOTAL_WIN_PCTG',
       'HOME_LAST_GAME_ROLLING_SCORING_MARGIN', 'HOME_LAST_GAME_ROLLING_OE',
       'AWAY_LAST_GAME_OE', 'AWAY_LAST_GAME_HOME_WIN_PCTG',
       'AWAY_NUM_REST_DAYS', 'AWAY_LAST_GAME_AWAY_WIN_PCTG',
       'AWAY_LAST_GAME_TOTAL_WIN_PCTG',
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




st.title('Previous Year Performance')
st.line_chart(test_data,x='GAME_DATE', y=['ACCURACY','PRECISION'])


st.title('Upcoming Games')
st.table(upcoming_games[['GAME_DATE','HOME_TEAM_NAME','AWAY_TEAM_NAME','MODEL_PREDICTION']])