from nba_api.stats.endpoints import cumestatsteam
import time
import requests
import pandas as pd
import json
import os 
from sqlalchemy import create_engine,text
import mysql.connector as sql


#Retrieve Connection Environment Variables
database = os.getenv('DB_NAME')
user = os.getenv('DB_USERNAME')
host = os.getenv('DB_HOST')
password = os.getenv('DB_PASSWORD')

# Create connection w/ mysql.connector
conn = sql.connect(host=host,
                  database=database,
                  user=user,
                  password=password,
                   )
mycursor = conn.cursor()



# Create SQL Alchemy Connection
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")



# Retry Wrapper 
def retry(func, retries=3):
    def retry_wrapper(*args, **kwargs):
        attempts = 0
        while attempts < retries:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                print(e)
                time.sleep(30)
                attempts += 1

    return retry_wrapper

# Get Single Game aggregation columns
def getSingleGameMetrics(gameID,homeTeamID,awayTeamID,awayTeamNickname,seasonYear,gameDate):

    @retry
    def getGameStats(teamID,gameID,seasonYear):
        gameStats = cumestatsteam.CumeStatsTeam(game_ids='00'+str(gameID),league_id ="00",
                                               season=seasonYear,season_type_all_star="Regular Season",
                                               team_id = teamID).get_normalized_json()

        gameStats = pd.DataFrame(json.loads(gameStats)['TotalTeamStats'])
        print(f'length of records is {len(gameStats)}')
        print(gameStats.columns)

        return gameStats

    data = getGameStats(homeTeamID,gameID,seasonYear)
    if len(data) != 0:
        data.at[1,'NICKNAME'] = awayTeamNickname
        data.at[1,'TEAM_ID'] = awayTeamID
        data.at[1,'OFFENSIVE_EFFICIENCY'] = (data.at[1,'FG'] + data.at[1,'AST'])/(data.at[1,'FGA'] - data.at[1,'OFF_REB'] + data.at[1,'AST'] + data.at[1,'TOTAL_TURNOVERS'])
        data.at[1,'SCORING_MARGIN'] = data.at[1,'PTS'] - data.at[0,'PTS']

        data.at[0,'OFFENSIVE_EFFICIENCY'] = (data.at[0,'FG'] + data.at[0,'AST'])/(data.at[0,'FGA'] - data.at[0,'OFF_REB'] + data.at[0,'AST'] + data.at[0,'TOTAL_TURNOVERS'])
        data.at[0,'SCORING_MARGIN'] = data.at[0,'PTS'] - data.at[1,'PTS']

        data['SEASON'] = seasonYear
        data['GAME_DATE'] = gameDate
        data['GAME_ID'] = gameID

    return data

def df_to_db(df,tot_rows,increment):
    for i in range(0,tot_rows,increment):
        chunk = df.iloc[i:i + increment]
        chunk.to_sql('wrk_wl_features', engine, if_exists='append', index=False, method='multi')