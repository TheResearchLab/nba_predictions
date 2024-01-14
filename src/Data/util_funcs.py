from dotenv import load_dotenv
from nba_api.stats.endpoints import cumestatsteam
import time
import requests
import pandas as pd
import json
import os 
load_dotenv()
from sqlalchemy import create_engine
import mysql.connector as sql
import datetime


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
    utc_now = datetime.datetime.utcnow()
    est_offset_hours = -5  # EST is 5 hours behind UTC
    est_offset = datetime.timedelta(hours=est_offset_hours)

    # Adjust the current UTC time to EST
    est_now = utc_now + est_offset

    # Format the datetime as required
    load_date = est_now.strftime('%Y-%m-%d %H:%M:%S')
    
    if len(data) != 0:
        data.at[1,'NICKNAME'] = awayTeamNickname
        data.at[1,'TEAM_ID'] = awayTeamID
        data.at[1,'OFFENSIVE_EFFICIENCY'] = (data.at[1,'FG'] + data.at[1,'AST'])/(data.at[1,'FGA'] - data.at[1,'OFF_REB'] + data.at[1,'AST'] + data.at[1,'TOTAL_TURNOVERS'])
        data.at[1,'SCORING_MARGIN'] = data.at[1,'PTS'] - data.at[0,'PTS']

        data.at[0,'OFFENSIVE_EFFICIENCY'] = (data.at[0,'FG'] + data.at[0,'AST'])/(data.at[0,'FGA'] - data.at[0,'OFF_REB'] + data.at[0,'AST'] + data.at[0,'TOTAL_TURNOVERS'])
        data.at[0,'SCORING_MARGIN'] = data.at[0,'PTS'] - data.at[1,'PTS']

        data['SEASON'] = seasonYear
        data['GAME_DATE_EST'] = gameDate
        data['GAME_ID'] = gameID
        data['LOAD_DATE_EST'] = load_date

    return data
