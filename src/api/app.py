from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
import pandas as pd 
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins = ["https://nba-predictions.streamlit.app"],
    allow_credentials = True,
    allow_methods = ["GET"],
    allow_headers = ["*"],

)

host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")
user =  os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")


@app.get("/all")
async def read_all():
    query = """ select * from wrk_wl_features"""
    df = pd.read_sql_query(query,engine)
    return df.to_json(orient='records')


@app.get("/upcoming_games")
async def read_upcoming_games():
    query = """SELECT sngs.gameId as GAME_ID
                     ,home.teamName as HOME_TEAM_NAME
                     ,away.teamName as AWAY_TEAM_NAME
                     ,DATE_FORMAT(STR_TO_DATE(sngs.gameDateTimeEST, '%Y-%m-%dT%H:%i:%sZ'), '%Y-%m-%d %H:%i:%s') AS GAME_DATE_EST
                     ,home.teamId as HOME_TEAM_ID
                     ,away.teamId as AWAY_TEAM_ID
                FROM 
                    stg_nba_game_schedule sngs
                LEFT OUTER JOIN (select * from stg_nba_team_schedule where homeFlag = 1 ) home
                    ON home.gameId = sngs.gameId
                LEFT OUTER JOIN (select * from stg_nba_team_schedule where homeFlag = 0 ) away
                    ON away.gameId = sngs.gameId
                WHERE 
                  sngs.postponedStatus = 'A'
              AND DATE_FORMAT(STR_TO_DATE(sngs.gameDateTimeEST, '%Y-%m-%dT%H:%i:%sZ'), '%Y-%m-%d %H:%i:%s') >= CURDATE()
              AND sngs.seriesText = '' 
            
            ORDER BY
                  DATE_FORMAT(STR_TO_DATE(sngs.gameDateTimeEST, '%Y-%m-%dT%H:%i:%sZ'), '%Y-%m-%d %H:%i:%s') ASC
            LIMIT 15      """
    df = pd.read_sql_query(query,engine)
    return df.to_json(orient='records')

@app.get("/model_features")
async def read_model_features():
    query = "select * from vw_wl_features"
    df = pd.read_sql_query(query,engine)
    return df.to_json(orient='records')


@app.get('/test_data')
async def read_test_data():
    query = "select * from wrk_wl_features where season = '2022-23'"
    df = pd.read_sql_query(query,engine)
    return df.to_json(orient='records')