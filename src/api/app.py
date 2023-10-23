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
    query = """SELECT gameId as GAME_ID
                     ,homeTeamName as HOME_TEAM_NAME
                     ,awayTeamName as AWAY_TEAM_NAME
                     ,gameDateTimeUTC as GAME_DATETIME
                     ,homeTeamID as HOME_TEAM_ID
                     ,awayTeamID as AWAY_TEAM_ID
            FROM 
                  stg_nba_schedule
            WHERE 
                  postponedStatus = 'A'
              AND (homeTeamId <> 0 and awayTeamID <> 0)
              AND seriesText = '' 
            
            ORDER BY
                  gameDateTimeUTC ASC
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