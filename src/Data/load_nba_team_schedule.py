from dotenv import load_dotenv
import os
load_dotenv()
import mysql.connector as sql
import pandas as pd
from sqlalchemy import create_engine
import requests 
import datetime

url = 'https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json'
response = requests.get(url)
data = response.json()['leagueSchedule']['gameDates']

host=os.getenv("DB_HOST")
database=os.getenv("DB_NAME")
user=os.getenv("DB_USERNAME")
password=os.getenv("DB_PASSWORD")

engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
utc_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

conn = sql.connect(host=host,
                   database=database,
                   user=user,
                   password=password,
                    )

mycursor = conn.cursor()

def df_to_db(df,increment):
    tot_rows = len(df)
    for i in range(0,tot_rows,increment):
        chunk = df.iloc[i:i + increment]
        chunk.to_sql('stg_nba_team_schedule', engine, if_exists='append', index=False, method='multi')

try:
    mycursor.execute('drop table stg_nba_team_schedule;commit;')
except:
    pass 

# get team schedule columns and create empty df
col_headers = data[0]['games'][0]['homeTeam'].keys()
team_schedule_df = pd.DataFrame(columns=col_headers)


for game_date_num in range(0,len(data)):
    games_on_day = data[game_date_num]
    num_games = len(games_on_day['games'])
    for game in range(0,num_games):
        
        data_values = pd.Series(games_on_day['games'][game])
        game_id = games_on_day['games'][game]['gameId']
        game_date = games_on_day['games'][game]['gameDateEst']
        away_team_dict = data_values.loc['awayTeam'] 
        away_team_dict['homeFlag'] = False
        home_team_dict = data_values.loc['homeTeam']
        home_team_dict['homeFlag'] = True
        
        df = pd.DataFrame([away_team_dict,home_team_dict],columns=home_team_dict.keys())
        df['gameId'] = game_id
        df['gameDateEST'] = game_date
        team_schedule_df = pd.concat([team_schedule_df,df],axis=0)


team_schedule_df['run_date'] = utc_time
df_to_db(team_schedule_df,5)