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
        chunk.to_sql('stg_nba_game_schedule', engine, if_exists='append', index=False, method='multi')

try:
    mycursor.execute('drop table stg_nba_game_schedule;commit;')
except:
    pass # no table to drop
# primitive cols relate to game specific data
prim_types = (bool,int,str,float)

# get a single game to find only primitive cols in data
data_col = data[0]['games'][0].keys()
data_values = pd.Series(data[0]['games'][0])
data_types = [type((data_values)[key]) for key in data_col]
col_headers = pd.Series(data_values.index,index=data_types)

prim_col_headers = col_headers[col_headers.index.isin(prim_types)].values
game_schedule_df = pd.DataFrame(columns=prim_col_headers)




for game_date_num in range(0,len(data)):
    games_on_day = data[game_date_num]
    num_games = len(games_on_day['games'])
    for game in range(0,num_games):
        game_schedule_df.loc[len(game_schedule_df)] = pd.Series(games_on_day['games'][game]).loc[prim_col_headers]


game_schedule_df['run_date'] = utc_time
df_to_db(game_schedule_df,5)