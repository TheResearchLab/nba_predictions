import pandas as pd
import os
import datetime
import util_funcs

engine = util_funcs.engine

game_stats_query = "SELECT * FROM stg_game_stats"
game_stats = pd.read_sql_query(game_stats_query,engine)

def get_current_season(today):
    if today.month >= 10:
        start_year = today.year
        end_year = today.year + 1
    else:
        start_year = today.year - 1
        end_year = today.year
    
    season = f"{start_year}-{end_year % 100:02d}"
    return season


nba_schedule_query = f"""SELECT  cast(sngs.gameId as signed) GAME_ID
                                ,sngs.gameDateTimeUTC as GAME_DATE_UTC
                                ,DATE_FORMAT(STR_TO_DATE(sngs.gameDateTimeEST, '%Y-%m-%dT%H:%i:%sZ'), '%Y-%m-%d %H:%i:%s') AS GAME_DATE_EST
                                ,sngs.seriesText as SERIES_TEXT
                                ,sngs.postponedStatus as POSTPONED_STATUS
                                ,sngs.gameDateTimeUTC
                                ,home.teamId as homeTeamID
                                ,home.teamName as homeTeamName
                                ,away.teamId as awayTeamID
                                ,away.teamName as awayTeamName
                                ,'{get_current_season(datetime.datetime.now())}' as SEASON
                         FROM 
                            stg_nba_game_schedule sngs
                        LEFT OUTER JOIN (select * from stg_nba_team_schedule where homeFlag = 1 ) home
                        ON home.gameId = sngs.gameId
                        LEFT OUTER JOIN (select * from stg_nba_team_schedule where homeFlag = 0 ) away
                        ON away.gameId = sngs.gameId

                         WHERE 
                            sngs.seriesText = ''
                        and sngs.gameDateTimeUTC < '{datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")}'
                        and sngs.postponedStatus = 'A'  """

nba_schedule = pd.read_sql_query(nba_schedule_query,engine)

for _,game in nba_schedule.iterrows():
    if game['GAME_ID'] not in game_stats['GAME_ID']:
        util_funcs.getSingleGameMetrics(game['GAME_ID'],game['homeTeamID'],game['awayTeamID'],game['awayTeamName'],game['SEASON'],game['GAME_DATE_EST']).to_sql('stg_game_stats', engine, if_exists='append', index=False, method='multi')
    else:
        continue
