import airplane


@airplane.task(
    slug="update_stg_game_stats",
    name="update_stg_game_stats",
)
def update_stg_game_stats():
    import pandas as pd
    import os
    import datetime
    import util_funcs

    engine = util_funcs.engine

    game_stats_query = "SELECT * FROM stg_game_stats"
    game_stats = pd.read_sql_query(game_stats_query,engine)

    nba_schedule_query = f"""SELECT  cast(gameId as signed) GAME_ID
                                    ,gameDateTimeUTC as GAME_DATE_UTC
                                    ,seriesText as SERIES_TEXT
                                    ,homeTeamID as HOME_TEAM_ID
                                    ,awayTeamID as AWAY_TEAM_ID
                                    ,homeTeamName as AWAY_TEAM_NICKNAME
                                    ,awayTeamName as HOME_TEAM_NICKNAME
                                    ,postponedStatus as POSTPONED_STATUS
                                    ,'2023-24' as SEASON
                    FROM 
                        stg_nba_schedule 
                    WHERE 
                        seriesText = ''
                    and gameDateTimeUTC < '{datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")}'
                    and postponedStatus = 'A'
                    and (homeTeamId <> 0 and awayTeamID <> 0)  """

    nba_schedule = pd.read_sql_query(nba_schedule_query, engine)


    for _,game in nba_schedule.iterrows():
        if game['GAME_ID'] not in game_stats['GAME_ID']:
            util_funcs.getSingleGameMetrics(game['GAME_ID'],game['HOME_TEAM_ID'],game['AWAY_TEAM_ID'],game['AWAY_TEAM_NICKNAME'],game['SEASON'],game['GAME_DATE_UTC']).to_sql('stg_game_stats', engine, if_exists='append', index=False, method='multi')
        else:
            continue

    return None 