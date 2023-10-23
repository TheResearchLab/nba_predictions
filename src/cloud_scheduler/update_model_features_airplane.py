import airplane


@airplane.task(
    slug="update_model_features",
    name="update_model_features",
)
def update_model_features():
    import pandas as pd
    import numpy as np
    import util_funcs

    #Create Engine
    engine = util_funcs.engine

    #Drop table before rebuilding
    query = "truncate table wrk_wl_features"
    util_funcs.mycursor.execute(query)
    
    #Transformation Functions
    def get_total_win_pctg(game_df):
        game_df['TOTAL_GAMES_PLAYED'] = game_df.groupby(['TEAM_ID','SEASON'])['GAME_DATE'].rank(ascending=True) # check
        game_df['TOTAL_WINS'] = game_df.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['W'].cumsum()
        game_df['TOTAL_WIN_PCTG'] = game_df['TOTAL_WINS']/game_df['TOTAL_GAMES_PLAYED']
        return game_df.drop(['TOTAL_GAMES_PLAYED','TOTAL_WINS'],axis=1)

    def get_home_win_pctg(game_df):
        game_df['HOME_GAMES_PLAYED'] = game_df.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['HOME_FLAG'].cumsum()
        game_df['HOME_WINS'] = game_df.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['W_HOME'].cumsum()
        game_df['HOME_WIN_PCTG'] = game_df['HOME_WINS']/game_df['HOME_GAMES_PLAYED']
        return game_df.drop(['HOME_GAMES_PLAYED','HOME_WINS'],axis=1)

    def get_away_win_pctg(game_df):
        game_df['AWAY_GAMES_PLAYED'] = game_df.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['AWAY_FLAG'].cumsum()
        game_df['AWAY_WINS'] = game_df.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['W_ROAD'].cumsum()
        game_df['AWAY_WIN_PCTG'] = game_df['AWAY_WINS']/game_df['AWAY_GAMES_PLAYED']
        return game_df.drop(['AWAY_GAMES_PLAYED','AWAY_WINS'],axis=1)

    def get_rolling_oe(game_df):
        game_df['ROLLING_OE'] = game_df.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['OFFENSIVE_EFFICIENCY'].transform(lambda x: x.rolling(3, 1).mean())
        return game_df

    def get_rolling_scoring_margin(game_df):
        game_df['ROLLING_SCORING_MARGIN'] = game_df.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['SCORING_MARGIN'].transform(lambda x: x.rolling(3, 1).mean())
        return game_df

    def get_rest_days(game_df):
        game_df['LAST_GAME_DATE'] = game_df.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['GAME_DATE'].shift(1)
        game_df['NUM_REST_DAYS'] = (pd.to_datetime(game_df['GAME_DATE']) - pd.to_datetime(game_df['LAST_GAME_DATE']))/np.timedelta64(1,'D') 
        return game_df.drop('LAST_GAME_DATE',axis=1)

    def get_feature_df(game_df):
        
        skip_columns = ['GAME_ID','SEASON','GAME_DATE']

        def get_shifted_df(game_df):
            game_df['LAST_GAME_OE'] = game_df.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['OFFENSIVE_EFFICIENCY'].shift(1)
            game_df['LAST_GAME_HOME_WIN_PCTG'] = game_df.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['HOME_WIN_PCTG'].shift(1)
            game_df['LAST_GAME_AWAY_WIN_PCTG'] = game_df.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['AWAY_WIN_PCTG'].shift(1)
            game_df['LAST_GAME_TOTAL_WIN_PCTG'] = game_df.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['TOTAL_WIN_PCTG'].shift(1)
            game_df['LAST_GAME_ROLLING_SCORING_MARGIN'] = game_df.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['ROLLING_SCORING_MARGIN'].shift(1)
            game_df['LAST_GAME_ROLLING_OE'] = game_df.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['ROLLING_OE'].shift(1)
            return game_df
        
        
        def get_home_team_df(game_df):
            home_team_df = game_df[game_df['CITY'] != 'OPPONENTS']
            home_team_df = home_team_df[['LAST_GAME_OE','LAST_GAME_HOME_WIN_PCTG','NUM_REST_DAYS','LAST_GAME_AWAY_WIN_PCTG','LAST_GAME_TOTAL_WIN_PCTG','LAST_GAME_ROLLING_SCORING_MARGIN','LAST_GAME_ROLLING_OE','W','TEAM_ID','GAME_ID','SEASON','GAME_DATE']]
            col_rename_dict = {col:'HOME_' + col if col not in skip_columns else col for col in home_team_df.columns}
            home_team_df.rename(columns=col_rename_dict,inplace=True)
            return home_team_df

        def get_away_team_df(game_df):
            away_team_df = game_df[game_df['CITY'] == 'OPPONENTS']
            away_team_df = away_team_df[['LAST_GAME_OE','LAST_GAME_HOME_WIN_PCTG','NUM_REST_DAYS','LAST_GAME_AWAY_WIN_PCTG','LAST_GAME_TOTAL_WIN_PCTG','LAST_GAME_ROLLING_SCORING_MARGIN','LAST_GAME_ROLLING_OE','TEAM_ID','GAME_ID','SEASON']]
            col_rename_dict = {col:'AWAY_' + col if col not in skip_columns else col for col in away_team_df.columns}
            away_team_df.rename(columns=col_rename_dict,inplace=True)
            return away_team_df
        
        game_df = get_shifted_df(game_df)
        away_team_df = get_away_team_df(game_df)
        home_team_df = get_home_team_df(game_df)
        
        return pd.merge(home_team_df, away_team_df, how="inner", on=[ "GAME_ID","SEASON"])

    def df_to_db(df,tot_rows,increment):
        for i in range(0,tot_rows,increment):
            chunk = df.iloc[i:i + increment]
            chunk.to_sql('wrk_wl_features', engine, if_exists='append', index=False, method='multi')

    # Get data
    query = """select
            *
            ,case when city='OPPONENTS' then 0 else 1 end as HOME_FLAG
            ,case when city='OPPONENTS' then 1 else 0 end as AWAY_FLAG
            ,row_number() over (partition by team_id,season order by game_date asc) as TOT_GAMES_PLAYED
            from stg_game_stats"""

    game_stats = pd.read_sql_query(query,engine)
    game_stats = get_home_win_pctg(game_stats)
    game_stats = get_away_win_pctg(game_stats)
    game_stats = get_total_win_pctg(game_stats)
    game_stats = get_rolling_scoring_margin(game_stats)
    game_stats = get_rolling_oe(game_stats)
    game_stats = get_rest_days(game_stats)

    feature_df = get_feature_df(game_stats)

    tot_rows = len(feature_df)

    df_to_db(feature_df,tot_rows,5)
    return None 
