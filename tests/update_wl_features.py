
from util_funcs import engine,text,df_to_db
import pandas as pd

class NBAWinLossModelDataset():
    def __init__(self,query,engine):
        self.engine = engine
        self.query = query 
        self.skip_columns = ['GAME_ID','SEASON','GAME_DATE']
        self._dataset = pd.read_sql_query(self.query,self.engine)


        with engine.connect() as connection:
            trunc_sql = text("truncate table wrk_wl_features")
            connection.execute(trunc_sql)
            connection.close()

    @property
    def dataset(self):
        return self._dataset
    
    def calc_model_features(self):

        def get_home_team_df():
            home_team_df = self._dataset[self._dataset['CITY'] != 'OPPONENTS']
            home_team_df = home_team_df[['LAST_GAME_OE','LAST_GAME_HOME_WIN_PCTG','REST_DAY_CNT','LAST_GAME_AWAY_WIN_PCTG','LAST_GAME_SEASON_WIN_PCTG','LAST_GAME_ROLLING_SCORING_MARGIN','LAST_GAME_ROLLING_OE','W','TEAM_ID','GAME_ID','SEASON','GAME_DATE']]
            col_rename_dict = {col:'HOME_' + col if col not in self.skip_columns else col for col in home_team_df.columns}
            home_team_df.rename(columns=col_rename_dict,inplace=True)
            return home_team_df

        def get_away_team_df():
            away_team_df = self._dataset[self._dataset['CITY'] == 'OPPONENTS']
            away_team_df = away_team_df[['LAST_GAME_OE','LAST_GAME_HOME_WIN_PCTG','REST_DAY_CNT','LAST_GAME_AWAY_WIN_PCTG','LAST_GAME_SEASON_WIN_PCTG','LAST_GAME_ROLLING_SCORING_MARGIN','LAST_GAME_ROLLING_OE','TEAM_ID','GAME_ID','SEASON']]
            col_rename_dict = {col:'AWAY_' + col if col not in self.skip_columns else col for col in away_team_df.columns}
            away_team_df.rename(columns=col_rename_dict,inplace=True)
            return away_team_df    
        
        
        self.calc_season_win_pctg()
        self.calc_season_home_win_pctg()
        self.calc_season_away_win_pctg()
        self.calc_rolling_oe()
        self.calc_rolling_scoring_margin()
        self.calc_rest_days()
        self.shift_model_data()
        self._dataset = pd.merge(get_home_team_df(),get_away_team_df(),how='inner',on=["GAME_ID","SEASON"])
        return self._dataset
    
    def calc_season_win_pctg(self):
        self._dataset['TOT_GAMES_PLAYED'] = self._dataset.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['GAME_DATE'].rank(ascending=True)
        self._dataset['TOT_WINS'] = (self._dataset
                                        .sort_values(by='GAME_DATE')
                                        .groupby(['TEAM_ID','SEASON'])['W']
                                        .cumsum())
        self._dataset['SEASON_WIN_PCTG'] = self._dataset['TOT_WINS']/self._dataset['TOT_GAMES_PLAYED']
        return self._dataset
    
    def calc_season_home_win_pctg(self):
        self._dataset['HOME_GAMES_PLAYED'] = self._dataset.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['HOME_FLAG'].cumsum() + 1 # +1 to start at 1 not 0
        self._dataset['HOME_WINS'] = self._dataset.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['W_HOME'].cumsum()
        self._dataset['HOME_WIN_PCTG'] = self._dataset['HOME_WINS'] / self._dataset['HOME_GAMES_PLAYED']
        return self._dataset
    
    def calc_season_away_win_pctg(self):
        self._dataset['AWAY_GAMES_PLAYED'] = self._dataset.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['AWAY_FLAG'].cumsum() + 1 # +1 to start at 1 not 0
        self._dataset['AWAY_WINS'] = self._dataset.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['W_ROAD'].cumsum()
        self._dataset['AWAY_WIN_PCTG'] = self._dataset['AWAY_WINS'] / self._dataset['AWAY_GAMES_PLAYED']
        return self._dataset
    
    def calc_rolling_oe(self):
        self._dataset['ROLLING_OE'] = self._dataset.sort_values('GAME_DATE').groupby('TEAM_ID')['OFFENSIVE_EFFICIENCY'].transform(lambda x: x.rolling(3,1).mean())
        return self._dataset
    
    def calc_rolling_scoring_margin(self):
        self._dataset['ROLLING_SCORING_MARGIN'] = self._dataset.sort_values('GAME_DATE').groupby('TEAM_ID')['SCORING_MARGIN'].transform(lambda x: x.rolling(3,1).mean())
        return self._dataset 
    
    def calc_rest_days(self):
        self._dataset['LAST_GAME_DATE'] = self._dataset.sort_values('GAME_DATE').groupby('TEAM_ID')['GAME_DATE'].shift(1)
        self._dataset['REST_DAY_CNT'] = (pd.to_datetime(self._dataset['GAME_DATE']) - pd.to_datetime(self._dataset['LAST_GAME_DATE']))
        self._dataset = self._dataset.dropna()
        self._dataset['REST_DAY_CNT'] = self._dataset['REST_DAY_CNT'].astype('str').str.replace(r'\s.*', '', regex=True)
        return self._dataset
    
    def shift_model_data(self):
        self._dataset['LAST_GAME_OE'] = self._dataset.sort_values('GAME_DATE').groupby(['TEAM_ID'])['OFFENSIVE_EFFICIENCY'].shift(1)
        self._dataset['LAST_GAME_HOME_WIN_PCTG'] = self._dataset.sort_values('GAME_DATE').groupby(['TEAM_ID'])['HOME_WIN_PCTG'].shift(1)
        self._dataset['LAST_GAME_AWAY_WIN_PCTG'] = self._dataset.sort_values('GAME_DATE').groupby(['TEAM_ID'])['AWAY_WIN_PCTG'].shift(1)
        self._dataset['LAST_GAME_SEASON_WIN_PCTG'] = self._dataset.sort_values('GAME_DATE').groupby(['TEAM_ID'])['SEASON_WIN_PCTG'].shift(1)
        self._dataset['LAST_GAME_ROLLING_SCORING_MARGIN'] = self._dataset.sort_values('GAME_DATE').groupby(['TEAM_ID'])['ROLLING_SCORING_MARGIN'].shift(1)
        self._dataset['LAST_GAME_ROLLING_OE'] = self._dataset.sort_values('GAME_DATE').groupby(['TEAM_ID'])['ROLLING_OE'].shift(1)
    
    






    

query = """select
            distinct *
            ,case when city='OPPONENTS' then 0 else 1 end as HOME_FLAG
            ,case when city='OPPONENTS' then 1 else 0 end as AWAY_FLAG
            from stg_game_stats"""
    

data = NBAWinLossModelDataset(query,engine).calc_model_features()
df_to_db(data,len(data),5)



