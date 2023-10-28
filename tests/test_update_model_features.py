import unittest 
from util_funcs import engine
from update_wl_features import NBAWinLossModelDataset
import pandas as pd

class TestNBAWinLossModelDataset(unittest.TestCase):
    def setUp(self):
        self.engine = engine
        self.query = """select *
            ,case when city='OPPONENTS' then 0 else 1 end as HOME_FLAG
            ,case when city='OPPONENTS' then 1 else 0 end as AWAY_FLAG
            ,row_number() over (partition by team_id,season order by game_date asc) as TOT_GAMES_PLAYED
            from stg_game_stats"""
    
    def test_connection(self):
        query = "select 3 as result"
        data_1 = NBAWinLossModelDataset(query,self.engine).dataset
        data_2 = pd.DataFrame({'result':[3]})
        self.assertTrue(data_1.equals(data_2))

    # CHECK FOR DUPLICATE VALUES IN INITIAL PULL

    def test_calc_season_win_pctg(self):
        data = NBAWinLossModelDataset(self.query,self.engine)
        data = data.calc_season_win_pctg()
        column_present = 'SEASON_WIN_PCTG' in data.columns
        has_na = data['SEASON_WIN_PCTG'].isna().any()  
        self.assertTrue(column_present,'Column is not in table')
        self.assertFalse(has_na,'table has NA values')

    def test_calc_season_home_win_pctg(self):
        data = NBAWinLossModelDataset(self.query,self.engine)
        data = data.calc_season_home_win_pctg()
        column_present = 'HOME_WIN_PCTG' in data.columns
        has_na = data['HOME_WIN_PCTG'].isna().any()  
        self.assertTrue(column_present,'Column is not in table')
        self.assertFalse(has_na,'table has NA values')

    def test_calc_season_away_win_pctg(self):
        data = NBAWinLossModelDataset(self.query,self.engine)
        data = data.calc_season_away_win_pctg()
        column_present = 'AWAY_WIN_PCTG' in data.columns
        has_na = data['AWAY_WIN_PCTG'].isna().any().any()  
        self.assertTrue(column_present,'Column is not in table')
        self.assertFalse(has_na,'table has NA values')

    def test_calc_model_features(self):
        data = NBAWinLossModelDataset(self.query,self.engine)
        data = data.calc_model_features()
        has_na = data.isna().any().any()  
        self.assertFalse(has_na,'table has NA values')

    




if __name__ == '__main__':
    unittest.main()