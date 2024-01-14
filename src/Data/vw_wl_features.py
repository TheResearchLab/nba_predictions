from util_funcs import mycursor


query = """ select wf.away_team_id as team_id
                  ,wf.game_id
                  ,wf.game_date
                  ,wf.season
                  ,wf.away_last_game_oe as last_game_oe
                  ,wf.away_last_game_home_win_pctg as last_game_home_win_pctg
                  ,wf.away_rest_day_cnt as num_rest_days
                  ,wf.away_last_game_away_win_pctg as last_game_away_win_pctg
                  ,wf.away_last_game_season_win_pctg as last_game_total_win_pctg
                  ,wf.away_last_game_rolling_scoring_margin as last_game_rolling_scoring_margin
                  ,wf.away_last_game_rolling_oe as last_game_rolling_oe
            from wrk_wl_features wf
            join vw_last_game lg on wf.away_team_id = lg.team_id and wf.game_id = lg.game_id

            union

            select wf.home_team_id as team_id
                  ,wf.game_id
                  ,wf.game_date
                  ,wf.season
                  ,wf.home_last_game_oe as last_game_oe
                  ,wf.home_last_game_home_win_pctg as last_game_home_win_pctg
                  ,wf.home_rest_day_cnt as num_rest_days
                  ,wf.home_last_game_away_win_pctg as last_game_away_win_pctg
                  ,wf.home_last_game_season_win_pctg as last_game_total_win_pctg
                  ,wf.home_last_game_rolling_scoring_margin as last_game_rolling_scoring_margin
                  ,wf.home_last_game_rolling_oe as last_game_rolling_oe
            from wrk_wl_features wf
            join vw_last_game lg on wf.home_team_id = lg.team_id and wf.game_id = lg.game_id

"""

mycursor.execute(f'CREATE VIEW vw_wl_features as {query}')