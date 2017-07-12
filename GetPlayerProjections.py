import numpy as np
import pandas as pd
import xgboost as xgb
import gc
import matplotlib.pyplot as plt
import psycopg2
import config

print ('Loading Data')

conn = psycopg2.connect(host=config.endpoint, database=config.database, user=config.user, password=config.password)

# Get training data
training = pd.read_sql_query("""SELECT  ops.*, oav.*, dts.*, ttd.home_or_away
                                FROM  offensive_player_stats ops JOIN offensive_average_stats oav
                                    ON (oav.game_id = ops.game_id AND oav.cfl_central_id = ops.cfl_central_id)
                                                                  JOIN team_training_data ttd
                                    ON (ttd.game_id = oav.game_id AND ttd.team_id = oav.team_id)
                                                                  JOIN defensive_average_stats dts
                                    ON (oav.game_id = dts.game_id AND ttd.opp_team_id = dts.team_id)""", conn)

# Get test data
test = pd.read_sql_query("""SELECT  oav.*, dts.*, ttd.home_or_away
                            FROM  offensive_average_stats oav JOIN team_training_data ttd
                                ON (ttd.game_id = oav.game_id AND ttd.team_id = oav.team_id)
                                                              JOIN defensive_average_stats dts
                                ON (oav.game_id = dts.game_id AND ttd.opp_team_id = dts.team_id)
                                                              JOIN games ON oav.game_id = games.game_id
                            WHERE event_status = 'Pre-Game'
                            AND  games.week = (SELECT MIN(week) FROM games WHERE event_status = 'Pre-Game')
                            AND  games.season = (SELECT MAX(season) FROM games WHERE event_status = 'Pre-Game')""", conn)
print training
print test

# Columns we want to predict
targets = ['pass_net_yards', 'pass_touchdowns', 'pass_interceptions', 'rush_net_yards', 'rush_touchdowns',
            'receive_caught', 'receive_yards', 'receive_touchdowns', 'punt_returns_yards' 'punt_returns_touchdowns',
            'kick_returns_yards', 'kick_returns_touchdowns']

# Columns to remove from both datasets
remove_cols = []

# Columns that need to be one hot encoded
one_hot_encode = ['team_id', 'home_or_away']

for target in targets:
    print target
    # Train model on target column

    # Predict on target column

    # Combine all targets into one dataframe

conn.close()

# Remove the tuples
# = [f[0] for f in future_games]