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
training = pd.read_sql_query("""SELECT  * FROM training_data""", conn)

# Get test data
test = pd.read_sql_query("""SELECT  * FROM test_data""", conn)
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