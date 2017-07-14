import numpy as np
import pandas as pd
import xgboost as xgb
import gc
import matplotlib.pyplot as plt
import psycopg2
import config
from sklearn.linear_model import LassoCV


def one_hot_encoding(cols, train, test):
    for col in cols:
        # Perform encoding on training data
        one_hot = pd.get_dummies(train[col], prefix=col)
        train = train.drop(col, axis=1)
        train = train.join(one_hot)

        # Perform encoding on test data
        one_hot = pd.get_dummies(test[col], prefix=col)
        test = test.drop(col, axis=1)
        test = test.join(one_hot)

    return train, test


def match_categoricals(cols, train, test):
    for col in cols:
        test[col] = test[col].astype("category", categories=train[col].unique(), ordered=False)
    return train, test


print ('Loading Data...')

conn = psycopg2.connect(host=config.endpoint, database=config.database, user=config.user, password=config.password)

cur = conn.cursor()

# Get training data
training = pd.read_sql_query("""SELECT  * FROM training_data""", conn)

# Get test data
test = pd.read_sql_query("""SELECT  * FROM test_data""", conn)

# Columns used only to insert data into database. Do not train on them
insert_data = ['game_id', 'cfl_central_id']

# Columns we want to predict
targets = ['pass_net_yards', 'pass_touchdowns', 'pass_interceptions', 'rush_net_yards', 'rush_touchdowns',
           'receive_caught', 'receive_yards', 'receive_touchdowns', 'punt_returns_yards', 'punt_returns_touchdowns',
           'kick_returns_yards', 'kick_returns_touchdowns']

# Columns that need to be one hot encoded
one_hot_encode = ['team_id', 'home_or_away', 'is_starter']

print ('One hot encode data...')

training, test = match_categoricals(one_hot_encode, training, test)

training, test = one_hot_encoding(one_hot_encode, training, test)

# Create list of training cols
train_cols = list(training)

predict_cols = insert_data + targets

# Remove cols from the list of training cols
for col in predict_cols :
    print ('Removing ' + col + ' from training data...')
    train_cols.remove(col)


print ('Update training NaN values to 0...')
for col in training:
    # Update NaN values to 0
    training[col].fillna(0, inplace=True)

print ('Update test NaN values to 0...')

for col in test:
    print col
    test[col].fillna(0, inplace=True)

# Store player projections here
player_projs = pd.DataFrame(columns=predict_cols)

col_algs = []

for target in targets:
    print ('Training on ' + target + '...')

    # Train model on target column
    # Save each alg as a list of [alg, col_name_to_predict]
    col_algs.append( [LassoCV(alphas=[1, 0.1, 0.001, 0.0005]).fit(training[train_cols], training[target]), target] )

print ('Predicting on ' + target + '...')

# Predict on each target column
for index, row in test.iterrows():
    for alg, col in col_algs:
        player_projs[col] = alg.predict(test[train_cols].loc[[index]])
        if player_projs[col].values < 0:
            player_projs[col] = 0

    # Grab data needed to insert projection into database
    player_projs["game_id"] = row['game_id']
    player_projs["cfl_central_id"] = row['cfl_central_id']

    print ('Inserting data for ' + str(row['cfl_central_id']))
    cur.execute("""DELETE FROM player_projections WHERE game_id = %s AND cfl_central_id = %s""",
                (str(row['game_id']), str(row['cfl_central_id']) ))
    conn.commit()

    cur.execute(
        """INSERT INTO player_projections (game_id, cfl_central_id, pass_net_yards, pass_touchdowns, pass_interceptions,
        rush_net_yards, rush_touchdowns, receive_caught, receive_yards, receive_touchdowns, punt_returns_yards,
        punt_returns_touchdowns, kick_returns_yards, kick_returns_touchdowns)"""
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
        player_projs.astype(np.float64).values[0, :])
    conn.commit()

cur.close()
conn.close()