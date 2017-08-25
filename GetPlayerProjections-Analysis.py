import numpy as np
import pandas as pd
import xgboost as xgb
import gc
import matplotlib.pyplot as plt
import psycopg2
import config
from sklearn.linear_model import LassoCV
from sklearn.preprocessing import StandardScaler

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2


def one_hot_encoding(cols, train, test):
    for col in cols:
        # Perform encoding on training data
        one_hot = pd.get_dummies(train[col], prefix=col)
        if col <> 'cfl_central_id':
            train = train.drop(col, axis=1)
        train = train.join(one_hot)

        # Perform encoding on test data
        one_hot = pd.get_dummies(test[col], prefix=col)
        if col <> 'cfl_central_id':
            test = test.drop(col, axis=1)
        test = test.join(one_hot)

    return train, test


def match_categoricals(cols, train, test):
    for col in cols:
        # Grab unique values for the column from the training and test datasets
        test[col] = test[col].astype("category",
                    categories=np.unique(np.concatenate((train[col].unique(), test[col].unique()))), ordered=False)
    return train, test

print ('Loading Data...')

conn = psycopg2.connect(host=config.endpoint, database=config.database, user=config.user, password=config.password)

cur = conn.cursor()

# Get training data
data = pd.read_sql_query("""SELECT td.*, ops.fantasy_points
                                FROM analysis_data td JOIN offensive_player_stats ops 
                                    ON td.cfl_central_id = ops.cfl_central_id AND td.game_id = ops.game_id
                                    """, conn)

training, test = train_test_split(data)

# Only include starters in the test group
training = training[training['is_starter'] == True]
test = test[test['is_starter'] == True]


cur.close()
conn.close()

# Columns used only to insert data into database. Do not train on them
insert_data = ['game_id', 'cfl_central_id', 'fantasy_points']

# Columns we want to predict
targets = ['pass_net_yards', 'pass_touchdowns', 'pass_interceptions', 'rush_net_yards', 'rush_touchdowns',
           'receive_caught', 'receive_yards', 'receive_touchdowns', 'punt_returns_yards', 'punt_returns_touchdowns',
           'kick_returns_yards', 'kick_returns_touchdowns']

# Columns that need to be one hot encoded
one_hot_encode = ['team_id', 'home_or_away', 'is_starter', 'position_abbreviation', 'cfl_central_id',
                      'percent_games_cur_season', 'num_games', 'wr_depth_num']

training['position_abbreviation'] = training['position_abbreviation'].fillna('NA')
test['position_abbreviation'] = test['position_abbreviation'].fillna('NA')

# Drop any rows where one hot encoded columns are null
training = training.dropna(subset=one_hot_encode)
test = test.dropna(subset=one_hot_encode)

print ('Set negative values to 0...')

# Set negative values to 0
for col in training:
        neg_index = training[col] < 0
        training.loc[neg_index, col] = 0

for col in test:
        neg_index = test[col] < 0
        test.loc[neg_index, col] = 0

print ('One hot encode data...')

training, test = match_categoricals(one_hot_encode, training, test)

training, test = one_hot_encoding(one_hot_encode, training, test)

# Create list of training cols
train_cols = list(training)

predict_cols = insert_data + targets

# Remove cols from the list of training cols
for col in predict_cols:
    print ('Removing ' + col + ' from training data...')
    train_cols.remove(col)

print ('Update training NaN values to 0...')
for col in training:
    # Update NaN values to 0
    training[col].fillna(0, inplace=True)

print ('Update test NaN values to 0...')

for col in test:
    if col <> 'cfl_central_id':
        test[col].fillna(0, inplace=True)

print ('Scale data...')
# Scale data
scaler = StandardScaler()
training[train_cols] = scaler.fit_transform(training[train_cols])
test[train_cols] = scaler.transform(test[train_cols])

# Store player projections here
player_projs = pd.DataFrame(columns=predict_cols)

col_algs = []

for target in targets:
    print ('Training on ' + target + '...')

    # Train model on target column
    # Save each alg as a list of [alg, col_name_to_predict]
    rf_test = xgb.XGBRegressor()
    print rf_test.get_params().keys()
    params = {'n_estimators': [10, 50, 150], 'max_depth': [6], 'learning_rate': [0.01, 0.05, 0.1], 'seed': [1337]}

    fit_params = {"early_stopping_rounds": 40,
                  "eval_metric": "mae",
                  "eval_set": [[training[train_cols], training[target]]]}

    gsCV = GridSearchCV(estimator=rf_test, param_grid=params, cv=4, n_jobs=-1, verbose=3, fit_params=fit_params)
    gsCV.fit(training[train_cols], training[target])
    print(gsCV.best_estimator_)
    print(gsCV.best_params_)

    # Use best params from GridSearchCV for each target
    xgb_alg = xgb.XGBRegressor()
    xgb_alg.set_params(**gsCV.best_params_)


    col_algs.append([xgb_alg.fit(training[train_cols], training[target]),
                    LassoCV(alphas=[1, 0.1, 0.001, 0.0005]).fit(training[train_cols], training[target]), target])

player_projs_all = []#pd.DataFrame(columns=['fantasy_points', 'proj_points', 'diff', 'abs_diff'])

# Predict on each target column
for index, row in test.iterrows():
    for alg1, alg2, col in col_algs:
        #player_projs[col] = np.expm1(alg.predict(test[train_cols].loc[[index]]))
        player_projs[col] = alg1.predict(test[train_cols].loc[[index]]) * .5 + alg2.predict(test[train_cols].loc[[index]]) * .5
        if player_projs[col].values < 0:
            player_projs[col] = 0

    # Grab data needed to insert projection into database
    player_projs["game_id"] = row['game_id']
    player_projs["cfl_central_id"] = row['cfl_central_id']
    player_projs["fantasy_points"] = row['fantasy_points']

    player_projs["proj_points"] = (4 * player_projs["pass_touchdowns"]) + (0.04 * player_projs["pass_net_yards"]) + \
                        (-1 * player_projs["pass_interceptions"]) + (0.1 * player_projs["rush_net_yards"]) + \
                        (6 * player_projs["rush_touchdowns"]) + (0.1 * player_projs["receive_yards"]) + \
                      (6 * player_projs["receive_touchdowns"]) + (6 * player_projs["punt_returns_touchdowns"]) + \
                      (6 * player_projs["kick_returns_touchdowns"]) + \
                      (0.05 * player_projs["punt_returns_yards"]) + (0.05 * player_projs["kick_returns_yards"])

    if player_projs["pass_net_yards"].values > 300:
        player_projs["proj_points"] = player_projs["proj_points"] + 3

    if player_projs["rush_net_yards"].values > 100:
        player_projs["proj_points"] = player_projs["proj_points"] + 3

    if player_projs["receive_yards"].values > 100:
        player_projs["proj_points"] = player_projs["proj_points"] + 3

    player_projs_all.append(player_projs["fantasy_points"] - player_projs["proj_points"])

    # print player_projs
    # print player_projs["fantasy_points"] - player_projs["proj_points"]
    # print abs(player_projs["fantasy_points"] - player_projs["proj_points"])

    #player_projs_all["diff"] = player_projs["fantasy_points"] - player_projs["proj_points"]
    #player_projs_all["abs_diff"] = abs(player_projs["fantasy_points"] - player_projs["proj_points"])

player_projs_all_abs = [abs(row) for row in player_projs_all]

print sum(player_projs_all) / len(player_projs_all)
print sum(player_projs_all_abs) / len(player_projs_all)
