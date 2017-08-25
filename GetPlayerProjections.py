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


def main():
    print ('Loading Data...')

    conn = psycopg2.connect(host=config.endpoint, database=config.database, user=config.user, password=config.password)

    cur = conn.cursor()

    # Get training data
    training = pd.read_sql_query("""SELECT  * FROM training_data""", conn)

    # Get test data
    test = pd.read_sql_query("""SELECT  * FROM test_data""", conn)

    cur.close()
    conn.close()

    # Only train on starters
    training = training[training['is_starter'] == True]

    # Columns used only to insert data into database.
    insert_data = ['game_id', 'cfl_central_id']

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

    # Remove cfl_central_id because if that category isn't in training set it will make it null
    # in the test set
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

        rf_test = xgb.XGBRegressor()
        params = {'n_estimators': [10, 50, 150], 'max_depth': [5], 'learning_rate': [0.01, 0.05, 0.1]}

        # fit_params = {"early_stopping_rounds": 40,
        #               "eval_metric": "mae",
        #               "eval_set": [[training[train_cols], training[target]]]}

        gsCV = GridSearchCV(estimator=rf_test, param_grid=params, cv=4, n_jobs=-1, verbose=3)
        #gsCV = GridSearchCV(estimator=rf_test, param_grid=params, cv=4, n_jobs=-1, verbose=3, fit_params=fit_params)
        gsCV.fit(training[train_cols], training[target])
        print(gsCV.best_estimator_)
        print(gsCV.best_params_)

        xgb_alg = xgb.XGBRegressor()
        xgb_alg.set_params(**gsCV.best_params_)

        # Use best params from GridSearchCV for each target
        col_algs.append([xgb_alg.fit(training[train_cols], training[target]),
                         LassoCV(alphas=[1, 0.1, 0.001, 0.0005]).fit(training[train_cols], training[target]), target])

    conn = psycopg2.connect(host=config.endpoint, database=config.database, user=config.user,
                            password=config.password)

    cur = conn.cursor()

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

        print ('Inserting data for ' + str(row['cfl_central_id']))

        cur.execute("""DELETE FROM player_projections WHERE game_id = %s AND cfl_central_id = %s""",
                    (str(row['game_id']), str(row['cfl_central_id'])))
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