import numpy as np
import pandas as pd
import xgboost as xgb
import gc
import matplotlib.pyplot as plt
import psycopg2
import config
from sklearn.linear_model import LassoCV
from sklearn.preprocessing import StandardScaler

from sklearn.model_selection import GridSearchCV
import pickle
import os


def one_hot_encoding(cols, train, test):
    for col in cols:
        # Perform encoding on training data
        one_hot = pd.get_dummies(train[col], prefix=col)
        if col <> 'team_id':
            train = train.drop(col, axis=1)
        train = train.join(one_hot)

        # Perform encoding on test data
        one_hot = pd.get_dummies(test[col], prefix=col)
        if col <> 'team_id':
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
    training = pd.read_sql_query("""SELECT  * FROM training_data_def""", conn)

    # Get test data
    test = pd.read_sql_query("""SELECT  * FROM test_data_def""", conn)

    cur.close()
    conn.close()

    # Columns used only to insert data into database.
    insert_data = ['game_id', 'team_id']

    # Columns we want to predict
    targets = ['sacks', 'pass_touchdowns', 'pass_interceptions', 'rush_touchdowns', 'receive_touchdowns',
               'punt_returns_touchdowns', 'kick_returns_touchdowns']

    # Columns that need to be one hot encoded
    one_hot_encode = ['team_id', 'home_or_away']

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

    print test[test.isnull().any(axis=1)]

    for col in test:
        # Update NaN values to 0
        if col <> 'team_id':
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
        params = {'n_estimators': [10, 50, 150], 'max_depth': [6, 15, 30], 'learning_rate': [0.01, 0.05, 0.1]}

        fit_params = {"early_stopping_rounds": 30,
                      "eval_metric": "mae",
                      "eval_set": [[training[train_cols], training[target]]]}

        # Get directory of current file
        dir = os.path.dirname(__file__)

        # Get path to the model file
        file_path = 'models/defense/' + target + '.dat'

        # If the file already exists load it. Otherwise generate it
        if os.path.exists(os.path.join(dir, file_path)):
            xgb_alg = pickle.load(open(file_path, "rb"))
        else:
            gsCV = GridSearchCV(estimator=rf_test, param_grid=params, cv=4, n_jobs=-1, verbose=1, fit_params=fit_params)
            gsCV.fit(training[train_cols], training[target])
            print(gsCV.best_estimator_)
            print(gsCV.best_params_)

            xgb_alg = xgb.XGBRegressor()
            xgb_alg.set_params(**gsCV.best_params_)

            # Train model on training data
            xgb_alg.fit(training[train_cols], training[target])

            # Save model to file for the next run
            pickle.dump(xgb_alg, open(file_path, "wb"))

        # Use best params from GridSearchCV for each target
        col_algs.append([xgb_alg,
                         LassoCV(alphas=[1, 0.1, 0.001, 0.0005]).fit(training[train_cols], training[target]), target])

    conn = psycopg2.connect(host=config.endpoint, database=config.database, user=config.user,
                            password=config.password)

    cur = conn.cursor()

    # Predict on each target column
    for index, row in test.iterrows():
        for alg1, alg2, col in col_algs:
            player_projs[col] = alg1.predict(test[train_cols].loc[[index]]) * .5 + alg2.predict(test[train_cols].loc[[index]]) * .5
            if player_projs[col].values < 0:
                player_projs[col] = 0

        # Grab data needed to insert projection into database
        player_projs["game_id"] = row['game_id']
        player_projs["team_id"] = row['team_id']

        print ('Inserting data for ' + str(row['team_id']))

        cur.execute("""DELETE FROM defense_projections WHERE game_id = %s AND team_id = %s""",
                    (str(row['game_id']), str(row['team_id'])))
        conn.commit()

        cur.execute(
            """INSERT INTO defense_projections (game_id, team_id, sacks, pass_touchdowns, pass_interceptions,
            rush_touchdowns, receive_touchdowns, punt_returns_touchdowns, kick_returns_touchdowns)"""
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
            player_projs.astype(np.float64).values[0, :])
        conn.commit()

    cur.close()
    conn.close()