import config
import psycopg2
import pandas as pd

conn = psycopg2.connect(host=config.endpoint, database=config.database, user=config.user, password=config.password)

cur = conn.cursor()

# Get training data
player_projs = pd.read_sql_query("""SELECT  *
                                    FROM    player_projections
                                    WHERE  game_id NOT IN (SELECT game_id FROM games WHERE event_status = 'Final')""",
                                 conn)

# Turn into function at some point
player_projs['proj_pts'] = (4 *  player_projs['pass_touchdowns']) +  (0.04 * player_projs['pass_net_yards']) + \
                           (-1 * player_projs['pass_interceptions']) + (0.1 * player_projs['rush_net_yards']) + \
                           (6 * player_projs['rush_touchdowns']) + (0.1 * player_projs['receive_yards']) + \
                           (6 * player_projs['receive_touchdowns']) + (6 * player_projs['punt_returns_touchdowns']) + \
                           (6 * player_projs['kick_returns_touchdowns']) + (0.05 * player_projs['punt_returns_yards']) + \
                           (0.05 * player_projs['kick_returns_yards'])

for index, row in player_projs.iterrows():
    if row['pass_net_yards'] > 300:
        row['proj_pts'] += 3

    if row['rush_net_yards'] > 100:
        row['proj_pts'] += 3

    if row['receive_yards'] > 100:
        row['proj_pts'] += 3


print player_projs


