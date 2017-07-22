import requests
import config
import json
import time
import psycopg2
import datetime

url = 'http://api.cfl.ca'

# Function to make request to CFL API
def make_request(endpoint, params):
    try:
        r = requests.get(url + endpoint, params)

        # Convert request into JSON
        data_json = json.dumps(r.json())

        # Convert JSON into Python object
        data = json.loads(data_json)

        # Only grab the data
        data = data['data']

    # If there is an error return empty JSON object
    except:
        data = {}

    return data

# Update new games that are finished to have a status of Final
def update_games(games):
    conn = psycopg2.connect(host=config.endpoint, database=config.database, user=config.user, password=config.password)
    cur = conn.cursor()

    # Get list of games that may need an update
    cur.execute("""SELECT game_id FROM games WHERE event_status <> 'Final'""")
    future_games = cur.fetchall()

    # Remove the tuples
    future_games = [f[0] for f in future_games]

    for game in games:
        if game['event_status']['name'] == 'Final' and game['game_id'] in future_games:
            print game['game_id']

            cur.execute("""UPDATE games SET temperature = %s, field_conditions = %s, event_status = %s
                           WHERE game_id = %s""",
                        (game['weather']['temperature'], game['weather']['field_conditions'], game['event_status']['name'],
                         str(game['game_id'])
                          ))
            conn.commit()
    cur.close()
    conn.close()

def main():
    games_endpoint = '/v1/games/'
    games_year = config.current_year


    params = {  'key' : config.auth
    }

    # Make request
    games = make_request(games_endpoint + str(games_year), params)

    if len(games) > 0:
        print 'Season ' + str(games_year) + ': Grabbing ' + str(len(games)) + ' games'

        # Update game statuses in database
        update_games(games)

        # Don't want to call API more than 30 times/sec
        time.sleep(5)

