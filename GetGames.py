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

# Save player data into database
def get_games(games):
    for game in games:
        if game['event_status']['name'] == 'Final' and game['event_type']['event_type_id'] <> 0: # Preseason
            if game['team_1']['is_at_home']:
                home_team = game['team_1']['team_id']
                away_team = game['team_2']['team_id']
            else:
                home_team = game['team_2']['team_id']
                away_team = game['team_1']['team_id']
            conn = psycopg2.connect(host=config.endpoint, database=config.database, user=config.user, password=config.password)
            cur = conn.cursor()
            cur.execute("""INSERT INTO games("game_id", "date_start", "week", "season", "temperature", "home_team",
                            "away_team", "field_conditions") SELECT %s, %s,
                        %s, %s, %s, %s, %s, %s WHERE NOT EXISTS(SELECT * FROM games WHERE game_id = %s)""",
                        (str(game['game_id']), game['date_start'], game['week'], game['season'], game['weather']['temperature'],
                         home_team, away_team, game['weather']['field_conditions'], str(game['game_id'])
                          ))
            conn.commit()
            cur.close()
            conn.close()

games_endpoint = '/v1/games/'
games_year = datetime.datetime.now().year


params = {  'key' : config.auth,
            'page[number]' : 1,
            'page[size]' : 20
}

while games_year >= config.start_year:
        # Make request
        games = make_request(games_endpoint + str(games_year), params)

        if len(games) > 0:
            print 'Season ' + str(games_year) + ': Grabbing ' + str(len(games)) + ' games from page ' + str(params['page[number]'])

            # Save data to database
            get_games(games)

            # Don't want to call API more than 30 times/sec
            time.sleep(5)

            games_year -= 1

