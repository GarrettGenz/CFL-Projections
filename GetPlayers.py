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
def get_players(players):
    for player in players:
        if player['position']['position_id'] in position_ids:
            if player['birth_date'] == '':
                player['birth_date'] = datetime.date(1990, 01, 01)
            conn = psycopg2.connect(host=config.endpoint, database=config.database, user=config.user, password=config.password)
            cur = conn.cursor()
            cur.execute("""INSERT INTO players("cfl_central_id", "stats_inc_id", "first_name",
                        "middle_name", "last_name", "birth_date", "position_id", "position_abbreviation") SELECT %s, %s,
                        %s, %s, %s, %s, %s, %s
                        WHERE NOT EXISTS(SELECT * FROM players WHERE cfl_central_id = %s)""",
                        ((str(player['cfl_central_id']), str(player['stats_inc_id']), player['first_name'],
                          player['middle_name'], player['last_name'], player['birth_date'], str(player['position']['position_id']),
                          player['position']['abbreviation'], str(player['cfl_central_id'])
                          )))
  #          if player['team']['is_set']:
  #              print player[
  #                  'team']  # str(player['team']['team_id']) + ', ' + player['team']['location'] + ' ' + player['team']['nickname']
            conn.commit()
            cur.close()
            conn.close()

player_endpoint = '/v1/players'

params = {  'key' : config.auth,
            'include' : 'current_team',
            'filter[position_id][lt]' : '10',
            'sort' : '-rookie_year',
            'page[number]' : 1,
            'page[size]' : 100
}

position_ids = [1, # Quarterback
                2, # Running back
                3, # Full back
                4, # Slot back
                8, # Wide Receiver
                9] # Receiver/Kick returner

last_page = False

while not last_page:

    # Make request
    players = make_request(player_endpoint, params)

    if len(players) > 0:

        print 'Grabbing ' + str(len(players)) + ' players from page ' + str(params['page[number]'])
        # Save data to database
        get_players(players)

        # Don't want to call API more than 30 times/sec
        time.sleep(5)

        # Make request for the next page
        params['page[number]'] += 1

    # If 0 players are returned, exit the loop
    else:
        last_page = True

