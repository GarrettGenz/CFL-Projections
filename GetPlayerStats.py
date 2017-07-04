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

# Save player stats into database
def get_playerstats(playerstats):
    team1 = playerstats[0]['boxscore']['teams']['team_1']
    team2 = playerstats[0]['boxscore']['teams']['team_2']
    print team1['abbreviation']
    print team2['abbreviation']
  #   for playerstat in playerstats:
  #       if player['position']['position_id'] in position_ids:
  #           if player['birth_date'] == '':
  #               player['birth_date'] = datetime.date(1990, 01, 01)
  #           conn = psycopg2.connect(host=config.endpoint, database=config.database, user=config.user, password=config.password)
  #           cur = conn.cursor()
  #           cur.execute("""INSERT INTO players("cfl_central_id", "stats_inc_id", "first_name",
  #                       "middle_name", "last_name", "birth_date", "position_id", "position_abbreviation") SELECT %s, %s,
  #                       %s, %s, %s, %s, %s, %s
  #                       WHERE NOT EXISTS(SELECT * FROM players WHERE cfl_central_id = %s)""",
  #                       ((str(player['cfl_central_id']), str(player['stats_inc_id']), player['first_name'],
  #                         player['middle_name'], player['last_name'], player['birth_date'], str(player['position']['position_id']),
  #                         player['position']['abbreviation'], str(player['cfl_central_id'])
  #                         )))
  # #          if player['team']['is_set']:
  # #              print player[
  # #                  'team']  # str(player['team']['team_id']) + ', ' + player['team']['location'] + ' ' + player['team']['nickname']
  #           conn.commit()
  #           cur.close()
  #           conn.close()

game_id = 2371
game_season = 2017

playerstats_endpoint = '/v1/games/' + str(game_season) + '/game/' + str(game_id)

params = {  'key' : config.auth,
            'include' : 'boxscore',
}

# Make request
playerstats = make_request(playerstats_endpoint, params)

if len(playerstats) > 0:

    print 'Grabbing data for game ' + str(game_id) + ' in season ' + str(game_season)
    # Save data to database
    get_playerstats(playerstats)

    # Don't want to call API more than 30 times/sec
    time.sleep(5)