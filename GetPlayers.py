import requests
import config
import json

url = 'http://api.cfl.ca'
player_endpoint = '/v1/players'

params = dict(
        key = config.auth,
        include = 'current_team'
)

# Make request
r = requests.get(url + player_endpoint, params)

# dumps converts request into JSON string
# loads converts JSON string into Python object
players = json.loads(json.dumps(r.json()))

# Only grab the player data
players = players['data']

for player in players:
    print player['first_name'] + ' ' + player['last_name'] + ', '
#    print player
    if player['team']['is_set']:
        print str(player['team']['team_id']) + ', ' + player['team']['location'] + ' ' + player['team']['nickname']