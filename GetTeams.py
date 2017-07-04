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

# Save team data into database
def get_teams(teams):
     for team in teams:
        print team
        conn = psycopg2.connect(host=config.endpoint, database=config.database, user=config.user, password=config.password)
        cur = conn.cursor()
        cur.execute("""INSERT INTO teams("team_id", "abbreviation", "location", "nickname", "full_name",
                        "venue_id", "venue_name") 
                        SELECT %s, %s, %s, %s, %s, %s, %s WHERE NOT EXISTS(SELECT * FROM teams WHERE team_id = %s)""",
                    (str(team['team_id']), team['abbreviation'], team['location'], team['nickname'], team['full_name'],
                     str(team['venue_id']), team['venue_name'], str(team['team_id'])
                      ))
        conn.commit()
        cur.close()
        conn.close()

teams_endpoint = '/v1/teams/'

params = {  'key' : config.auth
}

# Make request
teams = make_request(teams_endpoint, params)

if len(teams) > 0:
    print 'Grabbing ' + str(len(teams)) + ' teams'

    # Save data to database
    get_teams(teams)

    # Don't want to call API more than 30 times/sec
    time.sleep(5)

