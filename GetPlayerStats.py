import requests
import config
import json
import time
import psycopg2
import datetime

url = 'http://api.cfl.ca'
conn = psycopg2.connect(host=config.endpoint, database=config.database, user=config.user, password=config.password)
cur = conn.cursor()

game_season = 2017

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

def get_passing(game_id, team_id, players):
    for player in players['passing']:
        cur.execute("""INSERT INTO passing_stats("game_id", "team_id", "cfl_central_id", "pass_attempts",
                                "pass_completions", "pass_net_yards", "pass_long", "pass_touchdowns", 
                                "pass_interceptions") SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
                                WHERE NOT EXISTS(SELECT * FROM passing_stats WHERE game_id = %s AND team_id = %s 
                                AND cfl_central_id = %s)""",
                              ((str(game_id), str(team_id), str(player['player']['cfl_central_id']),
                                str(player['pass_attempts']), str(player['pass_completions']), str(player['pass_net_yards']),
                                str(player['pass_long']), str(player['pass_touchdowns']), str(player['pass_interceptions']),
                                str(game_id), str(team_id), str(player['player']['cfl_central_id'])
                                )))
        conn.commit()

def get_rushing(game_id, team_id, players):
    for player in players['rushing']:
        cur.execute("""INSERT INTO rushing_stats("game_id", "team_id", "cfl_central_id", "rush_attempts",
                                "rush_net_yards", "rush_long", "rush_touchdowns", 
                                "rush_long_touchdowns") SELECT %s, %s, %s, %s, %s, %s, %s, %s
                                WHERE NOT EXISTS(SELECT * FROM rushing_stats WHERE game_id = %s AND team_id = %s 
                                AND cfl_central_id = %s)""",
                    ((str(game_id), str(team_id), str(player['player']['cfl_central_id']),
                      str(player['rush_attempts']), str(player['rush_net_yards']), str(player['rush_long']),
                      str(player['rush_touchdowns']), str(player['rush_long_touchdowns']),
                      str(game_id), str(team_id), str(player['player']['cfl_central_id'])
                      )))
        conn.commit()

def get_receiving(game_id, team_id, players):
    for player in players['receiving']:
        cur.execute("""INSERT INTO receiving_stats("game_id", "team_id", "cfl_central_id", "receive_attempts",
                                "receive_caught", "receive_yards", "receive_long", "receive_touchdowns",
                                "receive_long_touchdowns", "receive_yards_after_catch") 
                                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                WHERE NOT EXISTS(SELECT * FROM receiving_stats WHERE game_id = %s AND team_id = %s 
                                AND cfl_central_id = %s)""",
                    ((str(game_id), str(team_id), str(player['player']['cfl_central_id']),
                      str(player['receive_attempts']), str(player['receive_caught']), str(player['receive_yards']),
                      str(player['receive_long']), str(player['receive_touchdowns']), str(player['receive_long_touchdowns']),
                      str(player['receive_yards_after_catch']), str(game_id), str(team_id), str(player['player']['cfl_central_id'])
                      )))
        conn.commit()

def get_punt_returns(game_id, team_id, players):
    for player in players['punt_returns']:
        cur.execute("""INSERT INTO punt_return_stats("game_id", "team_id", "cfl_central_id", "punt_returns",
                                "punt_returns_yards", "punt_returns_touchdowns", "punt_returns_long",
                                "punt_returns_touchdowns_long") 
                                SELECT %s, %s, %s, %s, %s, %s, %s, %s
                                WHERE NOT EXISTS(SELECT * FROM punt_return_stats WHERE game_id = %s AND team_id = %s 
                                AND cfl_central_id = %s)""",
                    ((str(game_id), str(team_id), str(player['player']['cfl_central_id']),
                      str(player['punt_returns']), str(player['punt_returns_yards']), str(player['punt_returns_touchdowns']),
                      str(player['punt_returns_long']), str(player['punt_returns_touchdowns_long']),
                      str(game_id), str(team_id), str(player['player']['cfl_central_id'])
                      )))
        conn.commit()

def get_kick_returns(game_id, team_id, players):
    for player in players['kick_returns']:
        cur.execute("""INSERT INTO kick_return_stats("game_id", "team_id", "cfl_central_id", "kick_returns",
                                "kick_returns_yards", "kick_returns_touchdowns", "kick_returns_long",
                                "kick_returns_touchdowns_long") 
                                SELECT %s, %s, %s, %s, %s, %s, %s, %s
                                WHERE NOT EXISTS(SELECT * FROM kick_return_stats WHERE game_id = %s AND team_id = %s 
                                AND cfl_central_id = %s)""",
                    ((str(game_id), str(team_id), str(player['player']['cfl_central_id']),
                      str(player['kick_returns']), str(player['kick_returns_yards']),
                      str(player['kick_returns_touchdowns']),
                      str(player['kick_returns_long']), str(player['kick_returns_touchdowns_long']),
                      str(game_id), str(team_id), str(player['player']['cfl_central_id'])
                      )))
        conn.commit()

def get_defensive_player_stats(game_id, team_id, players):
    for player in players['defence']:
        cur.execute("""INSERT INTO defensive_player_stats("game_id", "team_id", "cfl_central_id", "tackles_total",
                                "tackles_defensive", "tackles_special_teams", "sacks_qb_made", "interceptions",
                                "fumbles_forced", "fumbles_recovered", "passes_knocked_down") 
                                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                WHERE NOT EXISTS(SELECT * FROM defensive_player_stats WHERE game_id = %s AND team_id = %s 
                                AND cfl_central_id = %s)""",
                    ((str(game_id), str(team_id), str(player['player']['cfl_central_id']),
                      str(player['tackles_total']), str(player['tackles_defensive']),
                      str(player['tackles_special_teams']), str(player['sacks_qb_made']),
                      str(player['interceptions']), str(player['fumbles_forced']), str(player['fumbles_recovered']),
                      str(player['passes_knocked_down']),
                      str(game_id), str(team_id), str(player['player']['cfl_central_id'])
                      )))
        conn.commit()

# Save to database whether player was a starter or inactive
def get_player_statuses(game_id, team_id, players):
    for player in players:
        if player['is_starter'] or player['is_inactive']:
            cur.execute("""INSERT INTO game_player_status("game_id", "team_id", "cfl_central_id", "is_starter",
                                    "is_inactive")
                                    SELECT %s, %s, %s, %s, %s
                                    WHERE NOT EXISTS(SELECT * FROM game_player_status WHERE game_id = %s AND team_id = %s
                                    AND cfl_central_id = %s)""",
                        ((str(game_id), str(team_id), str(player['cfl_central_id']),
                          player['is_starter'], player['is_inactive'],
                          str(game_id), str(team_id), str(player['cfl_central_id'])
                          )))
            conn.commit()

# Save player stats into database
def get_playerstats(playerstats):
    team1 = playerstats[0]['boxscore']['teams']['team_1']
    team2 = playerstats[0]['boxscore']['teams']['team_2']
    team1_id = team1['team_id']
    team1 = team1['players']
    team2_id = team2['team_id']
    team2 = team2['players']
    get_passing(game_id, team1_id, team1)
    get_passing(game_id, team2_id, team2)
    get_rushing(game_id, team1_id, team1)
    get_rushing(game_id, team2_id, team2)
    get_receiving(game_id, team1_id, team1)
    get_receiving(game_id, team2_id, team2)
    get_punt_returns(game_id, team1_id, team1)
    get_punt_returns(game_id, team2_id, team2)
    get_kick_returns(game_id, team1_id, team1)
    get_kick_returns(game_id, team2_id, team2)
    get_defensive_player_stats(game_id, team1_id, team1)
    get_defensive_player_stats(game_id, team2_id, team2)

# Save player stats into database
def get_roster_data(playerstats):
    team1 = playerstats[0]['rosters']['teams']['team_1']
    team2 = playerstats[0]['rosters']['teams']['team_2']
    team1_id = team1['team_id']
    team1 = team1['roster']
    team2_id = team2['team_id']
    team2 = team2['roster']
    get_player_statuses(game_id, team1_id, team1)
    get_player_statuses(game_id, team2_id, team2)

params = {  'key' : config.auth,
            'include' : 'boxscore,rosters',
}

while game_season >= config.start_year:

    # Get list of games in season
    cur.execute("""SELECT game_id FROM games WHERE event_status = 'Final'
                    AND game_id NOT IN (SELECT game_id FROM defensive_player_stats)
                    AND season = %s""", ((str(game_season),)))

    games = cur.fetchall()

    for game in games:
        game_id = game[0]

        # Build the new endpoint
        playerstats_endpoint = '/v1/games/' + str(game_season) + '/game/' + str(game_id)

        # Make request
        playerstats = make_request(playerstats_endpoint, params)

        if len(playerstats) > 0:
            print 'Grabbing boxscore data for game ' + str(game_id) + ' in season ' + str(game_season)
            # Save data to database
            get_playerstats(playerstats)

            print 'Grabbing roster data for game ' + str(game_id) + ' in season ' + str(game_season)
            # Save data to database
            get_roster_data(playerstats)

        # Don't want to call API more than 30 times/sec
        time.sleep(5)

    game_season -= 1

cur.close()
conn.close()