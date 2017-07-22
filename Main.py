import psycopg2
import codecs

import GetPlayers
import GetGames
import GetTeams
import UpdateGames
import GetPlayerStats
import GetPlayerProjections
import config

conn = psycopg2.connect(host=config.endpoint, database=config.database, user=config.user, password=config.password)
cur = conn.cursor()

# print ('Get Teams...')
# GetTeams.main()
#
# print ('Get Players...')
# GetPlayers.main()
#
# print ('Get Games...')
# GetGames.main()
#
# print ('Update Games...')
# UpdateGames.main()
#
# print ('Get Player Stats...')
# GetPlayerStats.main()
#
print ('Populate table Offensive_Player_Stats...')
cur.execute(codecs.open("PopulateOffensivePlayerStats.sql", "r", encoding='us-ascii').read())
conn.commit()

print ('Populate table defensive_team_stats...')
cur.execute(codecs.open("PopulateDefensiveTeamStats.sql", "r", encoding='us-ascii').read())
conn.commit()

print ('Populate table game_player_proj_status...')
cur.execute(codecs.open("PopulatePlayerStatusProjections.sql", "r", encoding='us-ascii').read())
conn.commit()

print ('Populate table team_training_data...')
cur.execute(codecs.open("PopulateTeamTrainingData.sql", "r", encoding='us-ascii').read())
conn.commit()

# print ('Get Player Projections...')
GetPlayerProjections.main()

print ('Update player_projections table with projected points...')
cur.execute(codecs.open("UpdatePlayerProjections.sql", "r", encoding='us-ascii').read())
conn.commit()

#print ('Get optimal combinations for Draftkings...')
#cur.execute(codecs.open("GetDraftkingsCombos.sql", "r", encoding='us-ascii').read())
#conn.commit()

cur.close()
conn.close()
