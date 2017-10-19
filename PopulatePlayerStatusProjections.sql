INSERT INTO game_player_status(game_id, team_id, cfl_central_id, is_starter, is_inactive)
SELECT upcoming_games.game_id, gps.team_id, gps.cfl_central_id, gps.is_starter, gps.is_inactive
FROM	game_player_status gps JOIN 
	(SELECT team_id, MAX(game_id) AS game_id
	FROM	game_player_status
	GROUP BY team_id) latest_game 
		ON (gps.game_id = latest_game.game_id AND gps.team_id = latest_game.team_id)
				JOIN upcoming_games ON (home_team = gps.team_id OR away_team = gps.team_id)
WHERE NOT EXISTS (SELECT * FROM game_player_status gps2 WHERE upcoming_games.game_id = gps2.game_id AND
										gps.cfl_central_id = gps2.cfl_central_id);

INSERT INTO game_player_status(game_id, team_id, cfl_central_id, is_starter, is_inactive)
SELECT DISTINCT ttd.game_id, ttd.team_id, gps.cfl_central_id, False, True
FROM    game_player_status gps JOIN players ON gps.cfl_central_id = players.cfl_central_id
    JOIN team_training_data ttd
        ON (gps.team_id = ttd.team_id AND gps.game_id < ttd.game_id AND gps.game_id >= ttd.prev_game_id)
	  JOIN upcoming_games ON (ttd.game_id = upcoming_games.game_id)
WHERE   gps.cfl_central_id NOT IN ( SELECT cfl_central_id
																FROM    game_player_status
																WHERE   game_id = ttd.game_id
																AND 		team_id = ttd.team_id );

