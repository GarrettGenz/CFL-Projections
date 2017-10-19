DELETE FROM team_training_data;

-- Insert home teams
INSERT INTO team_training_data(game_id, team_id, home_or_away, opp_team_id)
SELECT game_id, home_team, 1, away_team 
FROM games
WHERE event_status = 'Final';

-- Insert away teams
INSERT INTO team_training_data(game_id, team_id, home_or_away, opp_team_id)
SELECT game_id, away_team, 0, home_team 
FROM games
WHERE event_status = 'Final';

SELECT * FROM team_training_data;

/* Insert data for games that happen in the next week */

-- Insert home teams
INSERT INTO team_training_data(game_id, team_id, home_or_away, opp_team_id)
SELECT game_id, home_team, 1, away_team 
FROM games
WHERE event_status = 'Pre-Game'
AND  games.week = (SELECT MIN(week) FROM games WHERE event_status = 'Pre-Game')
AND  games.season = (SELECT MAX(season) FROM games WHERE event_status = 'Pre-Game');

-- Insert away teams
INSERT INTO team_training_data(game_id, team_id, home_or_away, opp_team_id)
SELECT game_id, away_team, 0, home_team 
FROM games
WHERE event_status = 'Pre-Game'
AND  games.week = (SELECT MIN(week) FROM games WHERE event_status = 'Pre-Game')
AND  games.season = (SELECT MAX(season) FROM games WHERE event_status = 'Pre-Game');

UPDATE	team_training_data ttd
SET	prev_game_id =	
(	SELECT MIN (game_id)
	FROM
		(SELECT ttd2.game_id AS "game_id"
		FROM	team_training_data ttd2
		WHERE	ttd2.team_id = ttd.team_id
		AND 	ttd2.game_id < ttd.game_id	
		ORDER BY ttd2.game_id DESC LIMIT (SELECT config.value FROM config WHERE config.name = 'num_prev_games')) temp);

--SELECT	* FROM team_training_data
		