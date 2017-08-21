DELETE FROM offensive_player_stats;

INSERT INTO offensive_player_stats(game_id, team_id, cfl_central_id)
SELECT	game_id, team_id, cfl_central_id
FROM	passing_stats 
UNION
SELECT	game_id, team_id, cfl_central_id
FROM	rushing_stats
UNION
SELECT	game_id, team_id, cfl_central_id
FROM	receiving_stats
UNION
SELECT	game_id, team_id, cfl_central_id
FROM	punt_return_stats
UNION
SELECT	game_id, team_id, cfl_central_id
FROM	kick_return_stats;

UPDATE offensive_player_stats ops
SET 	pass_attempts = ps.pass_attempts,
	pass_completions = ps.pass_completions,
	pass_net_yards = ps.pass_net_yards,
	pass_long = ps.pass_long,
	pass_touchdowns = ps.pass_touchdowns,
	pass_interceptions = ps.pass_interceptions
   FROM passing_stats ps
WHERE 	ps.game_id = ops.game_id
AND	ps.team_id = ops.team_id
AND	ps.cfl_central_id = ops.cfl_central_id;

UPDATE offensive_player_stats ops
SET 	rush_attempts = rs.rush_attempts,
	rush_net_yards = rs.rush_net_yards,
	rush_long = rs.rush_long,
	rush_touchdowns = rs.rush_touchdowns,
	rush_long_touchdowns = rs.rush_long_touchdowns
   FROM rushing_stats rs
WHERE 	rs.game_id = ops.game_id
AND	rs.team_id = ops.team_id
AND	rs.cfl_central_id = ops.cfl_central_id;

UPDATE offensive_player_stats ops
SET 	receive_attempts = rs.receive_attempts,
	receive_caught = rs.receive_caught,
	receive_yards = rs.receive_yards,
	receive_long = rs.receive_long,
	receive_touchdowns = rs.receive_touchdowns,
	receive_long_touchdowns = rs.receive_long_touchdowns,
	receive_yards_after_catch = rs.receive_yards_after_catch
   FROM receiving_stats rs
WHERE 	rs.game_id = ops.game_id
AND	rs.team_id = ops.team_id
AND	rs.cfl_central_id = ops.cfl_central_id;

UPDATE offensive_player_stats ops
SET 	punt_returns = prs.punt_returns,
	punt_returns_yards = prs.punt_returns_yards,
	punt_returns_touchdowns = prs.punt_returns_touchdowns,
	punt_returns_long = prs.punt_returns_long,
	punt_returns_touchdowns_long = prs.punt_returns_touchdowns_long
   FROM punt_return_stats prs
WHERE 	prs.game_id = ops.game_id
AND	prs.team_id = ops.team_id
AND	prs.cfl_central_id = ops.cfl_central_id;

UPDATE offensive_player_stats ops
SET 	kick_returns = krs.kick_returns,
	kick_returns_yards = krs.kick_returns_yards,
	kick_returns_touchdowns = krs.kick_returns_touchdowns,
	kick_returns_long = krs.kick_returns_long,
	kick_returns_touchdowns_long = krs.kick_returns_touchdowns_long
   FROM kick_return_stats krs
WHERE 	krs.game_id = ops.game_id
AND	krs.team_id = ops.team_id
AND	krs.cfl_central_id = ops.cfl_central_id;

