DELETE FROM defensive_team_stats;

INSERT INTO defensive_team_stats
SELECT	game_id, home_team
FROM	games
WHERE	event_status = 'Final'
UNION ALL
SELECT	game_id, away_team
FROM	games
WHERE	event_status = 'Final';

UPDATE defensive_team_stats dts
SET 	pass_attempts = dps.pass_attempts,
	pass_completions = dps.pass_completions,
	pass_net_yards = dps.pass_net_yards,
	pass_long = dps.pass_long,
	pass_touchdowns = dps.pass_touchdowns,
	pass_interceptions = dps.pass_interceptions
   FROM def_passing_stats dps
WHERE 	dps.game_id = dts.game_id
AND	dps.def_team = dts.team_id;

UPDATE defensive_team_stats dts
SET 	rush_attempts = drs.rush_attempts,
	rush_net_yards = drs.rush_net_yards,
	rush_long = drs.rush_long,
	rush_touchdowns = drs.rush_touchdowns,
	rush_long_touchdowns = drs.rush_long_touchdowns
   FROM def_rushing_stats drs
WHERE 	drs.game_id = dts.game_id
AND	drs.def_team = dts.team_id;

UPDATE defensive_team_stats dts
SET 	receive_attempts = drs.receive_attempts,
	receive_caught = drs.receive_caught,
	receive_yards = drs.receive_yards,
	receive_long = drs.receive_long,
	receive_touchdowns = drs.receive_touchdowns,
	receive_long_touchdowns = drs.receive_long_touchdowns,
	receive_yards_after_catch = drs.receive_yards_after_catch
   FROM def_receiving_stats drs
WHERE 	drs.game_id = dts.game_id
AND	drs.def_team = dts.team_id;

UPDATE defensive_team_stats dts
SET 	punt_returns = drs.punt_returns,
	punt_returns_yards = drs.punt_returns_yards,
	punt_returns_touchdowns = drs.punt_returns_touchdowns,
	punt_returns_long = drs.punt_returns_long,
	punt_returns_touchdowns_long = drs.punt_returns_touchdowns_long
   FROM def_punt_return_stats drs
WHERE 	drs.game_id = dts.game_id
AND	drs.def_team = dts.team_id;

UPDATE defensive_team_stats dts
SET 	kick_returns = drs.kick_returns,
	kick_returns_yards = drs.kick_returns_yards,
	kick_returns_touchdowns = drs.kick_returns_touchdowns,
	kick_returns_long = drs.kick_returns_long,
	kick_returns_touchdowns_long = drs.kick_returns_touchdowns_long
   FROM def_kick_return_stats drs
WHERE 	drs.game_id = dts.game_id
AND	drs.def_team = dts.team_id;

--SELECT * FROM defensive_team_stats;