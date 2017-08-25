-- Update player projections
UPDATE  player_projections
SET     proj_points = NULL;

UPDATE  player_projections
SET     proj_points = (4 * pass_touchdowns) + (0.04 * pass_net_yards) + (-1 * pass_interceptions) +
                      (0.1 * rush_net_yards) + (6 * rush_touchdowns) + (1 * receive_caught) +
                      (0.1 * receive_yards) +
                      (6 * receive_touchdowns) + (6 * punt_returns_touchdowns) + (6 * kick_returns_touchdowns) +
                      (0.05 * punt_returns_yards) + (0.05 * kick_returns_yards)
WHERE  game_id NOT IN (SELECT game_id FROM games WHERE event_status = 'Final');

UPDATE  player_projections
SET     proj_points = proj_points + 3
WHERE   game_id NOT IN (SELECT game_id FROM games WHERE event_status = 'Final')
AND     pass_net_yards >= 300;

UPDATE  player_projections
SET     proj_points = proj_points + 3
WHERE   game_id NOT IN (SELECT game_id FROM games WHERE event_status = 'Final')
AND     rush_net_yards >= 100;

UPDATE  player_projections
SET     proj_points = proj_points + 3
WHERE   game_id NOT IN (SELECT game_id FROM games WHERE event_status = 'Final')
AND     receive_yards >= 100;

-- Update defense projections
UPDATE  defense_projections
SET     proj_points = (sacks * 1 + defense_projections.pass_interceptions * 2)
WHERE   game_id NOT IN (SELECT game_id FROM games WHERE event_status = 'Final');

-- Calculate touchdowns scored to determine how many points were scored against the defense
-- Use seven points per touchdown
UPDATE  defense_projections
SET     proj_points = proj_points +
                      CASE
                          WHEN (pass_touchdowns + rush_touchdowns +
                               punt_returns_touchdowns + kick_returns_touchdowns) * 7 = 0 THEN 10
                          WHEN (pass_touchdowns + rush_touchdowns +
                               punt_returns_touchdowns + kick_returns_touchdowns) * 7 <= 6 THEN 7
                          WHEN (pass_touchdowns + rush_touchdowns +
                               punt_returns_touchdowns + kick_returns_touchdowns) * 7 <= 13 THEN 4
                          WHEN (pass_touchdowns + rush_touchdowns +
                               punt_returns_touchdowns + kick_returns_touchdowns) * 7 <= 20 THEN 1
                          WHEN (pass_touchdowns + rush_touchdowns +
                               punt_returns_touchdowns + kick_returns_touchdowns) * 7 <= 27 THEN 0
                          WHEN (pass_touchdowns + rush_touchdowns +
                               punt_returns_touchdowns + kick_returns_touchdowns) * 7 <= 34 THEN -1
                          WHEN (pass_touchdowns + rush_touchdowns +
                               punt_returns_touchdowns + kick_returns_touchdowns) * 7 >= 35 THEN -4
                      END;
