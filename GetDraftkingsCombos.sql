/* QB */
CREATE TEMP TABLE dfs_qb(cfl_central_id INT, salary INT, dfs_score NUMERIC);

INSERT INTO dfs_qb(cfl_central_id, salary, dfs_score)
SELECT ppp.cfl_central_id, draftkings.salary, ppp.proj_points
FROM player_proj_points ppp JOIN draftkings
  ON (REPLACE(lower(draftkings.name), '.', '') = REPLACE(ppp.name, '.', '') AND upper(draftkings.team) = ppp.abbreviation)
                            JOIN games ON (ppp.game_id = games.game_id)
WHERE proj_points <> 0
AND   games.event_status = 'Pre-Game'
AND   draftkings.position = 'QB'
AND   draftkings.game_date = CAST(games.date_start AS DATE)
ORDER BY salary / proj_points
LIMIT 5;

/* RB */
CREATE TEMP TABLE dfs_rb(cfl_central_id INT, salary INT, dfs_score NUMERIC);

INSERT INTO dfs_rb(cfl_central_id, salary, dfs_score)
SELECT ppp.cfl_central_id, draftkings.salary, ppp.proj_points
FROM player_proj_points ppp JOIN draftkings
  ON (REPLACE(lower(draftkings.name), '.', '') = REPLACE(ppp.name, '.', '') AND upper(draftkings.team) = ppp.abbreviation)
                            JOIN games ON ppp.game_id = games.game_id
WHERE proj_points <> 0
AND   games.event_status = 'Pre-Game'
AND   draftkings.position = 'RB'
AND   draftkings.salary >= 7000
AND   draftkings.game_date = CAST(games.date_start AS DATE)
ORDER BY salary / proj_points
LIMIT 5;

INSERT INTO dfs_rb(cfl_central_id, salary, dfs_score)
SELECT ppp.cfl_central_id, draftkings.salary, ppp.proj_points
FROM player_proj_points ppp JOIN draftkings
  ON (REPLACE(lower(draftkings.name), '.', '') = REPLACE(ppp.name, '.', '') AND upper(draftkings.team) = ppp.abbreviation)
                            JOIN games ON ppp.game_id = games.game_id
WHERE proj_points <> 0
AND   games.event_status = 'Pre-Game'
AND   draftkings.position = 'RB'
AND   draftkings.salary < 7000
AND   draftkings.game_date = CAST(games.date_start AS DATE)
ORDER BY salary / proj_points
LIMIT 5;

/* WR */
CREATE TEMP TABLE dfs_wr(cfl_central_id INT, salary INT, dfs_score NUMERIC);

INSERT INTO dfs_wr(cfl_central_id, salary, dfs_score)
SELECT ppp.cfl_central_id, draftkings.salary, ppp.proj_points
FROM player_proj_points ppp JOIN draftkings
  ON (REPLACE(lower(draftkings.name), '.', '') = REPLACE(ppp.name, '.', '') AND upper(draftkings.team) = ppp.abbreviation)
                            JOIN games ON ppp.game_id = games.game_id
WHERE proj_points <> 0
AND   games.event_status = 'Pre-Game'
AND   draftkings.position = 'WR'
AND   draftkings.salary >= 7000
AND   draftkings.game_date = CAST(games.date_start AS DATE)
ORDER BY salary / proj_points
LIMIT 8;

INSERT INTO dfs_wr(cfl_central_id, salary, dfs_score)
SELECT ppp.cfl_central_id, draftkings.salary, ppp.proj_points
FROM player_proj_points ppp JOIN draftkings
  ON (REPLACE(lower(draftkings.name), '.', '') = REPLACE(ppp.name, '.', '') AND upper(draftkings.team) = ppp.abbreviation)
                            JOIN games ON ppp.game_id = games.game_id
WHERE proj_points <> 0
AND   games.event_status = 'Pre-Game'
AND   draftkings.position = 'WR'
AND   draftkings.salary >= 5000
AND   draftkings.salary < 7000
AND   draftkings.game_date = CAST(games.date_start AS DATE)
ORDER BY salary / proj_points
LIMIT 8;

INSERT INTO dfs_wr(cfl_central_id, salary, dfs_score)
SELECT ppp.cfl_central_id, draftkings.salary, ppp.proj_points
FROM player_proj_points ppp JOIN draftkings
  ON (REPLACE(lower(draftkings.name), '.', '') = REPLACE(ppp.name, '.', '') AND upper(draftkings.team) = ppp.abbreviation)
                            JOIN games ON ppp.game_id = games.game_id
WHERE proj_points <> 0
AND   games.event_status = 'Pre-Game'
AND   draftkings.position = 'WR'
AND   draftkings.salary < 5000
AND   draftkings.game_date = CAST(games.date_start AS DATE)
ORDER BY salary / proj_points
LIMIT 8;

/* UTIL */
CREATE TEMP TABLE dfs_util(cfl_central_id INT, salary INT, dfs_score NUMERIC);

INSERT INTO dfs_util(cfl_central_id, salary, dfs_score)
SELECT ppp.cfl_central_id, draftkings.salary, ppp.proj_points
FROM player_proj_points ppp JOIN draftkings
  ON (REPLACE(lower(draftkings.name), '.', '') = REPLACE(ppp.name, '.', '') AND upper(draftkings.team) = ppp.abbreviation)
                            JOIN games ON ppp.game_id = games.game_id
WHERE proj_points <> 0
AND   games.event_status = 'Pre-Game'
AND   draftkings.position IN ('RB', 'WR')
AND   draftkings.salary >= 8000
AND   draftkings.game_date = CAST(games.date_start AS DATE)
ORDER BY salary / proj_points
LIMIT 10;

INSERT INTO dfs_util(cfl_central_id, salary, dfs_score)
SELECT ppp.cfl_central_id, draftkings.salary, ppp.proj_points
FROM player_proj_points ppp JOIN draftkings
  ON (REPLACE(lower(draftkings.name), '.', '') = REPLACE(ppp.name, '.', '') AND upper(draftkings.team) = ppp.abbreviation)
                            JOIN games ON ppp.game_id = games.game_id
WHERE proj_points <> 0
AND   games.event_status = 'Pre-Game'
AND   draftkings.position IN ('RB', 'WR')
AND   draftkings.salary >= 6000
AND   draftkings.salary < 8000
AND   draftkings.game_date = CAST(games.date_start AS DATE)
ORDER BY salary / proj_points
LIMIT 10;

INSERT INTO dfs_util(cfl_central_id, salary, dfs_score)
SELECT ppp.cfl_central_id, draftkings.salary, ppp.proj_points
FROM player_proj_points ppp JOIN draftkings
  ON (REPLACE(lower(draftkings.name), '.', '') = REPLACE(ppp.name, '.', '') AND upper(draftkings.team) = ppp.abbreviation)
                            JOIN games ON ppp.game_id = games.game_id
WHERE proj_points <> 0
AND   games.event_status = 'Pre-Game'
AND   draftkings.position IN ('RB', 'WR')
AND   draftkings.salary < 6000
AND   draftkings.game_date = CAST(games.date_start AS DATE)
ORDER BY salary / proj_points
LIMIT 10;

/* DEF */
CREATE TEMP TABLE dfs_def(team_id INT, salary INT, dfs_score NUMERIC);

INSERT INTO dfs_def(team_id, salary, dfs_score)
SELECT dp.team_id, draftkings.salary, dp.proj_points
FROM defense_projections dp JOIN teams ON dp.team_id = teams.team_id
                            JOIN draftkings ON (TRIM(draftkings.name) = teams.nickname)
                            JOIN games ON dp.game_id = games.game_id
WHERE proj_points <> 0
AND   games.event_status = 'Pre-Game'
AND   draftkings.game_date = CAST(games.date_start AS DATE)
ORDER BY salary / proj_points
LIMIT 4;

CREATE TEMP TABLE combos(qb INT, rb INT, wr1 INT, wr2 INT, flex1 INT, flex2 INT, def INT);

-- Temp table to hold the combination of players
INSERT INTO combos(qb, rb, wr1, wr2, flex1, flex2, def)
SELECT  dfs_qb.cfl_central_id AS "QB", dfs_rb.cfl_central_id AS "RB", wr1.cfl_central_id AS "WR1",
    wr2.cfl_central_id AS "WR2", util1.cfl_central_id AS "Util1", util2.cfl_central_id AS "Util2",
    def.team_id AS "def"
FROM        dfs_qb  
        join dfs_rb ON 1 = 1
        join dfs_wr wr1 ON 1 = 1
        join dfs_wr wr2 ON 1 = 1
        join dfs_util util1 ON 1 = 1
        join dfs_util util2 ON 1 = 1
        JOIN dfs_def def ON 1 = 1
WHERE       dfs_qb.salary + dfs_rb.salary + wr1.salary + wr2.salary + util1.salary + util2.salary + def.salary <= 50000
AND     dfs_rb.cfl_central_id NOT IN (util1.cfl_central_id, util2.cfl_central_id)
AND     wr1.cfl_central_id NOT IN (wr2.cfl_central_id, util1.cfl_central_id, util2.cfl_central_id)
AND     wr2.cfl_central_id NOT IN (wr1.cfl_central_id, util1.cfl_central_id, util2.cfl_central_id)
AND     util1.cfl_central_id NOT IN (dfs_rb.cfl_central_id, wr1.cfl_central_id, wr2.cfl_central_id, util2.cfl_central_id)
AND     util2.cfl_central_id NOT IN (dfs_rb.cfl_central_id, wr1.cfl_central_id, wr2.cfl_central_id, util1.cfl_central_id)
-- QB and Defense must not play each other
AND     (SELECT ttd.opp_team_id FROM players p JOIN team_training_data ttd ON p.current_team_id = ttd.team_id
                          JOIN upcoming_games ug ON ttd.game_id = ug.game_id
          WHERE cfl_central_id = dfs_qb.cfl_central_id) <> def.team_id
ORDER BY    dfs_qb.dfs_score + dfs_rb.dfs_score + wr1.dfs_score + wr2.dfs_score +
            util1.dfs_score + util2.dfs_score + def.dfs_score DESC
LIMIT 1;

-- Delete existing combos for the upcoming week
DELETE FROM draftkings_combos
WHERE   season_year = (SELECT MIN(season) FROM games WHERE event_status = 'Pre-Game')
AND     week = (SELECT MIN(week) FROM games WHERE event_status = 'Pre-Game');

INSERT INTO draftkings_combos(position, cfl_central_id)
SELECT 'QB', qb
FROM  combos;

INSERT INTO draftkings_combos(position, cfl_central_id)
SELECT 'RB', rb
FROM  combos;

INSERT INTO draftkings_combos(position, cfl_central_id)
SELECT 'WR1', wr1
FROM  combos;

INSERT INTO draftkings_combos(position, cfl_central_id)
SELECT 'WR2', wr2
FROM  combos;

INSERT INTO draftkings_combos(position, cfl_central_id)
SELECT 'Flex1', flex1
FROM  combos;

INSERT INTO draftkings_combos(position, cfl_central_id)
SELECT 'Flex2', flex2
FROM  combos;

INSERT INTO draftkings_combos(position, cfl_central_id)
SELECT 'DST', def
FROM  combos;

DROP TABLE dfs_qb;
DROP TABLE dfs_rb;
DROP TABLE dfs_wr;
DROP TABLE dfs_util;
DROP TABLE dfs_def;
DROP TABLE combos;

-- Set season/week
UPDATE  draftkings_combos
SET     season_year = (SELECT MIN(season) FROM games WHERE event_status = 'Pre-Game'),
        week = (SELECT MIN(week) FROM games WHERE event_status = 'Pre-Game')
WHERE   season_year IS NULL;

SELECT position, first_name, last_name
FROM players JOIN draftkings_combos ON players.cfl_central_id = draftkings_combos.cfl_central_id
WHERE   season_year = (SELECT MIN(season) FROM games WHERE event_status = 'Pre-Game')
AND     week = (SELECT MIN(week) FROM games WHERE event_status = 'Pre-Game');

--SELECT * FROM draftkings_combos WHERE week = 15