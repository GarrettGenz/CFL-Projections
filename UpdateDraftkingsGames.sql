UPDATE  draftkings
SET     game_date = games.date_start
FROM    games JOIN player_proj_points ppp ON (games.game_id = ppp.game_id)
              JOIN upcoming_games ON games.game_id = upcoming_games.game_id
WHERE   REPLACE(lower(draftkings.name), '.', '') = REPLACE(ppp.name, '.', '') AND upper(draftkings.team) = ppp.abbreviation
AND     game_date IS NULL;

UPDATE  draftkings
SET     game_date = games.date_start
FROM  defense_projections dp JOIN teams ON dp.team_id = teams.team_id
                             JOIN games ON dp.game_id = games.game_id
                             JOIN upcoming_games ug ON dp.game_id = ug.game_id
WHERE   TRIM(draftkings.name) = teams.nickname
AND     game_date IS NULL;

DELETE FROM draftkings WHERE game_date IS NULL;