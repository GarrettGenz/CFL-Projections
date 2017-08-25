UPDATE  cfldb.public.offensive_player_stats
SET     cfldb.public.offensive_player_stats.fantasy_points = NULL;

UPDATE  offensive_player_stats
SET     fantasy_points = (4 * COALESCE(pass_touchdowns, 0)) + (0.04 * COALESCE(pass_net_yards, 0)) +
                        (-1 * COALESCE(pass_interceptions, 0)) + (1 * COALESCE(receive_caught, 0)) +
                         (0.1 * COALESCE(rush_net_yards, 0)) +
                        (6 * COALESCE(rush_touchdowns, 0)) + (0.1 * COALESCE(receive_yards, 0)) +
                      (6 * COALESCE(receive_touchdowns, 0)) + (6 * COALESCE(punt_returns_touchdowns, 0)) +
                      (6 * COALESCE(kick_returns_touchdowns, 0)) +
                      (0.05 * COALESCE(punt_returns_yards, 0)) + (0.05 * COALESCE(kick_returns_yards, 0));

UPDATE  offensive_player_stats
SET     fantasy_points = fantasy_points + 3
WHERE   pass_net_yards >= 300;

UPDATE  offensive_player_stats
SET     fantasy_points = fantasy_points + 3
WHERE   rush_net_yards >= 100;

UPDATE  offensive_player_stats
SET     fantasy_points = fantasy_points + 3
WHERE   receive_yards >= 100;
