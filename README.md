# CFL-Projections
Calculate CFL projections and determine an optimal DraftKings lineup from it.

## About
This project utilizes the cfl.ca API and stores relevant data in a PostgreSQL database.
A mix of Python and SQL are used to generate player projections for future games.
These projections are compared to DraftKings salaries to determine an optimal lineup.


## Key files
GetGames - Grabs all games for the years specified <br />
GetPlayers - Grabs all players (current and past) <br />
GetTeams - Grabs all teams <br />
GetPlayerStats - Grabs player boxscore stats for each completed game <br />
UpdateGames - Determines games that are recently completed and updates them in the database <br />

### TODO
Calculate average defensive stats per team <br />
Calculate average defensive stats leaguewide <br />
Use the above to numbers to create a coefficient for each offensive category <br />
Use the coefficient to adjust offensive stats for each player <br />
<br />
Create table to enter Draftkings salaries <br />
Create process for entering Draftkings salaries into table <br />
<br />
Create process for entering starters for future games
