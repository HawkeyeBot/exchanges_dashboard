# Exchanges dashboard

This repository is meant to provide an easy-to-run (local) web-UI that provides insight into your account(s) activities on exchanges. It uses a custom script to retrieve data from one or more exchanges (via websocket and/or REST), which is then inserted into a database (sqlite by default). A metabase container is then launched with a default configuration to display this information.

# Metabase configuration:

## Credentials
* First name: First
* Last name: Last
* Email: exchanges@dashboard.com
* Password: ExchangesDashboard1!
* Department: Department of Awesome

## Database
* Database type: SQLite
* Name: Exchanges
* Filename: /data/exchanges_db.sqlite

# Starting the dashboard
* Clone this repo:
  * `git clone https://github.com/hoeckxer/exchanges_dashboard.git`
* Copy the config.example.json to config.json
  * `cp config.example.json config.json`
* Enter the api-key & secret
  * `nano config.json`
* Start the dashboard
  * Run `docker-compose up -d`
* Go to http://localhost:3000

| WARNING: If you change the config.json file, make sure you rebuild the container using `docker-compose up -d --build` |
| --- |

# Stopping the dashboard
* Run `docker-compose stop`
