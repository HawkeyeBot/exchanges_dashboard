# Exchanges dashboard

This repository is meant to provide an easy-to-run (local) web-UI that provides insight into your account(s) activities on exchanges. It uses a custom script to retrieve data from one or more exchanges (via websocket and/or REST), which is then inserted into a database (sqlite by default). A metabase container is then launched with a default configuration to display this information.

# Screenshots

Overview of the dashboard:
![Futures](images/futures.png?raw=true "Futures")

When a value is entered in the DCA field (top left), the DCA quantity and the DCA price (x% above the current price) will be displayed in the additional fields in the table.
![DCA](images/dca.png?raw=true "DCA")

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

| INFO: If you get a message about `$PWD` being unknown, replace `$PWD` the docker-compose.yaml file with the full path |
| --- |

# Stopping the dashboard
* Run `docker-compose stop`

# Multiple dashboard
* If you want to deploy a second dashboard instance on the same machine, please clone the repo and update the `config.json` and `docker-compose.yaml` files.
* In the `docker-compose.yaml` you'll have to modify container names, exposed port and network interface. An example is provided in `docker-compose.yaml.example-second-dashboard` 

# Running on Raspberry Pi
* The metabase image is built for x86.
Therefore, on Raspberry Pi, a new image needs to be created using an ARM based image.
* I created a `Dockerfile` that creates this kind of image, which supposed to be identical to the `metabase` one.
* To use the new image, replace line 13 of `docker-compose.yml` from `image: metabase/metabase:v0.43.1` to `build: metabase_rpi`