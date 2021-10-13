# Exchanges dashboard

This repository is meant to provide an easy-to-run (local) web-UI that provides insight into your account(s) activities on exchanges. It uses a custom script to retrieve data from one or more exchanges (via websocket and/or REST), which is then inserted into a database (sqlite by default). A metabase container is then launched with a default configuration to display this information.
