#!/bin/bash
cd app

# start mongo db
docker compose create --no-recreate --remove-orphans mongo
docker compose start mongo

# Wait for service up
sleep 5

# run mongosh
docker compose exec -it mongo mongosh "mongodb://mongo:27017" --username root --password=example --authenticationDatabase admin
