#!/bin/bash
cd app

# start mongo db
docker compose create --no-recreate --remove-orphans mongo
docker compose start mongo

# run etl on file
docker compose run --rm etl $*