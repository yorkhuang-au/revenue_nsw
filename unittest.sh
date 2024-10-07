#!/bin/bash
cd app
docker build --no-cache-filter 'test'  -t revenuensw_etl_test --target=test . \
&&  docker run --rm revenuensw_etl_test \
&& docker rmi revenuensw_etl_test
