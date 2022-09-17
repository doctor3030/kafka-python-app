#!/bin/bash
docker compose --file="./docker-compose.yml" --env-file="./.env" down \
&& docker container prune -f \
&& docker image prune -f \
&& if [ -d "temp" ]; then rm -Rf "temp"; fi