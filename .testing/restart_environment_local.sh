#!/bin/bash
bash ./cleanup_environment.sh \
&& mkdir "temp" \
&& docker compose --file="./docker-compose.yml" --env-file="./.env" up --build


