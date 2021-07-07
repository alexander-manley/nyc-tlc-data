#!/usr/bin/env bash

# https://hub.docker.com/_/postgres/#initialization-scripts

set -e

create_database=$(cat <<EOF
CREATE DATABASE nyc_tlc;
GRANT ALL PRIVILEGES ON DATABASE nyc_tlc TO docker;
SET TIMEZONE='Europe/Amsterdam';
EOF
)

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  ${create_database}
EOSQL
