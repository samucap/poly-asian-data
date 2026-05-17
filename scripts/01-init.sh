#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
-- Your SQL goes here. You can now use variables with ${VAR}

CREATE USER appuser WITH PASSWORD '${POSTGRES_PASSWORD}';

EOSQL
