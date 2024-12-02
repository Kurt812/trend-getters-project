#!/bin/bash
source .env

PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USERNAME -d $DB_NAME -v schema_name=$SCHEMA_NAME 