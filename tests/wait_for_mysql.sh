#!/usr/bin/env bash

source ./tests/util.sh

check_db_status "${MYSQL_HOST}" "${MYSQL_PORT}" mysql
