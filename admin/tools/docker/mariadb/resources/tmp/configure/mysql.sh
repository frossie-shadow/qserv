#!/usr/bin/env bash

set -e

MYSQLD_DATA_DIR="/qserv/data/mysql"
MYSQLD_SOCKET="$MYSQLD_DATA_DIR/mysql.sock"
# TODO: Set password using k8s
MYSQLD_PASSWORD_ROOT="CHANGEME"
USER="mysql"
SQL_DIR="/tmp/configure/sql"
SQL_FILES="qserv-worker.sql"

# TODO remove me and check if empty before configuring
echo "-- Remove previous data."
rm -rf ${MYSQLD_DATA_DIR}/*

echo "-- "
echo "-- Installing mysql database files."
mysql_install_db --basedir="${MYSQL_DIR}" --user=${USER} >/dev/null ||
{
    echo "ERROR : mysql_install_db failed, exiting"
    exit 1
}

echo "-- "
echo "-- Start mariadb server."
mysqld &
sleep 5

echo "-- "
echo "-- Change mariadb root password."
mysqladmin -u root password "$MYSQLD_PASSWORD_ROOT"

echo "-- "
echo "-- Initializing Qserv Worker database"
for file_name in ${SQL_FILES}; do
    echo "-- Loading ${file_name} in MySQL"
    if mysql -vvv --user="root" --password="${MYSQLD_PASSWORD_ROOT}" \
        < "${SQL_DIR}/${file_name}"
    then
        echo "-- -> success"
    else
        echo "-- -> error"
        exit 1
    fi
done

echo "-- "
echo "-- Deploy scisql plugin"
# TODO fixme
echo "$MYSQLD_PASSWORD_ROOT" | scisql-deploy.py --mysql-plugin-dir=/usr/lib/mysql/plugin/ \
    --mysql-bin=/usr/bin/mysql --mysql-socket="$MYSQLD_SOCKET" \
    || echo "SCISQL FIX VERSION COMPATIBILITY CHECK"

echo "-- Stop mariadb server."
mysqladmin -u root --password="$MYSQLD_PASSWORD_ROOT" shutdown

# TODO check if restart is required
echo "-- Start mariadb server."
mysqld || echo "ERROR: Fail to start MariaDB"

