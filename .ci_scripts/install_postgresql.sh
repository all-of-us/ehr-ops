#!/usr/bin/env bash
#
# Install and configure PostgreSQL 13
#
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update
sudo apt-get -y install lsb-release, wget
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
RELEASE=$(lsb_release -cs)
echo "deb http://apt.postgresql.org/pub/repos/apt/ ${RELEASE}"-pgdg main | sudo tee  /etc/apt/sources.list.d/pgdg.list
sudo apt update
sudo apt -y install postgresql-13

sudo sed -i -E "s/(127.0.0.1\/32)(.*?)(md5)/\1\2trust/g" /etc/postgresql/13/main/pg_hba.conf
sudo cat /etc/postgresql/13/main/pg_hba.conf

sudo mkdir -p /tmp/postgres
sudo pg_ctlcluster 13 main start
sleep 1
sudo ps -ef | grep postgres

sudo su - postgres -c 'psql -c "CREATE USER circleci"'
sudo su - postgres -c 'psql -c "ALTER USER circleci WITH SUPERUSER"'

exit 0