#!/bin/bash

# Backup Database
cd `dirname $0`
cd ../sql
SQL_DUMP=backup_`date +%Y%m%d`.sql
mysqldump -u encsel -pencsel encsel > ${SQL_DUMP}

tar cvzf ${SQL_DUMP}.tar.gz ${SQL_DUMP}
rm ${SQL_DUMP}
# Delete files older than 7 days
find . -name 'backup_*.sql.tar.gz' -mtime +7 -delete

# Upload to GIT
git add -A .
git commit -m "Daily DB Backup"
git pull
git push origin master

