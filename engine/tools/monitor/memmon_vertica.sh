#!/bin/bash

vsql -w vdb -f vertica_query/"$1".sql > sql.tmp
memusg=`vsql -w vdb -f vertica_memusg.sql | head -3 | tail -1 | xargs`
