#!/bin/bash

java -cp . PrestoMonitor "$1" | sort -nr | head -1 &
java -jar ~/presto/presto-cli-0.226-executable.jar --catalog=hive --schema=tpch_20 -f presto_query/"$2".sql
