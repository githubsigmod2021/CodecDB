#!/bin/bash

PRESTO_PID=67645
perf stat -p $PRESTO_PID -x ' ' -o perf.tmp &
PID=$!
START=`date +%s%N`
BEFORE=`awk '{print $14, $15}' /proc/${PRESTO_PID}/stat`
java -jar presto/presto-cli-0.226-executable.jar --catalog=hive --schema=tpch_20 -f ~/presto_query/$1.sql > sql.tmp
AFTER=`awk '{print $14, $15}' /proc/${PRESTO_PID}/stat`
STOP=`date +%s%N`
VAR=`kill -INT $PID`

read -a before <<< "$BEFORE"
read -a after <<< "$AFTER"
usrbefore=${before[0]}
usrafter=${after[0]}
let usr=$((after[0] - before[0]))
let sys=$((after[1] - before[1]))

let wallclock=$(((STOP - START) / 1000000))

ready=`wc -l perf.tmp | awk '{print $1}'`
until [ $ready -gt 0 ]
do
     sleep 0.1
     ready=`wc -l perf.tmp | awk '{print $1}'`
done

numcpu=`head -3 perf.tmp | tail -1 | awk '{print $5}'`
echo $wallclock 
echo $sys 
echo $usr 
echo $numcpu