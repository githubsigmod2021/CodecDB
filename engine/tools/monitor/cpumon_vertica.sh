#!/bin/bash

perf stat -p 293247 -x ' ' -o perf.tmp &
PID=$!
BEFORE=`awk '{print $14, $15}' /proc/293247/stat`
vsql -w vdb -f vertica_query/"$1".sql > sql.tmp
AFTER=`awk '{print $14, $15}' /proc/293247/stat`
VAR=`kill -INT $PID`

read -a before <<< "$BEFORE"
read -a after <<< "$AFTER"
usrbefore=${before[0]}
usrafter=${after[0]}
let usr=$((after[0] - before[0]))
let sys=$((after[1] - before[1]))

wallclock=`tail -3 sql.tmp | head -1`

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