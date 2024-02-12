#!/usr/bin/bash

DATE_CHK=`date`
#PROCESS_LIST=`ps -ef|grep DailyMain|grep python3`
PROCESS_LIST=`ps -ef|grep DailyMain|grep python3| awk '{print $2}'`
echo "==process list check! start=="
echo $DATE_CHK
echo $PROCESS_LIST
echo "===process list check! end==="
echo "============================="
for PROC in ${PROCESS_LIST[@]}; do
    echo "Process ${PROC} will KILLED!"
    kill -9 ${PROC}
done