#!/usr/bin/env bash
# Computes the 3D EOFs using CSFR dataset
# You need to change the memory setting and location of the data for different platforms

DIR="$(cd "`dirname "$0"`"/..; pwd)"
LOGDIR=$DIR/eventLogs
JARNAME=$1
PLATFORM=CORI
INPUTSPEC=$DIR/fullinput.spec

JOBNAME=$INPUTSPEC
LOGNAME="$JOBNAME.log"

#[ -e $OUTDEST ] && (echo "Job already run successfully, stopping"; exit 1)

if [ $PLATFORM == "CORI" ]; then 
  # On Cori there are 32 cores/node and 128GB/node
NUMEXECUTORS=30
NUMCORES=28
DRIVERMEMORY=100G
EXECUTORMEMORY=100G
MASTER=$SPARKURL
fi

spark-submit --verbose \
  --master $MASTER \
  --num-executors $NUMEXECUTORS \
  --executor-cores $NUMCORES \
  --driver-memory $DRIVERMEMORY \
  --executor-memory $EXECUTORMEMORY \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --conf spark.driver.maxResultSize=30G \
  --conf spark.task.maxFailures=4 \
  --jars $JARNAME \
  --class org.apache.spark.mllib.climate.computeEOFs \
  $JARNAME $INPUTSPEC 2>&1 | tee $LOGNAME

