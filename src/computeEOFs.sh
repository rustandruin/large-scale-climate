#!/usr/bin/env bash
# Computes the 3D EOFs using CSFR-O dataset

DIR="$(cd "`dirname "$0"`"/..; pwd)"
LOGDIR="$DIR/eventLogs"
DATADIR="$DIR/data"
JARNAME=$1

INSOURCE=hdfs://master:9000/user/ubuntu/CSFROcsv/vals
FORMAT=csv
NUMROWS=104
NUMCOLS=6349676

PREPROCESS="centerOverAllObservations"
NUMEOFS=32

JOBNAME="eofs-$PREPROCESS-$NUMEOFS"
OUTDEST="$DATADIR/$JOBNAME.bin"
LOGNAME="$JOBNAME.log"

[ -e $OUTDEST ] && (echo "Job already run successfully, stopping"; exit 1)

spark-submit --verbose \
  --driver-memory 5G \
  --executor-memory 5G \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --conf spark.driver.maxResultSize=4G \
  --jars $JARNAME \
  --class org.apache.spark.mllib.climate.computeEOFs \
  $JARNAME \
  $FORMAT $INSOURCE $NUMROWS $NUMCOLS $PREPROCESS $NUMEOFS $OUTDEST \
  2>&1 | tee $LOGNAME
