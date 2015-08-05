#!/usr/bin/env bash
# Computes the 3D EOFs using CSFR-O dataset

DIR="$(cd "`dirname "$0"`"/..; pwd)"
LOGDIR="$DIR/eventLogs"
DATADIR="$DIR/data"
JARNAME=$1

# for the small dataset
NUMROWS=104
NUMCOLS=6349676
FORMAT=csv
INSOURCE=hdfs://master:9000/user/ubuntu/smallclimatevals
MASKSOURCE='notmasked'
MASKSOURCE=hdfs://master:9000/user/ubuntu/CSFROcsv/mask/part-00000.gz
# for the large dataset
# NUMROWS=46718
# NUMCOLS=6349676
# FORMAT=csv
# INSOURCE=hdfs://master:9000/user/ubuntu/CSFROcsv/vals
# MASKSOURCE=hdfs://master:9000/user/ubuntu/CSFROcsv/mask/part-00000.gz

PREPROCESS="centerOverAllObservations"
NUMEOFS=15

JOBNAME="eofs-$PREPROCESS-$NUMEOFS"
OUTDEST="$DATADIR/$JOBNAME.bin"
LOGNAME="$JOBNAME.log"

[ -e $OUTDEST ] && (echo "Job already run successfully, stopping"; exit 1)

spark-submit --verbose \
  --driver-memory 60G \
  --executor-memory 60G \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --conf spark.driver.maxResultSize=20G \
  --jars $JARNAME \
  --class org.apache.spark.mllib.climate.computeEOFs \
  $JARNAME \
  $FORMAT $INSOURCE $MASKSOURCE $NUMROWS $NUMCOLS $PREPROCESS $NUMEOFS $OUTDEST \
  2>&1 | tee $LOGNAME
