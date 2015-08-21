#!/usr/bin/env bash
# Computes the 3D EOFs using CSFR-O dataset

DIR="$(cd "`dirname "$0"`"/..; pwd)"
LOGDIR="$DIR/eventLogs"
DATADIR="$DIR/data"
JARNAME=$1

# for the small dataset
#NUMROWS=104
#NUMCOLS=6349676
#FORMAT=csv
#INSOURCE=hdfs://master:9000/user/ubuntu/smallclimatevals
#MASKSOURCE='notmasked'
#MASKSOURCE=hdfs://master:9000/user/ubuntu/CSFROcsv/mask/part-00000.gz
# for the large dataset
NUMROWS=46715
NUMCOLS=6349676
#INSOURCE=hdfs://`hostname`:9000/user/ubuntu/CFSROparquet
INSOURCE=hdfs://`hostname`:9000/user/root/CFSROparquet

PREPROCESS="standardize"
NUMEOFS=20

JOBNAME="eofs-$PREPROCESS-$NUMEOFS"
OUTDEST="$DATADIR/$JOBNAME.bin"
LOGNAME="$JOBNAME.log"

[ -e $OUTDEST ] && (echo "Job already run successfully, stopping"; exit 1)

# Add back --master yarn \ when running yarn
  # --num-executors 29 \
  # took memory down from 180G on starcluster/yarn
# added multiple executors vs 29 on starcluster/yarn
spark-submit --verbose \
  --num-executors 30 \
  --driver-memory 220G \
  --executor-memory 220G \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --conf spark.driver.maxResultSize=30G \
  --conf spark.task.maxFailures=4 \
  --conf spark.worker.timeout=1200000 \
  --conf spark.network.timeout=1200000 \
  --jars $JARNAME \
  --class org.apache.spark.mllib.climate.computeEOFs \
  $JARNAME \
  $INSOURCE $NUMROWS $NUMCOLS $PREPROCESS $NUMEOFS $OUTDEST \
  2>&1 | tee $LOGNAME
