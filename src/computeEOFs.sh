#!/usr/bin/env bash
# Computes the 3D EOFs using CSFR dataset

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

PREPROCESS="centerOverAllObservations"
#PREPROCESS="cosLat+centerOverAllObservations"
#PREPROCESS="standardizeEach"
#PREPROCESS="standardizeLevels"
#PREPROCESS="cosLat+standardizeLevels"
NUMEOFS=20

JOBNAME="eofs-$PREPROCESS-$NUMEOFS"
OUTDEST="$DATADIR/$JOBNAME.bin"
LOGNAME="$JOBNAME.log"

[ -e $OUTDEST ] && (echo "Job already run successfully, stopping"; exit 1)

# Parameters below relevant for Spark 1.4.0:
# use 220G in standalone mode, 210G for yarn
# see https://support.pivotal.io/hc/en-us/articles/201462036-Mapreduce-YARN-Memory-Parameters 
# for guidance on setting yarn memory parameters; note that Spark 1.4.0 only sets yarn's max-memory
# setting (to >240Gb), and everything else uses the default for both yarn and MapReduce. This seems
# to allow the EOF code to run as long as you don't ask for too much memory (hence the 210G recommendation
# in the yarn case)

spark-submit --verbose \
  --master yarn \
  --num-executors 30 \
  --driver-memory 210G \
  --executor-memory 210G \
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
