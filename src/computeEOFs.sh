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
NUMEOFS=10

JOBNAME="eofs-$PREPROCESS-$NUMEOFS"
OUTDEST="$DATADIR/$JOBNAME.bin"
LOGNAME="$JOBNAME.log"

[ -e $OUTDEST ] && (echo "Job already run successfully, stopping"; exit 1)

# On EC2 there are 32 cores/node and 244GB/node 
# use 30 executors b/c that's what did for CX (apparently, but I wonder if it helps to increase executors)
# use as much memory as available so can cache the entire 2GB RDD
#--num-executors 30
#--driver-memory 210G
#--executor-memory 210G

# On Cori there are 32 cores/node and 128GB/node
# use 30 executors b/c that's what did for CX (apparently, but I wonder if it helps to increase executors)
# can only cache about .71% of the 2GB RDD
#--num-executors 30
#--driver-memory 120G
#--executor-memory 120G

spark-submit --verbose \
  --master "spark://ec2-54-200-88-120.us-west-2.compute.amazonaws.com:7077" \
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
