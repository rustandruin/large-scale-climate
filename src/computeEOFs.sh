# Computes the 3D EOFs using CSFR-O dataset

DIR="$(cd "`dirname "$0"`"/..; pwd)"
LOGDIR="$DIR/eventlogs"
DATADIR="$DIR/data"
JARNAME=$1

INSOURCE=hdfs:///CSFROcsv/vals
FORMAT=csv
NUMROWS= 46728
NUMCOLS= 10627200

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
  --class org.apache.spark.mllib.linalg.distributed.SVDVariants \
  $JARNAME \
  $FORMAT $INSOURCE $NUMROWS $NUMCOLS $PREPROCESS $NUMEOFS $OUTDEST \
  2>&1 | tee $LOGNAME
