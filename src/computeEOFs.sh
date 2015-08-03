# Computes the 3D EOFs using CSFR-O dataset

DIR="$(cd "`dirname "$0"`"/..; pwd)"
LOGDIR="$DIR/eventlogs"
DATADIR="$DIR/data"
JARNAME=$1

INSOURCE=hdfs:///CSFR_O_parquet
NUMROWS= 46728
NUMCOLS= 10627200

IMPUTATION="dineof"
PREPROCESS="centerOverAllObservations"

NUMEOFS=32
SLACK=0
NITERS=4

NPARTS=""

JOBNAME="eofs-$IMPUTATION-$PREPROCESS-$NUMEOFS-$SLACK-$NITERS-$NPARTS"
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
  $INSOURCE $NUMROWS $NUMCOLS $IMPUTATION $PREPROCESS $OUTDEST $NUMEOFS $SLACK $NITERS $NPARTS \
  2>&1 | tee $LOGNAME
