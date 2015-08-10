#!/usr/bin/env bash
CURDIR=`dirname $(realpath $0)`
LOGDIR=$CURDIR/../eventLogs
JARNAME=$1
INSOURCE=hdfs://ip-172-31-3-93.ec2.internal:9000/user/root/CFSROcsv/vals
OUTDEST=hdfs://ip-172-31-3-93.ec2.internal:9000/user/root/CFSROparquet
LOGNAME=$CURDIR/../CSVToParquetConversion.log

# add back --master yarn \ if using yarn
spark-submit --verbose \
	--driver-memory 220G \
	--executor-memory 220G \
	--conf spark.eventLog.enable=true \
	--conf spark.eventLog.dir=$LOGDIR \
  --conf spark.driver.maxResultSize=50G \
  --conf spark.task.maxFailures=4 \
  --conf spark.worker.timeout=1200000 \
  --conf spark.network.timeout=1200000 \
	--jars $JARNAME \
	--class org.apache.spark.climate.CSVToParquet \
	$JARNAME \
	$INSOURCE $OUTDEST \
	2>&1 | tee $LOGNAME
