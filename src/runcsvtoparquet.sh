#!/usr/bin/env bash
CURDIR=`dirname $(realpath $0)`
LOGDIR=$CURDIR/../eventLogs
JARNAME=$1
INSOURCE=hdfs://master:9000/user/ubuntu/CFSROcsv/vals
OUTDEST=hdfs://master:9000/user/ubuntu/CFSROparquet
LOGNAME=$CURDIR/../CSVToParquetConversion.log

# add back --master yarn \ if using yarn
spark-submit --verbose \
	--driver-memory 220G \
	--executor-memory 220G \
	--conf spark.eventLog.enable=true \
	--conf spark.eventLog.dir=$LOGDIR \
	--jars $JARNAME \
	--class org.apache.spark.climate.CSVToParquet \
	$JARNAME \
	$INSOURCE $OUTDEST \
	2>&1 | tee $LOGNAME
