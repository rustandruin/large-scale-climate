#!/usr/bin/env bash
CURDIR=`dirname $(realpath $0)`
LOGDIR=$CURDIR/../eventLogs
JARNAME=$1
INSOURCE=$2
OUTDEST=$3
LOGNAME=$CURDIR/../CSVToParquetConversion.log

spark-submit --verbose \
	--master yarn \
	--driver-memory 40G \
	--executor-memory 40G \
	--conf spark.eventLog.enable=true \
	--conf spark.eventLog.dir=$LOGDIR \
	--jars $JARNAME \
	--class org.apache.spark.climate.CSVToParquet \
	$JARNAME \
	$INSOURCE $OUTDEST \
	2>&1 | tee $LOGNAME
