#!/usr/bin/env bash
CURDIR=`dirname $(realpath $0)`
LOGDIR=$CURDIR/../eventLogs
JARNAME=$1
INSOURCE=hdfs://ip-172-31-53-33.ec2.internal:9000/user/root/CSFROcsvfull/vals
MASKSOURCE=hdfs://ip-172-31-53-33.ec2.internal:9000/user/root/CSFROcsvfull/mask.gz
OUTDEST=hdfs://ip-172-31-53-33.ec2.internal:9000/user/root/CSFROParquet/vals
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
	$INSOURCE $MASKSOURCE $OUTDEST \
	2>&1 | tee $LOGNAME
