#!/usr/bin/env bash
# Converts the binary dump of the EOFs and the data on row and column indices into a nice numpy dataset

CURDIR=`dirname $(realpath $0)`
WORKINGDIR=$CURDIR/..
JARFILE=$1
SPARKHOME=/opt/Spark
SPARKHOME=/root/spark
SPARKHOME=/root/spark
SPARKHOME=/root/spark
SPARKHOME=/root/spark
SPARKHOME=/root/spark
SPARKHOME=/root/spark

INBIN=$WORKINGDIR/data/eofs-centerOverAllObservations-20.bin
OUTCSV=$WORKINGDIR/data
OUTNUMPY=$OUTCSV
COLINDICES_HDFS=/user/ubuntu/CFSROparquet/origcolindices # the mapping from col indices in the eofs to the hourly observation periods as columns
ROWINDICES_HDFS=/user/ubuntu/CFSROcsv/recordDateMapping # the mapping from columns corresponding to hourly observation periods to actual hourly observation periods 

# spark-submit is a BAD idea, try to just get the right classpath and use scala?
$SPARKHOME/bin/spark-submit --master local --verbose \
   --driver-memory 2G \
   --class org.apache.spark.mllib.linalg.distributed.ConvertDump \
   $JARFILE $INBIN $OUTCSV
