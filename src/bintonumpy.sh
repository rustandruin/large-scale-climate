#!/usr/bin/env bash
# Converts the binary dump of the EOFs and the data on row and column indices into a nice numpy dataset

CURDIR=`dirname $(realpath $0)`
WORKINGDIR=$CURDIR/..
JARFILE=$1
SPARKHOME=/root/spark

INBIN=$WORKINGDIR/data/eofs-centerOverAllObservations-20.bin
OUTCSV=$WORKINGDIR/data
OUTNUMPY=$OUTCSV
COLINDICES_HDFS=/user/root/CFSROparquet/origcolindices # the mapping from col indices in the eofs to the hourly observation periods as columns
ROWINDICES_HDFS=/user/root/CFSROcsv/recordDateMapping # the mapping from columns corresponding to hourly observation periods to actual hourly observation periods 

$SPARKHOME/bin/spark-submit --master local --verbose \
   --driver-memory 2G \
   --class org.apache.spark.mllib.linalg.distributed.ConvertDump \
   $JARFILE $INBIN $OUTCSV

python csvtonumpy.py $OUTCSV/colEOFs $OUTCSV/rowEOFs $COLINDICES_HDFS $ROWINDICES_HDFS $OUTNUMPY

