LOCATION=$1
SUBDIRS=`find $LOCATION -maxdepth 1 -mindepth 1 -type d -printf "%f\n"`
SEARCH=$2

for dir in $SUBDIRS 
do
  grep $SEARCH $LOCATION/$dir/*
done
