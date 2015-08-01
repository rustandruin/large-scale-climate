# Call with runparallelconversion.sh AWS_KEY AWS_SECRET_KEY from the master node as user ubuntu

# if you change these numbers, note that MASTERPROCESSES + numslaves * SLAVEPROCESSES must be prime
# this setting works for 3 machines (2 slaves 1 master)
SLAVEPROCESSES=2
MASTERPROCESSES=3
NUMSLAVES=$((`wc -l /home/mpich2.hosts | cut -f 1 -d " "` - 1))
NUMPROCESSES=$(($NUMSLAVES * $SLAVEPROCESSES + $MASTERPROCESSES))

CURDIR=`dirname $0`
WORKDIR=$CURDIR
CONVERSIONSCRIPT="$CURDIR/src/convertGRIBToCSV.py"
MYREMAINDER=0

while [ $MYREMAINDER -lt $MASTERPROCESSES ]; do
  echo "Launching conversion process $MYREMAINDER on the master"
  (cd $WORKDIR; python $CONVERSIONSCRIPT $AWS_KEY $AWS_SECRET_KEY $NUMPROCESSES $MYREMAINDER) &
  let MYREMAINDER+=1
done

for HOST in `tail -n +2 /home/mpich2.hosts`; do
  for ITER in `seq 1 $SLAVEPROCESSES`; do
    echo "Launching conversion process $MYREMAINDER on $HOST"
    ssh ubuntu@$HOST -c "cd $WORKDIR; python $CONVERSIONSCRIPT $AWS_KEY $AWS_SECRET_KEY $NUMPROCESSES $MYREMAINDER" &
    let MYREMAINDER+=1
  done
done

# concatenate error_logs
(cd $WORKDIR; cat grib_conversion_error_log* > conversion_error_log; rm grib_conversion_error_log*)
