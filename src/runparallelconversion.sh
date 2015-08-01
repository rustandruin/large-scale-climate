# Call with runparallelconversion.sh AWS_KEY AWS_SECRET_KEY from the master node as user ubuntu

# if you change these numbers, note that MASTERPROCESSES + numslaves * SLAVEPROCESSES must be prime
# this setting works for 3 machines (2 slaves 1 master)
SLAVEPROCESSES=2
MASTERPROCESSES=3
NUMSLAVES=$((`wc -l /home/mpich2.hosts | cut -f 1 -d " "` - 1))
NUMPROCESSES=$(($NUMSLAVES * $SLAVEPROCESSES + $MASTERPROCESSES))
JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64

CURDIR=`dirname $0`
LOGDIR=$CURDIR/..
CONVERSIONSCRIPT=convertGRIBToCSV.py
MYREMAINDER=0
cd $CURDIR
while [ $MYREMAINDER -lt $MASTERPROCESSES ]; do
  echo "Launching conversion process $MYREMAINDER on the master"
  python $CONVERSIONSCRIPT $AWS_KEY $AWS_SECRET_KEY $NUMPROCESSES $MYREMAINDER &
  let MYREMAINDER+=1
done

if [ $SLAVEPROCESSES -gt 0 ]; then
  for HOST in `tail -n +2 /home/mpich2.hosts`; do
    for ITER in `seq 1 $SLAVEPROCESSES`; do
      echo "Launching conversion process $MYREMAINDER on $HOST"
      ssh ubuntu@$HOST -c "cd $CURDIR;JAVA_HOME=$JAVA_HOME python $CONVERSIONSCRIPT $AWS_KEY $AWS_SECRET_KEY $NUMPROCESSES $MYREMAINDER" &
      let MYREMAINDER+=1
    done
  done
fi

# concatenate error_logs
cat grib_conversion_error_log* > $LOGDIR/conversion_error_log 
rm grib_conversion_error_log*

