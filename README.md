Before running ensure that there are directories named "data" and "eventLogs" in the base repo directory

Next edit these files:
- src/computeEOFs.sh : here you should define the PLATFORM variable ("EC2", "CORI") and call src/runOneJob.sh several times to 
  run experiments varying the type of PCA ("exact", "randomized") and the number of EOFs 
- src/runOneJob.sh : here you should define the inputs that vary according to the platform (location of the source data, the spark master URL, the executor memory and core settings, etc)

Now you can run experiments (assuming you have salloc-ed and started Spark as needed on the HPC platforms) with 
"sbt submit"

The console logs are saved to the base repo directory, and the eventLogs are stored to the eventLogs subdirectory
