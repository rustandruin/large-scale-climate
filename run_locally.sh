#!/bin/bash -l

./local-submit.sh ./../target/scala-2.10/large-scale-climate-assembly-0.0.1.jar ~/projects/spark-test/h5spark/file_list.txt 150 192 centerOverAllObservations 20 . exact
