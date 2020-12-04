#!/bin/bash

runDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $runDir

spark-submit --jars "config-1.2.0.jar" --class mck.qb.columbia.driver.Driver --master yarn --driver-memory 6g --executor-cores 4 --executor-memory 10g --conf spark.dynamicAllocation.enabled=false --conf "spark.driver.maxResultSize=4g" --driver-java-options="-Denv=dev" --num-executors 20 $runDir/columbia-1.0.0.jar $@

echo $@
job=$?;
echo $job

exit $job
