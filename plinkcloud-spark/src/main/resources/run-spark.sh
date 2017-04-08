#! /bin/bash
START=$(date +%s)
spark-submit --class org.plinkcloud.spark.VCF2TPEDSpark --master yarn --deploy-mode cluster --executor-cores 1 --executor-memory 1g --conf spark.network.timeout=10000000 --conf spark.yarn.executor.memoryOverhead=700 --conf spark.shuffle.memoryFraction=0.5 plinkcloud-spark.jar plinkcloud/input/ Spark/output $1 1-26 PASS
# lower executor memory to decrease GC time, and if executors need share resources or communicate, assign more cores(no more than 5) and larger memory(At most 5G) to improve performance.
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Total execution time is: $DIFF"

exit