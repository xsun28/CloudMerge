#! /bin/bash
START=$(date +%s)
hadoop jar plinkcloud-mapreduce.jar org.plinkcloud.mapreduce.MRVCF2TPED -D mapreduce.task.io.sort.mb=600 -D mapreduce.reduce.merge.inmem.threshold=0 -D mapreduce.reduce.input.buffer.percent=1 -i /user/hadoop/plinkcloud/input/ -o /user/hadoop/mapreduce/output/ -n $1 -r 0.0001 -c 1-26 -s false -q PASS -g 9 ## -e
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Total execution time is: $DIFF"

exit