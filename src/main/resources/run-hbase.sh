#! /bin/bash

export HBASE_CONF_DIR=/etc/hbase/conf/
export HADOOP_CLASSPATH=./plinkcloud-hbase.jar:$HBASE_CONF_DIR:$(hbase classpath):$HADOOP_CLASSPATH
START=$(date +%s)
hadoop jar plinkcloud-hbase.jar org.plinkcloud.hbase.HBaseVCF2TPEDSelectedChr -i plinkcloud/input/  -o HBase -r 0.0001 -n $1 -q PASS -c 1-26 -s true -g 9  ## -a
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Total execution time is: $DIFF"

exit