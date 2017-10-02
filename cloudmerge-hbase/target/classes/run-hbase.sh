#! /bin/bash

export HBASE_CONF_DIR=/etc/hbase/conf/
export HADOOP_CLASSPATH=./cloudmerge-hbase.jar:$HBASE_CONF_DIR:$(hbase classpath):$HADOOP_CLASSPATH
START=$(date +%s)
hadoop jar cloudmerge-hbase.jar org.cloudmerge.hbase.HBaseVCF2TPEDSelectedChr -i cloudmerge/input/  -o HBase -r 0.0001 -n $1 -q PASS -c 1-26 -s true -g 9  ## -a
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Total execution time is: $DIFF"

exit