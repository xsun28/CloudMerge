#! /bin/bash
START=$(date +%s)
hadoop jar plinkcloud-hbase.jar org.plinkcloud.hbase.HBaseVCF2TPEDSelectedChr -i plinkcloud/input/  -o HBase -r 0.0001 -n $1 -q PASS -c 1-26 -s true -g 9  ## -a
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Total execution time is: $DIFF"

exit