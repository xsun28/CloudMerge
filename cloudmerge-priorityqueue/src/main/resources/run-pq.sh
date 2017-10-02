#! /bin/bash
START=$(date +%s)
java -jar cloudmerge-priorityqueue.jar -i VCF/ -o Result.tped -c 1-26 -q PASS -s true -g 9
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Total execution time is: $DIFF"

exit