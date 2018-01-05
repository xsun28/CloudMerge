#!/bin/bash
START=$(date +%s)
input=VCF/
output=Result/

i=0
for file in $(ls $input); do
#if [[ ${file} =~ .*(gz)$ ]]; then
#echo $file
#fi
#done
#exit
if [[ ${file} =~ .*(gz)$ ]]; then
tabix -p vcf $input${file} &
pids[i]=$!
echo ${pids[i]}
i=$((i+1))
fi
done

for ((j=0;j<$i;j=j+1)); do
wait ${pids[j]}
echo tabix process$j finished...
done

i=0
j=0
java -jar cloudmerge-ppqsplitdata.jar ${output}/splits.csv $1 $2 $3 chr
for l in $(strings ${output}splits.csv); do
echo $l
i=$((i+1))
java -jar cloudmerge-parallelpriorityqueue.jar -i $input -o $output  -q PASS -s true -g 9 -p $i -r $l &
pids[j]=$!
echo ${pids[j]}
j=$((j+1))
done

for ((k=0;k<$j;k=k+1)); do
wait ${pids[k]}
echo merging process$k finished...
done

END=$(date +%s)
DIFF=$((END-START))
echo "Total execution time is: $DIFF"




