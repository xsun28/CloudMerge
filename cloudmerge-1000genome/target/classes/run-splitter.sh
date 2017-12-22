#!/bin/bash
PREFIX=ALL.chr
SUFFIX=.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
dir=$1
for ((i=$3;i<=$4;i=i+1)); do
tabix -p vcf $dir"/"${PREFIX}${i}${SUFFIX} &
pids[i]=$!
echo ${pids[i]}
done
for ((i=$3;i<=$4;i=i+1)); do
wait ${pids[i]}
done

for ((i=$3;i<=$4;i=i+2)); do
j=$((i+1))
java -jar cloudmerge-1000genome.jar -i $dir  -o $2 -d $5 -c $i-$j -p $6 -n $7  -s
done