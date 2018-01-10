#!/bin/bash
PREFIX=ALL.chr
SUFFIX=.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
dir=$1
for ((i=$3;i<=$4;i=i+1)); do
chrstr=$i
if [[ i == 23 ]]; then
chrstr=X
elif [[ i == 24 ]]; then
chrstr=Y
elif [[ i == 25 ]]; then
chrstr=MT
fi
tabix -p vcf $dir"/"${PREFIX}${chrstr}${SUFFIX} &
pids[i]=$!
echo ${pids[i]}
done
for ((i=$3;i<=$4;i=i+1)); do
wait ${pids[i]}
done

for ((i=$3;i<=$4;i=i+2)); do
j=$((i+1))
if ((j > $4)); then
j=$i
fi
java -jar cloudmerge-1000genome.jar -i $dir  -o $2  -c $i-$j  -n $5  -s -d $6 -p $7
done

i=1
for file in $2
do
if [[ "$file" =~ .*bz2$ ]];then
mv $file 1000genome/${i}.bz2
echo "${i}->${file}" >> names.txt
i=$((i+1))
fi
done