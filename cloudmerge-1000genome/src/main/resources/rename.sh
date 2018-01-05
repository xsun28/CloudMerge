#!/bin/bash
i=1
for file in $1
do
if [[ "$file" =~ .*bz2$ ]];then
mv $file 1000genome/${i}.bz2
echo "${i}->${file}" >> names.txt
i=$((i+1))
fi
done