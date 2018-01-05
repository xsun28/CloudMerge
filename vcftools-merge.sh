#! /bin/bash
files=""
for file in *
do
if [[ "$file" =~ .*bz2$ ]]; then
unzipfile=${file%bz2}
unzipfile=$unzipfile'vcf'
bunzip2 -kc $file > $unzipfile
bgzip -cf $unzipfile > ${unzipfile}.gz
tabix -p vcf ${unzipfile}.gz
files=$files"${unzipfile}.gz\n"
fi
done
#echo $files

sorted_files=$(printf $files | sort -n | tr '\n' '\ ')
#echo $sorted_files
start=$(date +%s)
vcf-merge ${sorted_files}  | bgzip -c > out.vcf.gz
#vcftools --vcf ${files} --out test.tped --remove-filtered-geno-all --plink-tped
end=$(date +%s)
diff=$((end-start))