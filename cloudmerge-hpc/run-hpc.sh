#!/bin/bash
START=$(date +%s)
mpiexec -n $1 --hostfile $2/cloudmerge-hpc/hosts python -m mpi4py $2/cloudmerge-hpc/mpi_merge.py -i $2/input/ -o $2/output/ -n $3 -l 1 -u 2 -g 9 -f PASS
END=$(date +%s)
DIFF=$((END-START))
echo "Total running wall time is: $DIFF"
exit
