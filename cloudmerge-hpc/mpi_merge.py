#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 27 16:54:42 2017

@author: Xiaobo
"""
import numpy as np
from mpi4py import MPI
import commands
import os
import sys
path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(path)
#sys.path.append('/Users/Xiaobo/git/CloudMerge/CloudMerge/cloudmerge-hpc')
#sys.path.append('/home/ubuntu/cloudmerge/cloudmerge-hpc/')
import multiway_merge as mm
import argparse
###############################################################################
def get_source(rank,size,rounds):
    divisor = np.power(2,rounds)
    new_rank = int(rank/divisor)
    new_size = int(size/divisor)
    if (new_rank%2 != 0) or (new_rank+1>=new_size):
        return []
    elif (new_rank+2+1>=new_size) and (new_rank+2<new_size):
        return [divisor*(new_rank+1),divisor*(new_rank+2)]
    else:
        return [divisor*(new_rank+1),]

#------------------------------------------------------------------------------    
def get_dest(rank,size,rounds):
    SELF = -1
    divisor = np.power(2,rounds)
    new_rank = int(rank/divisor)
    new_size = int(size/divisor)
    if new_rank % 2 !=0:
        dest = divisor*(new_rank-1)
        return dest
    elif (new_rank + 1) >= new_size:
        dest = divisor*(new_rank-2)
        return dest if dest >=0 else 0        
    else:
        return SELF
#------------------------------------------------------------------------------
def splits(filenum,size):
    assigned = 0
    sendcounts = np.zeros(size)    
    disp = np.zeros(size)
    for i in range(size):        
        nxt_sz = int((filenum-assigned)/(size-i))
        disp[i] = assigned
        sendcounts[i] = nxt_sz
        assigned = assigned + nxt_sz
    return tuple(sendcounts),tuple(disp)

#------------------------------------------------------------------------------



###############################################################################


                                     
parser = argparse.ArgumentParser(description='cloudmerge-hpc')
parser.add_argument('-i',required=True,help='input file directory path',dest='input',metavar='/home/ubuntu/cloudmerge/input/')
parser.add_argument('-o',required=True,help='output file directory path',dest='output',metavar='/home/ubuntu/cloudmerge/output/')
parser.add_argument('-n',required=True,help='input file number',dest='filenum',metavar='10',type=int)
parser.add_argument('-l',required=False,default='1',help='lower boundary of chrmosomes',dest='lower_chr',metavar='1')
parser.add_argument('-u',required=False,default='M',help='upper boundary of chromosomes',dest='upper_chr',metavar='M')
parser.add_argument('-g',required=False,default=9,help='genotype column number',dest='gtype_col',metavar='9',type=int)
parser.add_argument('-f',required=False,default='PASS',help='filter value',dest='filter',metavar='PASS')
args = parser.parse_args()
#args = parser.parse_args('-i abc -o def -n 10 -l 1 -u 26 -g 9 -f PASS'.split())
#input_path = '/home/ubuntu/cloudmerge/input/'
#output_path = '/home/ubuntu/cloudmerge/output/'
#input_path = '/Users/Xiaobo/Desktop/input/'
#output_path = '/Users/Xiaobo/Desktop/output/'
input_path = args.input
output_path = args.output
filenum = args.filenum
lower_chr = args.lower_chr
upper_chr = args.upper_chr
qfilter = args.filter
genotype_col = args.gtype_col
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
host = commands.getoutput("hostname")
rounds = 0
sendcounts,disp = splits(filenum,size)
if rank == 0:
    sendbuff = np.linspace(1,filenum,filenum)

else: 
    sendbuff = None

rcvbuff = np.zeros(int(sendcounts[rank])) 
       
comm.Scatterv([sendbuff,sendcounts,disp,MPI.DOUBLE],rcvbuff,root=0)

#local_input_files = map(lambda x: input_path+str(int(x))+'.bz2',rcvbuff)
#for file in local_input_files:
#    print('unzipping files %s in rank %d' % (str(local_input_files),rank))
#    os.system('bunzip2 '+file)

local_input_files = map(lambda x: input_path+str(int(x))+'.bz2',rcvbuff)
local_merged_files = "_".join(map(lambda x: str(int(x)),rcvbuff))
merger = mm.multiway_merger(local_input_files, output_path+local_merged_files,lower_chr,upper_chr,qfilter,genotype_col,merge_type='vcf')
merger.start()
print('merged_files %s'%local_merged_files)

while True:
  src = get_source(rank,size,rounds)
#  if len(src) == 0:   #only when no source, we need a destination
  dest = get_dest(rank,size,rounds)
  rounds = rounds+1   
  if len(src) == 0:
     if rank > 0: 
         comm.send(local_merged_files,dest=dest,tag=0)
     print('i am rank %d, host is %s, sent merged files is %s, source is %s, dest is %d' %(rank,host,local_merged_files,str(src),dest))
     break         ## send the filename to dest process and quit
  elif len(src) == 1:
      local_files = [output_path+local_merged_files]
      rcv_merged_file = comm.recv(source=src[0],tag=0)
      local_files.extend([output_path+rcv_merged_file])
      local_merged_files = '_'.join([local_merged_files,rcv_merged_file])
      print('i am rank %d, host is %s, local merged file is %s, src is %s, dest is %d' %(rank,host,local_merged_files,str(src),dest))
  else:
      local_files = [output_path+local_merged_files]
      rcv_merged_files = []
      for i,s in enumerate(src):
          print('i am rank %d, host is %s, src is %s, dest is %d' %(rank,host,s,dest))
          rcv_file = comm.recv(source=s,tag=0)
          local_files.extend([output_path+rcv_file])
          rcv_merged_files.extend([rcv_file])
      local_merged_files = '_'.join([local_merged_files]+rcv_merged_files)
  
  if rank == 0:
      src = get_source(rank,size,rounds)
      if len(src) == 0:  #### the last merging step
          merger = mm.multiway_merger(local_files,output_path+local_merged_files,lower_chr,upper_chr,qfilter,genotype_col,False,merge_type='tped')
          merger.start()
          break;
  merger = mm.multiway_merger(local_files,output_path+local_merged_files,lower_chr,upper_chr,qfilter,genotype_col,merge_type='tped')
  merger.start()





################################################################################ 
#  if rank >0:
#      comm.send(local_merged_files,dest=dest,tag=0)
#  print('i am rank %d, host is %s, send local merged files is %s, source is %s, dest is %d' %(rank,host,local_merged_files,str(src),dest))
  
      
       
#print('rank is %d, host is %s, data is %s' %(rank,host,str(rcvbuff)))        
# create numpy arrays to reduce

#src = (np.arange(8) + rank*8).reshape(4,2)
#dst = np.zeros_like(src)
#
#def myadd(xmem, ymem, dt):
#    x = np.frombuffer(xmem, dtype=src.dtype)
#    y = np.frombuffer(ymem, dtype=src.dtype)
#
#    z = x + y
#
#    print("Rank %d on host %s reducing %s (%s) and %s (%s), yielding %s" % (rank, host, x, type(x), y, type(y), z))
#
#    y[:] = z
#
#op = MPI.Op.Create(myadd)
#
#MPI.COMM_WORLD.Reduce(src, dst, op)
#
#if MPI.COMM_WORLD.rank == 0:
#    print("ANSWER: %s" % dst)