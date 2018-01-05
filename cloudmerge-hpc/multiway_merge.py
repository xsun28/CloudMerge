#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 26 15:51:57 2017

@author: Xiaobo
"""
import sys
import os
path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(path)
#sys.path.append('/Users/Xiaobo/git/CloudMerge/CloudMerge/cloudmerge-hpc')
#sys.path.append('/home/ubuntu/cloudmerge/cloudmerge-hpc/')
import SNP
import Quality 
import heapq as pq
import csv
import bz2
import re
import numpy as np
import TPED
##################################
class multiway_merger(object):
    
    def __init__(self,files,output,lower_chr,upper_chr,qfilter,genotype_col,add_ref=True,merge_type='vcf'):
        self.files = files
        self.file_num = len(files)
        self.write_fd = open(output,'w')
        self.writer = csv.writer(self.write_fd,delimiter='\t')
        self.queue = []
        self.add_ref = add_ref
        self.type = merge_type
        if merge_type == 'vcf':
            
            self.readers,self.fds = self.instantiate_vcf_readers(files)
            self.quality = Quality.quality(lower_chr,upper_chr,qfilter)
            self.genotype_col = genotype_col
        elif merge_type == 'tped':
            self.fids = []
            for f in files:
                names = re.split('/',f)[-1]
                if '_' in names:
                    sid = int(re.split('_',names)[0])
                    eid = int(re.split('_',names)[-1])
                else:
                    sid = int(names)
                    eid = int(names)            
#               self.file_num = self.file_num+(eid-sid+1)
                self.fids.extend([(sid,eid)])
            self.readers,self.fds = self.instantiate_tped_readers(files)
        else:
            print('merge type error,exiting')
            sys.exit(2)

    def start(self):
        if self.type == 'vcf':
            self.vcf_merge()
        else:
            self.tped_merge()
        self.all_close()

        
    def read_snp(self,i):
        reader = self.readers[i]
        while True:
            try:
                line =  reader.next()
                break;
            except csv.Error:
                print('CSV Null Error')
                continue
        
        while not self.quality.qualified(line):
            try:
                line = reader.next()
            except csv.Error:
                print('CSV end Error')
                line = None
                break
            except StopIteration:
                print('File'+str(i)+' EOF')
                line = None
                break
        snp = self.get_snp(i+1,line,self.genotype_col) if line is not None else None
        return snp
        
    def instantiate_vcf_readers(self,files):
        fds = []
        readers = []
        fds.extend(bz2.BZ2File(file,'r') for file in files)
#        fds.extend(open(file,'r') for file in files)
        readers.extend(csv.reader(fd,delimiter='\t') for fd in fds)
        return readers,fds
    
    def instantiate_tped_readers(self,files):
        fds = []
        readers = []
        fds.extend(open(file,'r') for file in files)
        readers.extend(csv.reader(fd,delimiter='\t') for fd in fds)
        return readers,fds
    
    def clean_up(self):
        self.all_close()
        for file in self.files:
            os.remove(file)
            
    def all_close(self):
        for fd in self.fds:
            fd.close()
        self.write_fd.close()
        
    def get_snp(self,sampleid,line,genotype_col):
        chrm = Quality.quality.getChr(line[0])
        pos = int(line[1])
        posid = line[2]
        ref = line[3]
        alt = line[4]
        gtype = line[genotype_col]
        genotype = self.get_genotype(gtype,ref,alt)
        return SNP.SNP(sampleid,chrm,pos,ref,posid,genotype)
            
    def get_genotype(self,gtype,ref,alt):
        genotype_pattern = re.compile(r'[\d]{1}([/|]{1}[\d]{1})+')
        types = map(int,re.split('[/|]',genotype_pattern.match(gtype).group(0)))
        alts = re.split(',',alt)
        alt_types = [ref if t==0 else alts[t-1] for t in types]
        return ' '.join(alt_types)
    
    def vcf_merge(self):
        for i in np.arange(len(self.readers)):
            snp = self.read_snp(i)
            pq.heappush(self.queue,snp)
        new_line = True
        while len(self.queue) >0:
            snp = pq.heappop(self.queue)
            i = snp.id
            if new_line:
                new_line = False
                curr_chr = snp.chr
                curr_pos = snp.pos
                ref = snp.ref
                default_gtype = snp.ref+' '+snp.ref
                snps = []
                for j in np.arange(self.file_num):
                    default_snp = SNP.SNP(j+1,curr_chr,curr_pos,ref,snp.posid,default_gtype)
                    snps.extend([default_snp])
            if (curr_chr == snp.chr) and (curr_pos == snp.pos):
                snps[i-1] = snp
            else:
                if self.add_ref:
                    self.write_vcf_to_tped(snps,ref)
                else:
                    self.write_vcf_to_tped(snps)
                new_line = True
            new_snp = self.read_snp(i-1)
            if new_snp is not None:
                pq.heappush(self.queue,new_snp)
            
    def write_vcf_to_tped(self,snps,ref=None):
        row = []
        for i,snp in enumerate(snps):
            if i == 0:
                row.extend([SNP.SNP.convert_chr_to_str(snp.chr)])
                row.extend([snp.posid])
                row.extend(['0'])
                row.extend([str(snp.pos)])
                if self.add_ref and ref is not None:
                    row.extend([ref])
            row.extend([snp.genotype])
#        print(tuple(row))
        self.writer.writerow(tuple(row))
    
    def tped_merge(self):
        for i in np.arange(len(self.readers)):
            tped = self.read_tped(i)
            pq.heappush(self.queue,tped)
        new_line = True
        while len(self.queue) >0:
            tped = pq.heappop(self.queue)
            readerid = tped.readerid
            if new_line:
                new_line = False
                curr_chr = tped.chr
                curr_pos = tped.pos
                posid = tped.posid
                dist = tped.dist
                ref = tped.ref
                tpeds = []
                for j in np.arange(self.file_num):
                    sid,eid = self.fids[j]
                    default_gtype = []
                    for i in np.arange(sid,eid+1):
                        default_gtype.extend([tped.ref+' '+tped.ref])
                    default_tped = TPED.TPED(j,sid,eid,curr_chr,posid,dist,curr_pos,ref,default_gtype)
                    tpeds.extend([default_tped])
            if (curr_chr == tped.chr) and (curr_pos == tped.pos):                
                tpeds[readerid] = tped
            else:
                if self.add_ref:
                    self.write_tped_to_tped(tpeds,ref)
                else:
                    self.write_tped_to_tped(tpeds)
                new_line = True
            new_tped = self.read_tped(readerid)
            if new_tped is not None:
                pq.heappush(self.queue,new_tped)
                
    
    def read_tped(self,i):
        reader = self.readers[i]
        try:
            line = reader.next()
        except csv.Error:
            print('read tped CSV Error, exiting....')
            sys.exit(1)
        except StopIteration:
            print('File'+str(i)+' EOF')
            line = None
        sid,eid = self.fids[i]
        tped = TPED.TPED(i,sid,eid,line[0],line[1],line[2],line[3],line[4],line[5:]) if line is not None else None
        return tped
    
    def write_tped_to_tped(self,tpeds,ref=None):
        row = []
        for i,tped in enumerate(tpeds):
            if i == 0:
                row.extend([TPED.TPED.convert_chr_to_str(tped.chr)])
                row.extend([tped.posid])
                row.extend([tped.dist])
                row.extend([str(tped.pos)])
                if self.add_ref and ref is not None:
                    row.extend([ref])
            row.extend(tped.genotypes)
#        print(tuple(row))
        self.writer.writerow(tuple(row))
    
    
    ###############################################################################    
#if __name__ == '__main__':
#    files = os.listdir('/Users/Xiaobo/Desktop/input')
#    files = map(lambda x: '/Users/Xiaobo/Desktop/input/'+x, files)
#    output = '/Users/Xiaobo/Desktop/merged'
#    lower_chr = 1
#    upper_chr = 2
#    qfilter = 'PASS'
#    genotypecol=9
#    mm = multiway_merger(files,output,lower_chr,upper_chr,qfilter,genotypecol)
#    mm.start()

        