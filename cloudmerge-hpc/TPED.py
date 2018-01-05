#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 31 23:30:37 2017

@author: Xiaobo
"""

class TPED(object):
    def __init__(self,readerid,sid,eid,chrm,posid,dist,pos,ref,genotypes):
        self.readerid = readerid
        self.sid = sid
        self.eid = eid
        self.chr = self.convert_chr(chrm)
        self.pos = int(pos)
        self.dist = dist
        self.posid = posid
        self.ref = ref
        self.genotypes = genotypes
    
    def __eq__(self,other):
        return  (self.sid == other.sid if self.pos == other.pos else self.pos == other.pos) if self.chr == other.chr else self.chr == other.chr
    
    def __gt__(self,other):
        return (self.sid > other.sid if self.pos == other.pos else self.pos > other.pos) if self.chr == other.chr else self.chr > other.chr
         
    def __lt__(self,other):
        return (self.sid < other.sid if self.pos == other.pos else self.pos < other.pos) if self.chr == other.chr else self.chr < other.chr
    
    def __str__(self):
        return str(self.sid)+'-'+self.eid+'\t'+str(self.chr)+'\t'+str(self.pos)+'\t'+self.dist+'\t'+str(self.posid)+'\t'+self.ref+'\t'+str(self.genotypes)
    
    def convert_chr(self,chr):
        str_chr = str.lower(str(chr))
        if str_chr == 'x':
            return 23
        elif str_chr == 'y':
            return 24
        elif str_chr == 'xy':
            return 25
        elif str_chr == 'm':
            return 26
        else: 
            return int(chr)
    
    @staticmethod
    def convert_chr_to_str(chrm):
        if chrm == 23:
            return 'X'
        elif chrm == 24:
            return 'Y'
        elif chrm == 25:
            return 'XY'
        elif chrm == 26:
            return 'M'
        else:
            return str(chrm)