#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 26 15:53:03 2017

@author: Xiaobo
"""

class SNP(object):
    def __init__(self,sample,chrm,pos,ref,posid,genotype):
        self.id = sample
        self.chr = self.convert_chr(chrm)
        self.pos = pos
        self.ref = ref
        self.posid = posid
        self.genotype = genotype
    
    def __eq__(self,other):
        return  (self.id == other.id if self.pos == other.pos else self.pos == other.pos) if self.chr == other.chr else self.chr == other.chr
    
    def __gt__(self,other):
        return (self.id > other.id if self.pos == other.pos else self.pos > other.pos) if self.chr == other.chr else self.chr > other.chr
         
    def __lt__(self,other):
        return (self.id < other.id if self.pos == other.pos else self.pos < other.pos) if self.chr == other.chr else self.chr < other.chr
    
    def __str__(self):
        return str(self.id)+'\t'+str(self.chr)+'\t'+str(self.pos)+'\t'+str(self.ref)+'\t'+str(self.posid)+'\t'+str(self.genotype)
    
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