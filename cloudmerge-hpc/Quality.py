#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 31 18:25:10 2017

@author: Xiaobo
"""


class quality(object):
    def __init__(self,lower_chr,upper_chr,qual):
        self.lower_chr = self.getChr(lower_chr)
        self.upper_chr = self.getChr(upper_chr)
        self.qual = qual
    
        
    def qualified(self,line):
        if line is None:
            return False
        if len(line) == 0:
            return False
        if line[0].startswith('#'):
            return False
        if line[6].upper() != 'PASS':
            return False
        chr = self.getChr(line[0])
        if (chr<self.lower_chr) or (chr>self.upper_chr):
            return False
        return True
    
    @staticmethod    
    def getChr(chr):
        chrm = chr[3:] if chr.startswith('chr') else chr
        if chrm.upper() == 'X':
            return 23
        elif chrm.upper() == 'Y':
            return 24
        elif chrm.upper() == 'XY':
            return 25
        elif chrm.upper() == 'M':
            return 26
        else:
            return int(chrm)
            