# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 16:59:45 2019

@author: miran
"""

from mrjob.job import MRJob

class MRPart1(MRJob):

    def mapper(self, _, line):                         
        (CustomerID, ItemID, AmountSpent) = line.split(',')   #?why use comma & no space for variable defination
        yield CustomerID, float(AmountSpent)                   #float function 保留一位小数 

    def reducer(self, CustomerID, AmountSpent):                        
        yield CustomerID, sum(AmountSpent) 
        

if __name__ == '__main__':
    MRPart1.run()
    
 #!python Part1.py DataA1.csv > Part1.txt
                                                            #?how to sort Customer ID in order