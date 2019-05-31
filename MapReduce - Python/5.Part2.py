# -*- coding: utf-8 -*-
"""
Created on Mon Feb  4 01:59:22 2019

@author: miran
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
                           
class Part2(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                   reducer=self.reducer1),
            MRStep(mapper=self.mapper2,
                   reducer = self.reducer2)
        ]

    def mapper1(self, _, line):                         
        (CustomerID, ItemID, AmountSpent) = line.split(',')   
        yield CustomerID, float(AmountSpent)                   

    def reducer1(self, CustomerID, AmountSpent):                        
        yield CustomerID, sum(AmountSpent)                                                               

    def mapper2(self, CustomerID, AmountSpent):
        yield '%04.02f'%float(AmountSpent), CustomerID
                                                                                 
    def reducer2(self, AmountSpent, CustomerIDs):
        for CustomerID in CustomerIDs:
            yield AmountSpent, CustomerID
if __name__ == '__main__':
    Part2.run()


#!python Part2.py DataA1.csv > Part2.txt