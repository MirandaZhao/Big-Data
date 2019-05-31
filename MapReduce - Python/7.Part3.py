# -*- coding: utf-8 -*-
"""
Created on Mon Feb  4 01:59:22 2019

@author: miran
"""
from mrjob.job import MRJob
from mrjob.step import MRStep

                      
class Part3(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                   reducer=self.reducer1),
            MRStep(mapper=self.mapper2,
                   reducer = self.reducer2)
        ]

    def mapper1(self, _, line):                         
        (hostid, price, numberofreviews) = line.split(',')   
        yield hostid, int(numberofreviews)                   

    def reducer1(self, hostid, numberofreviews):                        
        yield hostid, sum(numberofreviews) 
     

    def mapper2(self, hostid, numberofreviews):
        yield '%05d'%int(numberofreviews), hostid
                                                                                 
    def reducer2(self, numberofreviews, hostids):
        for hostid in hostids:
            yield numberofreviews, hostid
if __name__ == '__main__':
    Part3.run()

#!python Part3.py AIRBNBLISTINGS.csv > Part3.txt