# -*- coding: utf-8 -*-
"""
Created on Thu May 17 00:11:14 2018

@author: TH
"""

__author__ = 'Helen Huang'
import os
import sys

from pyspark import SparkContext

try:
    sc.stop()
except:
    pass

sc = SparkContext('local')
doc = sc.parallelize([['a','b','c'],['b','d','d']])
words = doc.flatMap(lambda d:d).distinct().collect()
word_dict = {w:i for w,i in zip(words,range(len(words)))}
word_dict_b = sc.broadcast(word_dict)

def wordCountPerDoc(d):
    dict={}
    wd = word_dict_b.value
    for w in d:
        if wd[w] in dict:
            dict[wd[w]] +=1
        else:
            dict[wd[w]] = 1
    return dict

print(doc.map(wordCountPerDoc).collect())
print( "successful!")


