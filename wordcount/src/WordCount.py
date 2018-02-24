'''
Created on Feb 6, 2018

@author: runshengsong
'''
import sys

from pyspark import SparkContext, SparkConf


if __name__ == '__main__':
    conf = SparkConf().setAppName("WordCount")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("/Users/runshengsong/Documents/workspace/learning_spark/wordcount/data/long_text.txt")
    
    counts = lines.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
    
    output = counts.collect()  
    counts.saveAsTextFile("WordCount")