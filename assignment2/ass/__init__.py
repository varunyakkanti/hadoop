from pyspark import SparkConf, SparkContext
import os
import sys

# Configure the Spark environment
sparkConf = SparkConf().setAppName("WordCounts").setMaster("local")
sc = SparkContext(conf = sparkConf)

# The WordCounts Spark program
tfile1=sys.argv[1]
tfile=sys.argv[2]
textFile = sc.textFile(tfile1)
wordCounts = textFile.map(lambda word: ("iliad", 1)).reduceByKey(lambda a, b: a+b)
for wc in wordCounts.collect(): print wc
textFile2 = sc.textFile(tfile)
wordCounts2 = textFile2.map(lambda word: ("odyssey", 1)).reduceByKey(lambda a, b: a+b)
for wc2 in wordCounts2.collect(): print wc2