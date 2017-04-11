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
val="achille"
'''wordCount = textFile.map(lambda word:"iliad"if val in word else "a", 1).reduceByKey(lambda a, b: a+b)'''
wordCount=textFile.filter(lambda word:val in word).filter(lambda value:"starting" in value)

wordCounts=wordCount.count()

print wordCounts
textFile2 = sc.textFile(tfile)
wordCount2 = textFile2.filter(lambda word:val in word).filter(lambda value:"starting" in value)

wordCounts2=wordCount2.count()
print wordCounts2