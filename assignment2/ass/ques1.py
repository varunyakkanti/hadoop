from pyspark import SparkConf, SparkContext
import os
import sys

# Configure the Spark environment
sparkConf = SparkConf().setAppName("Q1").setMaster("local")
sc = SparkContext(conf = sparkConf)

# The WordCounts Spark program
tfile1=sys.argv[1]
tfile2=sys.argv[2]
hostname1=os.path.basename(tfile1)
hostname2=os.path.basename(tfile2)
textFile = sc.textFile(tfile1)
wordCounts1 = textFile.map(lambda word: (hostname1, 1)).reduceByKey(lambda a, b: a+b)

textFile2 = sc.textFile(tfile2)
wordCounts2 = textFile2.map(lambda word: (hostname2, 1)).reduceByKey(lambda a, b: a+b)
print '*Q1 Line Count'
for wc in wordCounts1.collect(): print '+ ' +str(wc)
for wc2 in wordCounts2.collect(): print '+ '+str(wc2)