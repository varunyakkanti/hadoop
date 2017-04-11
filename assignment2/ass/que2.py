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
val2="Started"
'''wordCount = textFile.map(lambda word:"iliad"if val in word else "a", 1).reduceByKey(lambda a, b: a+b)'''

final=textFile.filter(lambda values:val2 in values)
wordCount=final.filter(lambda word:val in word)
wordCounts=wordCount.count()

print wordCounts
textFile2 = sc.textFile(tfile)
wordCount2 = textFile2.filter(lambda word:val in word)
final2=wordCount2.filter(lambda values:val2 in values)
wordCounts2=final2.count()
print wordCounts2