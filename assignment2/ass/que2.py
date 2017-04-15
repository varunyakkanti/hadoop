from pyspark import SparkConf, SparkContext
import os
import sys

# Configure the Spark environment
sparkConf = SparkConf().setAppName("WordCounts").setMaster("local")
sc = SparkContext(conf = sparkConf)

# The WordCounts Spark program
tfile1=sys.argv[1]
tfile2=sys.argv[2]
hostname1=os.path.basename(tfile1)
hostname2=os.path.basename(tfile2)
textFile = sc.textFile(tfile1)
val="of user achille"
val2="Starting Session"
'''wordCount = textFile.map(lambda word:"iliad"if val in word else "a", 1).reduceByKey(lambda a, b: a+b)'''

final=textFile.filter(lambda values:val2 in values)
wordCount=final.filter(lambda word:val in word)
wordCounts=wordCount.count()


textFile2 = sc.textFile(tfile2)
wordCount2 = textFile2.filter(lambda word:val in word)
final2=wordCount2.filter(lambda values:val2 in values)
wordCounts2=final2.count()
print '*Q2 sessions of user "achille"'
print '+ '+hostname1+" "+str(wordCounts)
print '+ '+hostname2+" "+str(wordCounts2)