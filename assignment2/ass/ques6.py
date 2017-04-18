from pyspark import SparkConf, SparkContext

import os
import sys

def parseLogLine(logline):
    
    logline=logline.lower();
    
    if 'error' in logline:
        return str(logline)
    else:
        return "none"
    
# Configure the Spark environment
sparkConf = SparkConf().setAppName("Q6").setMaster("local")
sc = SparkContext(conf = sparkConf)
# The WordCounts Spark program
tfile1=sys.argv[1]
tfile2=sys.argv[2]
#val1=os.path.basename(tfile1)
hostname1=os.path.basename(tfile1)
hostname2=os.path.basename(tfile2)
val1=" "
textFile = sc.textFile(tfile1)
filter11=textFile.map(lambda x:x.split(None,4))
filter12=filter11.map(lambda a: str(parseLogLine(a[4])))
filter13=filter12.filter(lambda values:val1 in values)
filter14=filter13.map(lambda values:(values,1)).reduceByKey(lambda a, b: a+b)
filter15=filter14.map(lambda (a,b):(b,a)).sortByKey(0,1)





textFile = sc.textFile(tfile2)
filter21=textFile.map(lambda x:x.split(None,4))
filter22=filter21.map(lambda a: str(parseLogLine(a[4])))
filter23=filter22.filter(lambda values:val1 in values)
filter24=filter23.map(lambda values:(values,1)).reduceByKey(lambda a, b: a+b)
filter25=filter24.map(lambda (a,b):(b,a)).sortByKey(0,1)
print'Q6 5 most frequent errors'
print '+ ' +hostname1
for wc in filter15.take(5): print wc
print '+ ' +hostname2
for wc in filter25.take(5): print wc