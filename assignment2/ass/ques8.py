
from pyspark import SparkConf, SparkContext

import os
import sys

def parseLogLine(logline):
    if 'Started' in logline:
        i=logline.index('user')
        return str(logline[i+1])
    
# Configure the Spark environment
sparkConf = SparkConf().setAppName("Q8").setMaster("local")
sc = SparkContext(conf = sparkConf)

# The WordCounts Spark program
tfile1=sys.argv[1]
tfile2=sys.argv[2]
val1="Started Session"
val2="of user"
hostname1=os.path.basename(tfile1)
hostname2=os.path.basename(tfile2)
textFile = sc.textFile(tfile1)
filter11=textFile.filter(lambda values:val1 in values)
filter12=filter11.filter(lambda values:val2 in values)
filter13=filter12.map(lambda b:b.split())
filter14=filter13.map(lambda a: str(parseLogLine(a)))
filter15=filter14.distinct()


textFile2 = sc.textFile(tfile2)
filter21= textFile2.filter(lambda word:val1 in word)
filter22=filter21.filter(lambda word:val2 in word).map(lambda b:b.split())
filter23=filter22.map(lambda a: str(parseLogLine(a)))
filter24=filter23.distinct()

filter16=filter15.subtract(filter24)
filter17=filter16.map(lambda values:( values,hostname1))
filter25=filter24.subtract(filter15)
filter26=filter25.map(lambda values:( values,hostname2))
filter=filter17.union(filter26)
print'* Q8: users who started a session on exactly one host, with host name.'
print '+ '+str(filter.collect())
