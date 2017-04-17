from pyspark import SparkConf, SparkContext
from collections import Counter
from pyspark.sql.functions import size
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql import Row
import os
import sys

def parseLogLine(logline):
    logline=logline.lower()
    if 'error' in logline:
        return str(logline)
    else:
        return "none"
    
# Configure the Spark environment
sparkConf = SparkConf().setAppName("Q5").setMaster("local")
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
count1=filter13.count()



textFile = sc.textFile(tfile2)

filter21=textFile.map(lambda x:x.split(None,4))
filter22=filter21.map(lambda a: str(parseLogLine(a[4])))
filter23=filter22.filter(lambda values:val1 in values)
count2=filter23.count()
print 'Q5: number of errors'
print '+ '+hostname1+' '+str(count1)
print '+ '+hostname2+' '+str(count2)

