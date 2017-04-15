from pyspark import SparkConf, SparkContext
from collections import Counter
from pyspark.sql.functions import size
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql import Row
import os
import sys

def parseLogLine(logline):
    i=logline.index(': ')
    
    newstring=logline[i+1:]
    newstring=newstring.lower();
    
    if 'error' in newstring:
        return str(logline)
    else:
        return "none"
    
# Configure the Spark environment
sparkConf = SparkConf().setAppName("WordCounts").setMaster("local")
sc = SparkContext(conf = sparkConf)

# The WordCounts Spark program
tfile1=sys.argv[1]
tfile=sys.argv[2]
#val1=os.path.basename(tfile1)
val2="of user"
val1=" "
textFile = sc.textFile(tfile)


filter11=textFile.map(lambda a: str(parseLogLine(a)))
filter12=filter11.filter(lambda values:val1 in values)
count=filter12.count()
print count




