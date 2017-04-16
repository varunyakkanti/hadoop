from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import size
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql import Row
import os
import sys

def parseLogLine(logline):
    if 'Started' in logline:
        i=logline.index('user')
        return str(logline[i+1])
def changeusername(log,filter):
    for value in filter:
        if value[0] in log:
            log = log.replace(value[0],value[1])
    return log    
# Configure the Spark environment
sparkConf = SparkConf().setAppName("WordCounts").setMaster("local")
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
filter15=filter14.distinct().zipWithIndex()
filter16=filter15.map(lambda x:(x[0],'user-'+str(x[1])))
broadcast = sc.broadcast(filter16.collect())
last1= textFile.map(lambda y:changeusername(y,broadcast.value))


textFile2 = sc.textFile(tfile2)
filter21= textFile2.filter(lambda word:val1 in word)
filter22=filter21.filter(lambda word:val2 in word).map(lambda b:b.split())
filter23=filter22.map(lambda a: str(parseLogLine(a)))
filter24=filter23.distinct().zipWithIndex()
filter25=filter24.map(lambda x:(x[0],'user-'+str(x[1])))
broadcast = sc.broadcast(filter25.collect())
last2 = textFile.map(lambda y:changeusername(y,broadcast.value))
print '*Q3 unique user names '
print '+ '+hostname1+' '+str(last1)
print '+ '+hostname2+' '+str(last2)