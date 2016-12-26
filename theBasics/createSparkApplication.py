
from pyspark import SparkContext, SparkConf 

#define SparkConf info here, but that is for later modules
conf = SparkConf().setAppName("firstPySparkApp").setMaster("local")

sc = SparkContext(conf=conf)

#Build a simple dataset in python
data = [1, 2, 3, 4, 5]

#intilize the data in spark context with 'parallelize()'
distData = sc.parallelize(data)

#Completely ineffienct and quite foolish, don't do this in real development
#Only here to show data moving back and forth between python and spark
localData = distData.collect()

print localData

#placeholder to view output easily
input("Import Successful! Hit any key to continue...")
