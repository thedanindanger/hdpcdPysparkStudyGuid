
from pyspark import SparkContext, SparkConf 

#define SparkConf info here, but that is for later modules
conf = SparkConf().setAppName("firstPySparkApp").setMaster("local")

sc = SparkContext(conf=conf)

#Textfile from here: http://www.gutenberg.org/cache/epub/1661/pg1661.txt
#Would be better to import directly from source, but odds are the text will be local


#Build a simple dataset in python
rawText = sc.textFile("file:///home/daniel/python/HdpcdSpark/data/sherlock.txt")

#Most examples seem to start with lambda functions, I find it easier to seperate the functions

def lineLength(data):
	words = data.split(" ")
	return(len(words))

lengthFunc = rawText.map(lineLength)
lengthLambda = rawText.map(lambda x: len(x.split(" ")))

#Can combine defined functions and annomous functions as well:

lengthCheck = rawText.map(lambda x: (x, lineLength(x)))

print lengthFunc.top(5)
print lengthLambda.top(5)
print lengthCheck.top(5)
	
#placeholder to view output easily
input("Hit any key to continue...")
