
#from pyspark import SparkContext, SparkConf #this is correct
from pyspark import * #this is lazy :)
import time

#define SparkConf info here, but that is for later modules
conf = SparkConf().setAppName("RDDPersistance").setMaster("local")

sc = SparkContext(conf=conf)

#Putting some timers to show the difference in compilation

def time_usage(func):
    def wrapper(*args, **kwargs):
        beg_ts = time.time()
        func(*args, **kwargs)
        end_ts = time.time()
        print("elapsed time: %f" % (end_ts - beg_ts))
    return wrapper


@time_usage
def printSamples(data):
	#some actions to generate times
	print len(data.top(5))
	print len(data.top(20))
	print len(data.top(100))
#Textfile from here: http://www.gutenberg.org/cache/epub/1661/pg1661.txt
#Would be better to import directly from source, but odds are the text will be local

rawText = sc.textFile("file:///home/daniel/python/HdpcdSpark/data/sherlock.txt")

#Most examples seem to start with lambda functions, I find it easier to seperate the functions

def lineLength(data):
	words = data.split(" ")
	return(len(words))


lengthCheck = rawText.map(lambda x: (x, lineLength(x)))

print "Running twice without persisting"
printSamples(lengthCheck)

printSamples(lengthCheck)

#Must persist RDD first before an action, otherwise will recompile
lengthCheck.persist(StorageLevel.MEMORY_ONLY)

print "Twice with persistance"
printSamples(lengthCheck)

printSamples(lengthCheck)

lengthCheck.unpersist()

lengthCheck.persist(StorageLevel.DISK_ONLY)

print "Running again, with Disk Only"
printSamples(lengthCheck)

print "And again to confirm persistance"

printSamples(lengthCheck)

lengthCheck.unpersist()

	
#placeholder to view output easily
input("Hit any key to continue...")
