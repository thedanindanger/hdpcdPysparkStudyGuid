
#from pyspark import SparkContext, SparkConf #this is correct
from pyspark import * #this is lazy :)
from pyspark.sql import *
from pyspark.sql.types import * #Still have to do this
import datetime
#import sys
import traceback
#define SparkConf info here, but that is for later modules
conf = SparkConf().setAppName("sparkSQLintro").setMaster("local")

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)  #I can see why they changed this to SparkSession in 2.0!

path = "file:///home/daniel/python/HdpcdSpark/data/mockCustInfo.csv"

#First attempting to read directly into sql context

try:
	sqlData = sqlContext.read.load(path)

except Exception as exc:
	print traceback.format_exc()
	print exc

#Fails spectacularly!

#Next we try with reading the csv into an RDD then converting to sql context

try:
	csvRDD = sc.textFile(path)
	sqlData = sqlContext.createDataFrame(csvRDD)

except Exception as exc:
	print traceback.format_exc()
	print exc

#Well that didn't work either...

#This is due to textfile reading each line as a siingle string
#For context

csvRDD = sc.textFile(path)
samp5 = csvRDD.take(5)

print samp5

#So we need to parse our csv


try:
	csvRDD = sc.textFile(path)
	csvParsed = csvRDD.map(lambda x: x.split(','))
	sqlData = sqlContext.createDataFrame(csvParsed)

except Exception as exc:
	print traceback.format_exc()
	print exc


#Success! Or is it?...

sqlData.show(5)

sqlData.dtypes

#Our headers were read as columns, so we need to rip our the first row of data and store it for names

#First collect our column names for use later and to visualize

colNames = csvParsed.first()

#Exclude the colnames from our data set

csvParsedBody = csvParsed.filter(lambda line: line != colNames) 

#Now we would expect spark sql to be able to deduce the schema...

sqlParsedBody = sqlContext.createDataFrame(csvParsedBody)
sqlParsedBody.printSchema()

#But we would be wrong. they are still all strings

#So we need to make a schemma... good thing we saved those colNames :)

#this is straight out of the documentation, we just assign a type to all our values with the StructFields

#Creating intial tuple of field structures
#We don't need to do all the fancy string splitting since we already have the parsed colNames

fields =  [StructField(field_name, StringType(), True) for field_name in colNames]

#This gives us a tuple of StructField objects, all assigned the type of string

#We want to modify the values to correspond to our actual data

sqlParsedBody.show(5)

#Which only the last three columns require changeing

#The standard way of changing is as follows

fields[5].dataType = IntegerType()

#The syntax for the types are absolutely burried in the documentation: https://spark.apache.org/docs/1.6.1/api/python/pyspark.sql.html#pyspark.sql.types.DataType

#But I would rather be a little more confident in what I'm changing as the process was somewhat repetitive
#I know my attention span, or lake there of, so I was worried about messing something up
# Which is why I designed this function, also as an exercise to understand the concept better

def dataChange(fieldTuple, fieldName, dataType):
	index = [y.name for y in fieldTuple].index(fieldName)
	fieldTuple[index].dataType = dataType
	return(fieldTuple)

fields = dataChange(fields,'age',IntegerType())

fields = dataChange(fields,'salary',IntegerType())

fields = dataChange(fields,'date_of_birth',DateType())

#DateType was of course giving me a headache
#Today I remembered why I hate dates in programming!
#Also why I hate working with csvs in spark to spark sql. It's practically like being a dba...

csvParsedDateBody = csvParsedBody.map(lambda x: [x[0],x[1],x[2],x[3],x[4],int(x[5]),int(x[6]),datetime.datetime.strptime(x[7],'%Y-%m-%d')])


#finally we can make a schema

schema = StructType(fields)


#and finally, finally make a dataframe...

sqlSchemaData = sqlContext.createDataFrame(csvParsedDateBody, schema)


sqlSchemaData.show(5)
	

