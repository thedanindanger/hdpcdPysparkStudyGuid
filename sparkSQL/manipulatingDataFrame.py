
#from pyspark import SparkContext, SparkConf #this is correct
from pyspark import * #this is lazy :)
from pyspark.sql import *
from pyspark.sql.types import * #Still have to do this
import datetime

import traceback
#define SparkConf info here, but that is for later modules

conf = SparkConf().setAppName("manipulatingDataFrames").setMaster("local")

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)  #I can see why they changed this to SparkSession in 2.0!

path = "file:///home/daniel/python/HdpcdSpark/data/mockCustInfo.csv"


try:
	csvRDD = sc.textFile(path)
	csvParsed = csvRDD.map(lambda x: x.split(','))

except Exception as exc:
	print traceback.format_exc()
	print exc


#Success! Or is it?...

#Our headers were read as columns, so we need to rip our the first row of data and store it for names

#First collect our column names for use later and to visualize

colNames = csvParsed.first()

#Exclude the colnames from our data set

csvParsedBody = csvParsed.filter(lambda line: line != colNames) 

#Now we would expect spark sql to be able to deduce the schema...

sqlParsedBody = sqlContext.createDataFrame(csvParsedBody)

fields =  [StructField(field_name, StringType(), True) for field_name in colNames]

#This gives us a tuple of StructField objects, all assigned the type of string


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


#sqlSchemaData.show(5)


#+---+----------+---------+--------------------+------+---+------+-------------+
#| id|first_name|last_name|               email|gender|age|salary|date_of_birth|
#+---+----------+---------+--------------------+------+---+------+-------------+
#|  1|      Joan|    White|   jwhite0@phpbb.com|Female| 95|173434|   1974-12-26|


#Table for reference

#now for a few basic operations

#Summation

totalSaldf = sqlSchemaData.select(sum('salary').alias('salary'))

print "pyspark aggregation"

print totalSaldf.show()

#Summation with sql

sqlSchemaData.registerTempTable("sqlTemp")

totalSalsql = sqlContext.sql("SELECT SUM(salary) AS salary FROM sqlTemp")

print "SQL aggregation"

print totalSalsql.show()

#both values are the same and produce new dataframes

totalSaldfInt = totalSaldf.collect()[0][0]

totalSalsqlInt = totalSalsql.collect()[0]['salary']

print ("Core pyspark.sql gives %s, and sqlContext.sql give %s" % (totalSaldfInt, totalSalsqlInt))


#Aggregation by column values

avgGenderSaldf = sqlSchemaData.groupby('gender').avg('salary')

print "pyspark.sql aggregation"

print avgGenderSaldf.show()

avgGenderSalsql = sqlContext.sql("SELECT gender,AVG(salary) AS avgSalary FROM sqlTemp GROUP BY gender")

print "sqlContext aggregation"

print avgGenderSalsql.show()


# Multiple aggregations

avgTotGenderSaldf = sqlSchemaData.groupby('gender')\
	.agg(avg('salary').alias('avgSalary'), sum('salary').alias('totSalary'))

avgTotGenderSalsql = sqlContext.sql("SELECT gender, AVG(salary) AS avgSalary, SUM(salary) AS totSalary FROM sqlTemp GROUP BY gender")

print avgTotGenderSaldf.show()

print avgTotGenderSalsql.show()


#Practicing window function

#DataFrame API

window = Window.partitionBy("date_of_birth").orderBy("salary")

rankSal = sqlSchemaData.select(rank().over(window).alias("rank"), "first_name", "last_name", "gender", "salary")

print rankSal.show(5)

rankSalsql = sqlContext.sql("SELECT RANK() OVER (PARTITION BY date_of_birth ORDER BY salary) AS salrank, gender, date_of_birth FROM sqlTemp")

print rankSalsql.show(5)