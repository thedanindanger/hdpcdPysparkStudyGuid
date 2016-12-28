from pyspark import * #this is lazy :)
from pyspark.sql import *
from pyspark.sql.types import * #Still have to do this
import datetime

conf = SparkConf().setAppName("HiveData").setMaster("local")

sc = SparkContext(conf=conf)

sqlContext = HiveContext(sc)  #require Hive .jars

demo = sqlContext.sql("SELECT * FROM xademo.call_detail_records")

json = sqlContext.read.json("hdfs:///user/spark/mockCustInfo.json")

json.registerTempTable("cust_info")

##Testing...

#print json.printSchema()

#sqlTest = sqlContext.sql("SELECT * FROM cust_info")

#print sqlTest.show()

##...

##Loading

sqlContext.sql("CREATE TABLE cust_info_test AS SELECT * FROM cust_info")

##Extracting

newTable = sqlContext.sql("Select * from cust_info_test")

##Verifying

#print newTable.show(5)