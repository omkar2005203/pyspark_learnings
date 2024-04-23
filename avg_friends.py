from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func


#create a sparksession object

spark = SparkSession.builder.appName("test_new_one_i").getOrCreate()


my_frame = spark.read.option("header","true").option("inferSchema","true").csv("file:///C:/Users/omkar/Desktop/pyspark_code/fakefriends-header.csv")

new_frame = my_frame.select("age","friends")

process_frame = new_frame.groupBy("age").avg("friends")

process_frame.show()

#sorted

new_frame.groupBy("age").avg("friends").sort("age").show()



# decimal formatting

new_frame.groupBy("age").agg(func.round(func.avg("friends"),2)).sort("age").show()

#alias 

new_frame.groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show()

spark.stop()