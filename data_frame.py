from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("new").getOrCreate()

people = spark.read.option("header","true").option("inferSchema","true").csv("file:///C:/Users/omkar/Desktop/pyspark_code/fakefriends-header.csv")

print("Here is our inferred schema:")

people.printSchema()

print("display column name")

people.select("name").show()

print("filter out anyone above 21 age")

people.filter(people.age > 21).show()

print("Group by age :")

people.groupBy(people.age < 21).count().show()

print("make everyone 10 year older :")

people.select(people.name,people.age + 10).show()

spark.stop()

