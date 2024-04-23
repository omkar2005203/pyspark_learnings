from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()


df = spark.read.csv(r"C:\Users\omkar\Desktop\pyspark_code\fakefriends.csv",header=True,inferSchema=True)

df.show()

df.createOrReplaceTempView("people")

result = spark.sql("select * from people WHERE age >=13 and age <=19")

result.show()

spark.stop()

