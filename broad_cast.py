from pyspark.sql import SparkSession
from pyspark.sql.functions import col,broadcast

spark = SparkSession.builder.appName("test").getOrCreate()

data = [("tom",23),("kom",34),("lol",45),("lim",34)]

df = spark.createDataFrame(data,["Name","age"])

broadcast_var = spark.sparkContext.broadcast(['tom','lim'])

filtered_df = df.filter(col("name").isin(broadcast_var.value))

filtered_df.show()

spark.stop()