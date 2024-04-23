from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType

spark = SparkSession.builder.appName("totalcustSpend").getOrCreate()

schema = StructType([StructField("customer_id",IntegerType(),True),StructField("product_id",IntegerType(),True),StructField("price",FloatType(),True)])

df = spark.read.schema(schema).csv("file:///C:/Users/omkar/Desktop/pyspark_code/customer-orders.csv")

df.printSchema()

customer_data = df.select("customer_id","price")

total_spend_df = customer_data.groupBy("customer_id").agg(func.round(func.sum("price"),2).alias("total_spent"))

total_spend_df_sorted = total_spend_df.sort("total_spent")

total_spend_df_sorted.show(total_spend_df_sorted.count())

spark.stop()