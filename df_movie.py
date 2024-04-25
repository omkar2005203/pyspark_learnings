from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,LongType

spark = SparkSession.builder.appName("popularMovie").getOrCreate()

schema = StructType([StructField("user_id",IntegerType(),True),StructField("movie_id",IntegerType(),True),StructField("rating",LongType(),True)])

df = spark.read.option("sep","\t").schema(schema).csv("file:///C:/Users/omkar/Desktop/pyspark_code/ml-100k/u.data")

topMovie = df.groupBy("movie_id").count().orderBy(func.desc("count"))

topMovie.show(10)

spark.stop()