from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark = SparkSession.builder.appName("MostPopHero").getOrCreate()

schema = StructType([StructField("id",IntegerType(),True),StructField("name",StringType(),True)])

names = spark.read.schema(schema).option("sep"," ").csv("file:///C:/Users/omkar/Desktop/pyspark_code/Marvel-names.txt")

lines = spark.read.text("file:///C:/Users/omkar/Desktop/pyspark_code/Marvel-graph.txt")

connnections = lines.withColumn("id",func.split(func.col("value")," ")[0]).withColumn("connections",func.size(func.split(func.col("value")," "))-1).groupBy("id").agg(func.sum("connections").alias("connections"))

mostPopular = connnections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id")==mostPopular[0]).select("name").first()

#minimum connection count

minConnectioncount = connnections.agg(func.min("connections")).first()[0]

minConnection = connnections.filter(func.col("connections")==minConnectioncount)

minConnectionWithName = minConnection.join(names,"id")

print("The following characters have only "+ str(minConnectioncount)+" connections(s):")
minConnectionWithName.select("name").show()

print(mostPopularName[0]+" is the popular superhero with "+str(mostPopular[1])+ " co-appearance ")