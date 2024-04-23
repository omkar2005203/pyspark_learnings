from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType

spark = SparkSession.builder.appName("MinTempDf").getOrCreate()

schema = StructType([StructField("station_id",StringType(),True),StructField("date",IntegerType(),True),StructField("measure_type",StringType(),True),StructField("temperature",FloatType(),True)])

df = spark.read.schema(schema).csv("file:///C:/Users/omkar/Desktop/pyspark_code/1800.csv")

df.printSchema()

# filter for TMIN

minTemp = df.filter(df.measure_type == "TMIN")

stationTemp = minTemp.select("station_id","temperature")

minTempStation = stationTemp.groupBy("station_id").min("temperature")

minTempStation.show()


# Convert temperature to fahrenheit and sort the dataset
minTempsByStationF = minTempStation.withColumn("temperature",
                                                  func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                                                  .select("station_id", "temperature").sort("temperature")
                                                  
# Collect, format, and print the results
results = minTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))


spark.stop()