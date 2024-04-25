from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,LongType
import codecs

def loadMovieNames():
    movieNames = {}

    with codecs.open("C:/Users/omkar/Desktop/pyspark_code/ml-100k/u.ITEM","r",encoding='ISO-8859-1',errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
        return movieNames


spark = SparkSession.builder.appName("popularMovie").getOrCreate()

#broadcast object
nameDict = spark.sparkContext.broadcast(loadMovieNames())


schema = StructType([StructField("user_id",IntegerType(),True),StructField("movie_id",IntegerType(),True),StructField("rating",LongType(),True)])

df = spark.read.option("sep","\t").schema(schema).csv("file:///C:/Users/omkar/Desktop/pyspark_code/ml-100k/u.data")

topMovie = df.groupBy("movie_id").count()


def NameLookUp(movieID):
    return nameDict.value[movieID]

#calling user defined function to look up for movie names based on movie id key
lookupNameUDF = func.udf(NameLookUp)

#movies with names

movieWithNames = topMovie.withColumn("movie_title",lookupNameUDF(func.col("movie_id")))

#sorted

sorted_movie = movieWithNames.orderBy(func.desc("count"))

sorted_movie.show(10,False)

spark.stop()