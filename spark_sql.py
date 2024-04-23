'''
executing SQL commands and SQL style functions on a dataframe
'''
from pyspark.sql import SparkSession
from pyspark.sql import Row


#create a sparksession object

spark = SparkSession.builder.appName("test_new_one").getOrCreate()

def mapper(line):
    fields = line.split(",")

    return Row(ID=int(fields[0]),name=str(fields[1].encode("utf-8")),age = int(fields[2]),numFriends = int(fields[3]))

#read file
lines = spark.sparkContext.textFile(r"C:\Users\omkar\Desktop\pyspark_code\fakefriends.csv")
people = lines.map(mapper)

#infer schema and register as table
schema_people = spark.createDataFrame(people).cache()
schema_people.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * from people WHERE age >= 13 and age <=19")

for teen in teenagers.collect():
    print(teen)


## use of function instead of sql queries


schema_people.groupBy("age").count().orderBy("age").show()


spark.stop()
