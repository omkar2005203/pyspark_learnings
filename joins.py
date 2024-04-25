from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("joins").getOrCreate()


data1 = [("alice",1),("bob",2),("charlier",3)]
data2 = [("alice","A"),("bob","B"),("David","D")]

df1 = spark.createDataFrame(data1,["name","value1"])
df2 = spark.createDataFrame(data2,["name","value2"])


print("left data frame :")
df1.show()

print("right dataframe :")
df2.show()

#inner join
print("inner join ")
inner_join = df1.join(df2,"name","inner")

inner_join.show()

#outer join
print("outer join")
outer_join = df1.join(df2,"name","outer")
outer_join.show()

#left
print("left join ")
left_join = df1.join(df2,"name","left")
left_join.show()

#right 
print("right join")
right_join = df1.join(df2,"name","right")
right_join.show()


spark.stop()

