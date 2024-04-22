'''
concept of key/value RDD's . The average friends by age.
'''

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("age_avg")
sc = SparkContext(conf = conf)

def parseline(line):
    fields = line.split(",")
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age,num_friends)


lines = sc.textFile("file:///C:/Users/omkar/Desktop/pyspark_code/fakefriends.csv")

rdd = lines.map(parseline)

totalByAge = rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))

averageByAge = totalByAge.mapValues(lambda x:x[0]/x[1])

data = averageByAge.collect()

for i in data:
    print(i)