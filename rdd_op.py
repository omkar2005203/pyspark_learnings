from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("age_avg")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///C:/Users/omkar/Desktop/pyspark_code/test.csv")

data = lines.map(lambda x : (x,1))

for i in data.collect():
    print(i)