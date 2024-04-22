from pyspark import SparkConf , SparkContext
import collections




#stop existing running context
#sc.stop()

#spark context creation
conf = SparkConf().setMaster("local").setAppName("p")

sc = SparkContext(conf = conf)

#file reading
lines = sc.textFile("file:///C:/Users/omkar/Desktop/pyspark_code/ml-100k/u.data")

#actions that can be performed
elements = lines.collect()

#print(elements)

ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))