from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("age_avg")
sc = SparkContext(conf = conf)
'''
RDDs (Resilient Distributed Datasets) support two types of operations: transformations and actions. Transformations are operations on RDDs that produce another RDD, while actions are operations that trigger computation and return a result to the driver program. 

'''

'''
map(func): Applies a function to each element in the RDD and returns a new RDD with the results.

'''
rdd =  sc.parallelize([1,2,3,4,5,6,7,8,9,10])

result = rdd.map(lambda x : x**2)

print(f" map op: {result.collect()}")

result = rdd.filter(lambda x : x > 5)

print(f" filter op: {result.collect()}")

rdd_new = sc.parallelize(["hello world","how is the world doing ?"])

result = rdd_new.flatMap(lambda x : x.split())
print(result.collect())


rdd_grp = sc.parallelize([(1,'a'),(2,'b'),(1,'c'),(2,'d'),(3,'c')])
result = rdd_grp.groupByKey()
print(result.mapValues(list).collect())

# applying mapValues transformation

new_data = sc.parallelize([('alice',30),('alice',35),("sam",45),("tom",56),("kim",55)])
result = new_data.mapValues(lambda x:x+5)
print(result.collect())


sentences_rdd = sc.parallelize([
    "Apache Spark is a unified analytics engine",
    "for big data processing, with built-in modules",
    "for streaming, SQL, machine learning, and graph processing"
])


data = sentences_rdd.map(lambda x:x.split(" "))

print(data.collect())

#applying flatmap
flat_map_data = sentences_rdd.flatMap(lambda x:x.split(" "))
print(flat_map_data.collect())