from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("wordCount")
sc = SparkContext(conf = conf)

def normalize(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())


input = sc.textFile("file:///C:/Users/omkar/Desktop/pyspark_code/book.txt")

# words = input.flatMap(lambda x:x.split())

words = input.flatMap(normalize)
# word_count = words.countByValue()
word_count = words.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
word_count_sorted = word_count.map(lambda x:(x[1],x[0])).sortByKey()

results = word_count_sorted.collect()


print(word_count.collect())

print(word_count_sorted.collect())


# print(word_count)

# for word,count in word_count.items():
#     cleanword = word.encode('ascii','ignore')
#     if (cleanword):
#         print (cleanword ,count)

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii','ignore')
    if word:
        print(word," :count ",count)