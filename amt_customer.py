from pyspark import SparkConf,SparkContext
from operator import add
conf = SparkConf().setMaster("local").setAppName("customer")
sc = SparkContext(conf = conf)


def customer_data(data):
    field = data.split(",")
    customer_id = int(field[0])
    product_id = field[1]
    amount = float(field[2])

    # return (customer_id,product_id,amount)
    return (customer_id,amount)



data = sc.textFile("file:///C:/Users/omkar/Desktop/pyspark_code/customer-orders.csv")

parsed_data = data.map(customer_data)

customer_spend = parsed_data.reduceByKey(lambda x,y:x+y)

# flip key and value
flip = customer_spend.map(lambda x:(x[1],x[0]))

customer_sorted = flip.sortByKey()

print(customer_spend.collect())


print("\n")

print(customer_sorted.collect())