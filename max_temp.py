from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("maxTemp")
sc = SparkContext(conf = conf)


def process(line):
    field = line.split(",")
    station_id = field[0]
    entry_type = field[2]
    temperature = float(field[3])*0.1*(9.0/5.0)+32.0

    return (station_id,entry_type,temperature)


data = sc.textFile("file:///C:/Users/omkar/Desktop/pyspark_code/1800.csv")


parsedlines = data.map(process)

maxTemp = parsedlines.filter(lambda x :"TMAX" in x[1])

# # station and temp 

station_temp = maxTemp.map(lambda x: (x[0],x[2]))

final_max_temp = station_temp.reduceByKey(lambda x,y:max(x,y))

for i in final_max_temp.collect():
    print(i)


