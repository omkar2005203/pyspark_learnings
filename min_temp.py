from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("minTemp")
sc = SparkContext(conf = conf)


def process(line):
    fields = line.split(",")
    station_id = fields[0]
    entryType = fields[2]
    #converting temperature to degree degree farhenheit
    temperature = float(fields[3])*0.1*(9.0/5.0)+32.0
    
    # return tuple (station id,entry type , temperature)
    return (station_id,entryType,temperature)

lines = sc.textFile("file:///C:/Users/omkar/Desktop/pyspark_code/1800.csv")

parsedline = lines.map(process)

minTemps = parsedline.filter(lambda x:"TMIN" in x[1])

#create station id and temperature pair
stationTemp = minTemps.map(lambda x:(x[0],x[2]))

final_min_val = stationTemp.reduceByKey(lambda x,y:min(x,y))

for i in final_min_val.collect():
    print(i)