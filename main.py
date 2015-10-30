from pyspark.sql import SQLContext

filename = 'output.txt'

sqlContext = SQLContext(sc)
f = sqlContext.jsonFile(filename)

a = f.map(lambda x: x.asDict())
b = a.filter(lambda x: x['text'] != None)
