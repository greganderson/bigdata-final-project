from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from personality_calc import rate_tweet

filename = 'output.txt'

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)
f = sqlContext.jsonFile(filename)


def map_to_count(s):
	word_list = s['text'].split(' ')
	result_d = dict([])
	for word in word_list:
		word = word.lower()
		if(word in result_d):
			result_d[word] += 1
		else:
			result_d[word] = 1
	# TODO: Is this the best way to get the coordinates?
	#return (s['coordinates'], result_d)
	return (s['coordinates'].asDict()['coordinates'], result_d)
	#return (s['place'].asDict()['bounding_box'].asDict()['coordinates'][0][0], result_d)


def toCSVLine(data):
	return str(a[0][0]) + ',' + str(a[0][1]) + ',' + ''.join(str(d) + ',' + str(e) for d,e in a[1].items)
	#return ','.join(str(d) for d in data)



a = f.map(lambda x: x.asDict())				# Turn JSON output into dictionary
b = a.filter(lambda x: x['text'] != None and x['coordinates'] != None)		# Filter out non-tweets


# Store {location : tweet text} for all tweets
loc_n_text = b.map(map_to_count)

# Store {location : score} for all tweets
loc_n_score = loc_n_text.map(lambda x: (x[0], rate_tweet(x[1])))

lines = loc_n_score.map(toCSVLine)
lines.saveAsTextFile('BAM.csv')
