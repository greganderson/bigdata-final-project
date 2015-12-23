from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from personality_calc import rate_tweet
from xml_parser import get_categories 

filename = 'output.txt'

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)
f = sqlContext.jsonFile(filename)


# Get the location and map it to the word count of the tweet
def map_to_count(s):
	word_list = s['text'].split(' ')
	result_d = dict([])
	for word in word_list:
		word = word.lower()
		if(word in result_d):
			result_d[word] += 1
		else:
			result_d[word] = 1
	return (s['place'].asDict()['full_name'], result_d)


# Convert to CSV
def toCSVLine(data):
	try:
		result = str(data[0]) + ',' + ','.join(str(c) for c in data[1].values()) + ',\n'
	except:
		return 'BLAMO'
	return result


a = f.map(lambda x: x.asDict())				# Turn JSON output into dictionary
b = a.filter(lambda x: x['text'] != None and x['place'] != None and x['place'].asDict()['full_name'] != None)		# Filter out non-tweets


# Store {location : tweet text} for all tweets
loc_n_text = b.map(map_to_count)

list_of_cats = get_categories()

# Store {location : score} for all tweets
loc_n_score = loc_n_text.map(lambda x: (x[0], rate_tweet(x[1], list_of_cats)))

lines = loc_n_score.map(toCSVLine)
lines.saveAsTextFile('BAM.csv')
