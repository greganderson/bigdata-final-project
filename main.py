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
	return (s['place'].asDict()['bounding_box'].asDict()['coordinates'][0][0], result_d)


a = f.map(lambda x: x.asDict())				# Turn JSON output into dictionary
b = a.filter(lambda x: x['text'] != None)		# Filter out non-tweets


### 
### TODO This code just needs to be repeated for each category.  Not sure how to do that yet though
###	

# Compute the rating for an individual tweet
loc_n_text =  map_to_count(b.first())
loc_n_text = b.map(map_to_count)

print (loc_n_text[0], rate_tweet(loc_n_text[1]))

