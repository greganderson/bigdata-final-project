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

# List of categories
narcissism = set(['i', 'i\'m', 'me', 'my', 'mine', 'we', 'our', 'ours', 'we\'re'])
neuroticism = set(['scared', 'worried', 'frustrated'])
agreeableness = set(['agree', 'agreed', 'love', 'feel', 'i thought of you', 'you have my sympathy', 'i feel for you', 'donated'])
extraversion = set(['party', 'going out', 'people'])
openness_to_experience = set(['new', 'experience'])
conscientiousness = set(['vigilant', 'careful', 'efficient', 'organize', 'organized', 'achieve', 'achievement', 'award', 'plan', 'planned', 'depend', 'dependable'])
machiavellianism = set(['i don\'t care', 'your fault', 'mine', 'who cares', 'dui', 'dwi', 'prison', 'jail', 'illegal'])
psycopathy = set(['Crazy', 'not my fault', 'no remorse', 'crime', 'irresponsible'])

categories = [narcissism, neuroticism, agreeableness, extraversion, openness_to_experience, conscientiousness, psycopathy]

# Take a tweet and map the user to a list of points
def split_words(s, words):
	word_list = s['text'].split(' ')
	return (s['user'].asDict()['screen_name'], map(lambda x: 1 if x.lower() in words else 0, word_list))

def map_to_count(s):
	word_list = s['text'].split(' ')
	result_d = dict([])
	for word in word_list:
		word = word.lower()
		if(word in result_d):
			result_d[word] += 1
		else:
			result_d[word] = 1
	return (s['coordinates'], result_d)


a = f.map(lambda x: x.asDict())				# Turn JSON output into dictionary
b = a.filter(lambda x: x['text'] != None)		# Filter out non-tweets


# Compute the rating for an individual tweet
loc_n_text =  map_to_count(b.first())

print (loc_n_text[0], rate_tweet(loc_n_text[1]))


### 
### TODO This code just needs to be repeated for each category.  Not sure how to do that yet though
###	

# Narcissism
c = b.map(lambda x: split_words(x, narcissism))	# Map user to a list of points for each tweet
d = c.map(lambda lst: (lst[0], reduce(lambda x, y: x+y, lst[1])))	# Add up points for each tweet
e = d.reduceByKey(lambda x, y: x+y)				# Gather all of the scores for each user
g = e.filter(lambda x: x[1] > 3)				# Show only those that had more points than 3




### In the works
for category in categories:
	c = b.map(lambda x: split_words(x, categories))	# Map user to a list of points for each tweet
	d = c.map(lambda lst: (lst[0], reduce(lambda x, y: x+y, lst[1])))	# Add up points for each tweet
	e = d.reduceByKey(lambda x, y: x+y)				# Gather all of the scores for each user
	g = e.filter(lambda x: x[1] > 3)				# Show only those that had more points than 3
	
