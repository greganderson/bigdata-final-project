from pyspark.sql import SQLContext

filename = 'output.txt'

sqlContext = SQLContext(sc)
f = sqlContext.jsonFile(filename)

narcissism = set(['i', 'i\'m', 'me', 'my', 'mine', 'we', 'our', 'ours', 'we\'re'])
neuroticism = set(['scared', 'worried', 'frustrated'])

# Take a tweet and map the user to a list of points
def split_words(s, words):
	word_list = s['text'].split(' ')
	return (s['user'].asDict()['screen_name'], map(lambda x: 1 if x.lower() in words else 0, word_list))

a = f.map(lambda x: x.asDict())					# Turn JSON output into dictionary
b = a.filter(lambda x: x['text'] != None)		# Filter out non-tweets

# Narcissism
c = b.map(lambda x: split_words(x, narcissism))	# Map user to a list of points for each tweet
d = c.map(lambda lst: (lst[0], reduce(lambda x, y: x+y, lst[1])))	# Add up points for each tweet
e = d.reduceByKey(lambda x, y: x+y)				# Gather all of the scores for each user
g = e.filter(lambda x: x[1] > 0)				# Show only those that had more points than 3
