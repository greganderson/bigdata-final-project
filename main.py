from pyspark.sql import SQLContext

filename = 'output.txt'

sqlContext = SQLContext(sc)
f = sqlContext.jsonFile(filename)

words = set()
words.add('I')
words.add('I\'m')
words.add('my')
words.add('mine')
words.add('we')
words.add('our')
words.add('ours')

# Take a tweet and map the user to a list of points
def split_words(s):
	word_list = s['text'].split(' ')
	return (s['user'].asDict()['screen_name'], map(lambda x: 1 if x in words else 0, word_list))

a = f.map(lambda x: x.asDict())				# Turn JSON output into dictionary
b = a.filter(lambda x: x['text'] != None)	# Filter out non-tweets
c = b.map(split_words)						# Map user to a list of points for each tweet
d = c.map(lambda lst: (lst[0], reduce(lambda x, y: x+y, lst[1])))	# Add up points for each tweet
e = d.reduceByKey(lambda x, y: x+y)			# Gather all of the scores for each user
g = e.filter(lambda x: x[1] > 3)			# Show only those that had more points than 3
