from pyspark.sql import SQLContext

filename = 'output.txt'

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

# Take a tweet and map the user to a list of points
def split_words(s, words):
	word_list = s['text'].split(' ')
	return (s['user'].asDict()['screen_name'], map(lambda x: 1 if x.lower() in words else 0, word_list))

a = f.map(lambda x: x.asDict())					# Turn JSON output into dictionary
b = a.filter(lambda x: x['text'] != None)		# Filter out non-tweets

### 
### TODO This code just needs to be repeated for each category.  Not sure how to do that yet though
###	

# Narcissism
c = b.map(lambda x: split_words(x, narcissism))	# Map user to a list of points for each tweet
d = c.map(lambda lst: (lst[0], reduce(lambda x, y: x+y, lst[1])))	# Add up points for each tweet
e = d.reduceByKey(lambda x, y: x+y)				# Gather all of the scores for each user
g = e.filter(lambda x: x[1] > 0)				# Show only those that had more points than 3
