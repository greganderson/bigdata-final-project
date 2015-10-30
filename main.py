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

def split_words(s):
	word_list = s.split(' ')
	return map(lambda x: 1 if x in words else 0, word_list)

a = f.map(lambda x: x.asDict()['text'])
b = a.filter(lambda x: x != None)
c = b.map(split_words)
d = c.map(lambda lst: reduce(lambda x, y: x+y, lst))

