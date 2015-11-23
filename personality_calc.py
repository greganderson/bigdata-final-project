from xml_parser import get_categories
from enum import Enum

userRatng = dict()
dtweet = dict()

class pers(Enum):
    na = 1
    ma = 2
    ps = 3
    op = 4
    co = 5
    ex = 6
    ag = 7
    ne = 8
    lst = 9

# The list of all of the available categories
list_of_cats = get_categories()


def update_ratings(category, num):
    for e in pers:
        if(e is not pers.lst):
            try:                   
                increase = num * float(category[e.value][1])
                userRatng[e.name] = increase;
            except ValueError:
                print "Could not parse" + \
                    category[e.value][1] + "as number"

def split_words(s):
    word_list = s.split(' ')
    return word_list

def check_personality(str):
    map(lambda category: check_personality1(str, category, dtweet), list_of_cats)

def check_personality1(str, category, dict_tweet):
    word = str.lower() 
    words = map(lambda x: x.lower(), category[pers.lst.value])

    if word in words and word in dict_tweet:
        update_ratings(category, dict_tweet[word])
        return True;
    else:
        return False;

def rate_tweet(dict_tweet, usr_Ratings):

    global dtweet
    dtweet = dict_tweet
    global userRatng
    userRatng = usr_Ratings

    # Map through all of the words in a single tweet
    map(check_personality, dtweet)
    return userRatng



# User's personality rating
uuu_userRatng = dict([
        ('na', 0), ('ma', 0), ('ps', 0), ('op', 0), \
            ('co', 0), ('ex', 0), ('ag', 0), ('ne', 0)]) # Shoud come from Greg's code

#tweet = "I am a fake tweet. She went swimming on the lake, we went swimming on the lake"
tweet = "today was a fun day"

tttweet = tweet.lower().split(' ')

# Tweet should come in, in the form of (word, word_count)
ddd_dtweet = dict.fromkeys(tttweet, 1) # Should come from Gregs code

print rate_tweet(ddd_dtweet, uuu_userRatng)
