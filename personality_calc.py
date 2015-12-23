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
#list_of_cats = get_categories()
list_of_cats = None


def update_ratings(category, num):
    for e in pers:
        if(e is not pers.lst):
            try:                   
                increase = num * float(category[e.value][1])
                userRatng[e.name] = increase
            except ValueError:
                print "Could not parse" + \
                    category[e.value][1] + "as number"

def check_personality(string):
    map(lambda category: check_personality1(string, category, dtweet), list_of_cats)

def check_personality1(string, category, dict_tweet):
    word = string.lower() 
    words = map(lambda x: x.lower(), category[pers.lst.value])

    if word in words and word in dict_tweet:
        update_ratings(category, dict_tweet[word])
        return True
    else:
        return False

def rate_tweet_w_usr_rating(dict_tweet, usr_Ratings):

    global dtweet
    dtweet = dict_tweet
    global userRatng
    userRatng = usr_Ratings

    # Map through all of the words in a single tweet
    map(check_personality, dtweet)
    return userRatng

def rate_tweet(dict_tweet, categories):
    global list_of_cats
    list_of_cats = categories
    rating = dict([
            ('na', 0), ('ma', 0), ('ps', 0), ('op', 0), \
                ('co', 0), ('ex', 0), ('ag', 0), ('ne', 0)])
    return rate_tweet_w_usr_rating(dict_tweet, rating)
