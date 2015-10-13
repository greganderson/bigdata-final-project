# bigdata-final-project
Final project for my Big Data Computer Systems class.

Our project aims to determine a user's personality based on the contents of
Tweets made by them. We will use a combination of Big Five and Dark Triad
personality traits along with a scoring system to categorize users by their
Tweets. Our project will utilize third party libraries for Python such as Pandas
with the possibility of NLP and machine learning libraries if need be.

---------
### Goals

1. Determine relationship between personality and keywords from Tweets.
2. Determine scoring system for rating personality.
3. Display results in readable manner.

------------------
### Specifications

The purpose of this project is to determine personality traits based on Tweets
made by a given user. This will require us to identify keywords that relate to
certain personality traits. For personality traits, we will use those identified
as the Big Five in psychological study along with traits known as Dark Triad.
Together, these traits are:  Machiavellianism, narcissism, openness,
conscientiousness, extraversion, agreeableness, and neuroticism. 

In addition to the above, we will need to design a scoring system to be able to
categorize keywords contained in Tweets with the personality traits previously
described. Beyond analyzing keywords we will also look at the frequency at which
a user Tweets. This will further help to determine certain traits such as
narcissism.

To implement these goals, we will need to parse the Twitter data into a format
that will be useful for Spark. From there we will need to apply our scoring
algorithm to the parsed data. We will be using the Pandas library in Python for
data analysis. We have not yet determined if this project requires any machine
learning or NLP but in the event that it does we have identified SciPy and
Natural Language Toolkit to help facilitate those concerns. 

Previous research has been conducted on this topic which we will use to help
determine how accurate our own findings are. 


----------
## Authors
Greg Anderson, Umair Naveed, Jesus Zarate
