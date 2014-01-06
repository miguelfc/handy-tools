#!/usr/bin/python

# This script is based on the example of the TwitterSearch Package, 0.78.3
# It extracts tweets with some keywords from the public twitter API.
# It is limited by your own account limit, and it tries to extract tweets
# until the twitter limit is reached.

# Remember that the twitter API has limits in the number of tweets extracted
# in a period of time, so keep that in mind and try not to abuse their TOS.

from TwitterSearch import *
import json, yaml, os

try:
    stream = file(os.path.dirname(os.path.realpath(__file__))+'/extract.conf', 'r')
    configuration = yaml.load(stream);

    try:
        with open (configuration['nextIdFile'], "r") as fnext_max_id:
            next_max_id=fnext_max_id.readlines()
            fnext_max_id.close();
    except IOError:
        next_max_id = [0];
        next_max_id[0] = "";
            
    tso = TwitterSearchOrder() 
    tso.setKeywords(configuration['keywords']) 
    tso.setLanguage(configuration['language'])
    tso.setCount(configuration['tweetsPerCall'])
    
    if next_max_id[0] != "":
        tso.setMaxID(int(next_max_id[0]));
    tso.setIncludeEntities(False) 
        
    ts = TwitterSearch(
        consumer_key = configuration['consumerKey'],
        consumer_secret = configuration['consumerSecret'],
        access_token = configuration['accessToken'],
        access_token_secret = configuration['accessTokenSecret']
     )

    
    # init variables needed in loop
    todo = True
    next_max_id = 0

    # let's start the action
    while(todo):

        # first query the Twitter API
        response = ts.searchTweets(tso)

        # print rate limiting status
        print "Current rate-limiting status: %s" % ts.getMetadata()['x-rate-limit-limit']

        # check if there are statuses returned and whether we still have work to do
        todo = not len(response['content']['statuses']) == 0

        # check all tweets according to their ID
        ftweets = open(configuration['outputFile'], 'a');
        
        for tweet in response['content']['statuses']:
            tweet_id = tweet['id']
            print("Seen tweet with ID %i" % tweet_id)

            # Uncomment only if you need to interrupt the extraction 
            # of tweet older than a timestamp
            if tweet_id  <= int(configuration['oldestTweetTimestamp']):
                exit();
            
            json.dump(tweet, ftweets);
            ftweets.write(",\n");
            
            # current ID is lower than current next_max_id?
            if (tweet_id < next_max_id) or (next_max_id == 0):
                next_max_id = tweet_id
                next_max_id -= 1 # decrement to avoid seeing this tweet again
        
        ftweets.close();
        
        # set lowest ID as MaxID
        tso.setMaxID(next_max_id)
        fnext_max_id = open(configuration['nextIdFile'], 'w');
        fnext_max_id.write(repr(next_max_id));
        fnext_max_id.close();
        

    #print(ts.searchTweets(tso));

except TwitterSearchException as e: 
    print(e)
