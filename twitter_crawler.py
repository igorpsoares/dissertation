#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This script harvests tweets from a list of
# predefined accounts gathering the following
# statistics: number of retweets, number of favourites,
# number of followers, number of following accounts and
# individual followers.

import tweepy
import json
import sqlite3
import logging
from tweepy import OAuthHandler
from time import sleep

def get_replies_and_mentions(cursor, query):
    ''' Search Twitter API for @initiative related tweets '''

    max_tweets = 1000
    replies_and_mentions_list = []
    replies_mentions = []
    users = []

    cursor.execute('SELECT max(id) FROM mentions')
    last_tweet = cursor.fetchone()[0]

    if last_tweet:
        logging.info('Getting mentions for ' + query + ' since tweet id=' + last_tweet)
    else:
        logging.info('Getting mentions for ' + query)

    for status in tweepy.Cursor(api.search, since_id = last_tweet, q=query).items(max_tweets):

        tweet = status._json
        parsed_json = json.loads(json.dumps(tweet))
        replies_and_mentions_list.append(parsed_json)

        text = status.text
        id_str = parsed_json['id_str']
        logging.info('Getting mention id=' + id_str)
        date_time = parsed_json['created_at']
        retweet_count = parsed_json['retweet_count']
        favorite_count = parsed_json['favorite_count']
        user_id = parsed_json['user']['id_str']
        followers_count = parsed_json['user']['followers_count']
        following_count = parsed_json['user']['friends_count']
        in_reply_to = parsed_json['in_reply_to_status_id']

        mentions_fields = (id_str, text, date_time, retweet_count, favorite_count, in_reply_to, followers_count, following_count)
        replies_mentions.append(mentions_fields)

        user_fields = (user_id, followers_count, following_count)
        users.append(user_fields)

    logging.info('Saving mentions to database')
    cursor.executemany('INSERT OR IGNORE INTO mentions VALUES (?,?,?,?,?,?,?,?)', replies_mentions)
    cursor.executemany('INSERT OR IGNORE INTO audience VALUES (?,?,?)', users)

    return (replies_and_mentions_list)
    
    
def get_reply_count(tweet_id, replies_and_mentions_list):
    ''' Count how many replies were found on the replies and mentions list '''

    reply_count = 0

    for tweet in replies_and_mentions_list:
        
        if tweet['in_reply_to_status_id'] == tweet_id:
            reply_count += 1

    return reply_count

def get_retweeters(tweet_id, cursor):
    ''' Get the retweeters from a given tweet and save their info to the database '''

    logging.info('Getting retweets from tweet ' + tweet_id)

    retweets = api.retweets(tweet_id)

    retweeters = []
    for retweet in retweets:
        retweet_data = (retweet.user.id_str, retweet.user.followers_count, retweet.user.friends_count)
        retweeters.append(retweet_data)

    cursor.executemany('INSERT OR IGNORE INTO audience VALUES (?,?,?) ', retweeters)

    return

if __name__ == '__main__':

    # logger settings
    log_file = '/home/igor/Documents/KCL/Dissertation/code/twitter.log'
    logging.basicConfig(filename=log_file, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO)
        
    logging.info('Execution started')

    accounts = ['aosfatos', 'BudgITng', 'openspending', 'OKFN']
    loop_count = 0
    pause_loop_max = 50

    consumer_key = ''
    consumer_secret = ''
    access_token = ''
    access_secret = ''
 
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)

    # Loop initiatives
    for account in accounts:

        conn = sqlite3.connect('/home/igor/Documents/KCL/Dissertation/code/database/dissertation_' + account + '.db')
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS tweets (id TEXT PRIMARY KEY, message TEXT, date_time TEXT, retweet_count INTEGER, favorite_count INTEGER, reply_count INTEGER, followers_count INTEGER, following_count INTEGER)')
        cursor.execute('CREATE TABLE IF NOT EXISTS mentions (id TEXT PRIMARY KEY, message TEXT, date_time TEXT, retweet_count INTEGER, favorite_count INTEGER, in_reply_to TEXT, followers_count INTEGER, following_count INTEGER)')
        cursor.execute('CREATE TABLE IF NOT EXISTS audience (user_id TEXT PRIMARY KEY, followers_count INTEGER, following_count INTEGER)')
        cursor.execute('SELECT max(id) FROM tweets')
        last_tweet = cursor.fetchone()[0]

        if last_tweet:
            logging.info('Getting tweets from @' + account + ' since tweet id=' + last_tweet)
        else:
            logging.info('Getting tweets from @' + account)

        tweets = []

        replies_and_mentions_list = get_replies_and_mentions(cursor, '@' + account)

        for status in tweepy.Cursor(api.user_timeline, since_id = last_tweet, id=account).items(1000):

            # Process a single status
            #process_or_store(status._json)
            tweet = status._json
            parsed_json = json.loads(json.dumps(tweet))

            text = status.text
            id_str = parsed_json['id_str']
            logging.info('Getting status id=' + id_str)

            date_time = parsed_json['created_at']
            retweet_count = parsed_json['retweet_count']
            favorite_count = parsed_json['favorite_count']
            followers_count = parsed_json['user']['followers_count']
            following_count = parsed_json['user']['friends_count']
            reply_count = get_reply_count(id_str, replies_and_mentions_list)
            
            # Get the tweet retweeters
            get_retweeters(id_str, cursor)

            print 'ID: ' + id_str
            print text
            print 'Date and time: ' + date_time
            print 'Retweets: ' + unicode(retweet_count)
            print 'Favourites: ' + unicode(favorite_count)
            print 'Replies: ' + unicode(reply_count)
            print 'Followers: ' + unicode(followers_count)
            print 'Following: ' + unicode(following_count)
            print '\n'

            tweet_fields = (id_str, text, date_time, retweet_count, favorite_count, reply_count, followers_count, following_count)
            tweets.append(tweet_fields)

            loop_count += 1

            if loop_count > pause_loop_max:
                loop_count = 0
                logging.info('Sleeping...')
                sleep(60 * 15)

        # Database
        logging.info('Saving initiative tweets to database')
        cursor.executemany('INSERT OR IGNORE INTO tweets VALUES (?,?,?,?,?,?,?,?)', tweets)
        conn.commit()
        conn.close()
        logging.info('Done for @' + account)

