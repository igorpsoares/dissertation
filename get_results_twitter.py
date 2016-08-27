#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This script makes use of data stored on SQLite 
# by twitter_crawler.py to calculate and output the Retweet
# Ratio, Interactor Ratio and the Social Networking Potential.

import sqlite3

if __name__ == '__main__':

    database_folder = '/home/igor/Documents/KCL/Dissertation/code/database/'
    accounts = ['aosfatos', 'BudgITng', 'openspending', 'OKFN']

    for account in accounts:

        conn = sqlite3.connect(database_folder + 'dissertation_' + account + '.db')
        cursor = conn.cursor()

        cursor.execute('SELECT max(id) FROM tweets')
        most_recent_id = cursor.fetchone()

        # Get most recent followers and following count
        cursor.execute('SELECT followers_count,following_count FROM tweets where id=?', most_recent_id)
        most_recent_data = cursor.fetchone()
        followers = most_recent_data[0]
        following = most_recent_data[1]

        # Get tweet count
        cursor.execute('select count(*) from tweets')
        tweet_count = cursor.fetchone()[0]

        # Get how many tweets were retweeted
        cursor.execute('select count(*) from tweets where retweet_count>0')
        retweeted = cursor.fetchone()[0]

        # Get the number of individual users who interacted with the account
        cursor.execute('select count(*) from audience')
        unique_users_interaction = cursor.fetchone()[0]

        retweet_ratio = float(retweeted)/float(tweet_count)
        interactor_ratio = float(unique_users_interaction)/float(followers)
        snp = (retweet_ratio + interactor_ratio)/2

        print 'Results for account: ' + account
        print 'Tweets: ' + unicode(tweet_count)
        print 'Followers: ' + unicode(followers)
        print 'Following: ' + unicode(following)
        print 'Retweeted: ' + unicode(retweeted)
        print 'Individual users who interacted: ' + unicode(unique_users_interaction)
        print '------'
        print 'Retweet ratio: ' + '{:.2f}'.format(retweet_ratio * 100) + '%'
        print 'Interactor ratio: ' + '{:.2f}'.format(interactor_ratio * 100) + '%'
        print '------'
        print 'Social Network Potential: ' + '{:.2f}'.format(snp * 100) + '%'
        print '\n'

        
