#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This script makes use of data stored on SQLite 
# by facebook_crawler.py to calculate and output the Share and
# Comment Ratio, Interactor Ratio and the Social Networking Potential.

import sqlite3

if __name__ == '__main__':

    # SQLite database folder and account list
    # Example:
    # database_folder = '/home/igor/devel/crawler/'
    # accounts = ['aosfatos.org', 'budgitng', 'OpenSpending', 'OKFNetwork']
    database_folder = ''
    accounts = ['']

    for account in accounts:

        conn = sqlite3.connect(database_folder + 'dissertation_' + account + '_fb.db')
        cursor = conn.cursor()

        cursor.execute('SELECT max(date_time) FROM page_stats')
        most_recent_dt = cursor.fetchone()

        # Get most recent fan and storyteller stats
        cursor.execute('SELECT fan_count,storytellers_count FROM page_stats where date_time=?', most_recent_dt)
        most_recent_data = cursor.fetchone()
        fan_count = most_recent_data[0]
        storytellers_count = most_recent_data[1]

        # Get posts count
        cursor.execute('select count(*) from posts')
        post_count = cursor.fetchone()[0]

        # Get how many posts were shared or commented on
        cursor.execute('select count(*) from posts where share_count > 0 or comment_count > 0')
        interaction_count = cursor.fetchone()[0]

        # Calculate engagement ratios
        shared_comment_ratio = float(interaction_count)/float(post_count)
        interactor_ratio = float(storytellers_count)/float(fan_count)
        snp = (shared_comment_ratio + interactor_ratio)/2

        print 'Results for account: ' + account
        print 'Posts: ' + unicode(post_count)
        print 'Fans: ' + unicode(fan_count)
        print 'Interacted: ' + unicode(interaction_count)
        print 'Storytellers: ' + unicode(storytellers_count)
        print '------'
        print 'Share and Comment ratio: ' + '{:.2f}'.format(shared_comment_ratio * 100) + '%'
        print 'Interactor ratio: ' + '{:.2f}'.format(interactor_ratio * 100) + '%'
        print '------'
        print 'Social Network Potential: ' + '{:.2f}'.format(snp * 100) + '%'
        print '\n'

        
