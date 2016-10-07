#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This script harvests posts from a list of
# predefined Facebook accounts gathering the following
# statistics: number of shares, number of likes,
# number of comments, number of fans and storytellers.
# Intitially based on a post from "Simple. Beautiful. Data."

import urllib2
import json
import logging
import datetime
import sqlite3

def build_auth_url(graph_url, APP_ID, APP_SECRET):
    ''' Build graph API post url ''' 
 
    post_args = '?key=value&access_token=' + APP_ID + '|' + APP_SECRET
    post_url = graph_url + post_args
 
    return post_url

def render_to_json(graph_url):
    ''' Render graph url call to JSON '''

    web_response = urllib2.urlopen(graph_url)
    readable_page = web_response.read()
    json_data = json.loads(readable_page)
    
    return json_data

def scrape_posts_by_date(graph_url, posts_url, date, post_data, APP_ID, APP_SECRET):
    ''' Get posts data by paging recursively '''

    # Render URL to JSON
    page_posts = render_to_json(posts_url)
    
    # Flag to control whether to stop collecting
    collecting = True

    if 'paging' in page_posts:

        next_page = page_posts['paging']['next']

        # Grab all posts
        page_posts = page_posts['data']
	
        # Iterate over the captured posts
        for post in page_posts:

            # Get the counters
            likes_count = get_likes_count(graph_url, post['id'], APP_ID, APP_SECRET)
            comments_count = get_comments_count(graph_url, post['id'], APP_ID, APP_SECRET)
            shares_count = get_shares_count(graph_url, post['id'], APP_ID, APP_SECRET)

            # Assign post basic data
            try:
                post_id = post['id']
                created_time = post['created_time']
                message = post['message']        
                                
            except Exception:
                post_id = created_time = message = 'error'

            current_post = (post_id, message, created_time, shares_count, likes_count, comments_count)
            if created_time != 'error':

                # Save the post if it is within the time frame specified,
                # otherwise stop collecting
                if date <= created_time:
                    logging.info('Getting post ' + post_id)
                    post_data.append(current_post)
                
                elif date > created_time:
                    logging.info('Done collecting')
                    collecting = False
                    break

    else:
        collecting = False
	
    # Move to next page if there are more posts to get within the time frame
    if collecting == True:
        scrape_posts_by_date(graph_url, next_page, date, post_data, APP_ID, APP_SECRET)
	
    return post_data


def get_counts(graph_url, count_type, post_id, APP_ID, APP_SECRET):
    ''' Generic function to get post counters '''

    args = post_id + '/' + count_type + '?summary=true&key=value&access_token=' + APP_ID + '|' + APP_SECRET
    url = graph_url + args
    counter_json = render_to_json(url)
 
    # get the count number
    try:
        count = counter_json["summary"]["total_count"]
    except:
        count = 0
 
    return count

def get_likes_count(graph_url, post_id, APP_ID, APP_SECRET):
    ''' Get how many likes the post received '''

    count_likes = get_counts(graph_url, 'likes', post_id, APP_ID, APP_SECRET)
 
    return count_likes

def get_comments_count(graph_url, post_id, APP_ID, APP_SECRET):
    ''' Get how many comments the post received '''
    
    count_likes = get_counts(graph_url, 'comments', post_id, APP_ID, APP_SECRET)
 
    return count_likes

def get_shares_count(graph_url, post_id, APP_ID, APP_SECRET):
    ''' Get how many times the post was shared '''
    
    args = '?id=' + post_id + '&fields=shares&access_token=' + APP_ID + '|' + APP_SECRET
    url = graph_url + args
    
    counter_json = render_to_json(url)
 
    
    # Get share count
    try:
        count_shares = counter_json['shares']['count']
    except:
        count_shares = 0

    return count_shares

def get_fan_count(graph_url, page_name, APP_ID, APP_SECRET):
    ''' Get how many fans the given page has '''
    
    args = page_name + '/?fields=fan_count&access_token=' + APP_ID + '|' + APP_SECRET
    url = graph_url + args
    counter_json = render_to_json(url)
 
    # Get fan count
    count_fans = counter_json['fan_count']
 
    return count_fans

def get_page_storytellers(graph_url, page_name, APP_ID, APP_SECRET):
    ''' Get how many storytellers are related to the given post '''

    args = page_name + '/insights/page_storytellers_by_country/days_28/?&access_token=' + APP_ID + '|' + APP_SECRET
    url = graph_url + args
    counter_json = render_to_json(url)
 
    # Get storytellers count by summing the per country stats
    try:
        storytellers_by_country = counter_json['data'][0]['values'][0]['value']
    except:
        storytellers_by_country = {}

    count_storytellers = sum(storytellers_by_country.values())

    return count_storytellers

if __name__ == '__main__':

    # App Secret and App ID
    APP_SECRET = ''
    APP_ID = ''

    # Logger settings
    log_file = '/home/igor/Documents/KCL/Dissertation/code/facebook.log'
    logging.basicConfig(filename=log_file, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO)
        
    logging.info('Execution started')

    # Account list
    # Example:
    # accounts = ['aosfatos.org', 'budgitng', 'OpenSpending', 'OKFNetwork']
    accounts = ['']
    graph_url = 'https://graph.facebook.com/'

    # The time of last weeks crawl
    last_crawl = datetime.datetime.now() - datetime.timedelta(weeks=52)
    last_crawl = last_crawl.isoformat()

    for account in accounts:

        logging.info('Getting data for account ' + account)

        # Database preparation
        conn = sqlite3.connect('/home/igor/Documents/KCL/Dissertation/code/database/dissertation_' + account + '_fb.db')
        conn.execute('CREATE TABLE IF NOT EXISTS posts (id TEXT PRIMARY KEY, message TEXT, created_time TEXT, share_count INTEGER, like_count INTEGER, comment_count INTEGER)')
        conn.execute('CREATE TABLE IF NOT EXISTS page_stats (date_time TEXT, fan_count INTEGER, storytellers_count INTEGER)')
        cursor = conn.cursor()

        # Build graph api url with initiative username
        current_page = build_auth_url(graph_url + account, APP_ID, APP_SECRET)
        
        # Open public page in facebook graph api
        json_fbpage = render_to_json(current_page)
        
        print current_page

        # Gather page level JSON Data
        fan_count = get_fan_count(graph_url, account, APP_ID, APP_SECRET)
        page_storytellers = get_page_storytellers(graph_url, account, APP_ID, APP_SECRET)
        page_stats = (datetime.datetime.today(), fan_count, page_storytellers)

        # Store page stats
        logging.info('Saving page stats to database')
        cursor.execute('INSERT INTO page_stats VALUES (?,?,?)', page_stats)
        conn.commit()
        logging.info(account + ' page stats saved')

        # Extract post data
        posts_url = build_auth_url(graph_url + account + '/posts/', APP_ID, APP_SECRET)
        post_data = []
        post_data = scrape_posts_by_date(graph_url, posts_url, last_crawl, post_data, APP_ID, APP_SECRET)

        # Store post data
        logging.info('Saving posts to database')
        cursor.executemany('INSERT INTO posts VALUES (?,?,?,?,?,?)', post_data)
        conn.commit()
        conn.close()
        logging.info(account + ' posts saved')

    logging.info('Execution finished')


    
