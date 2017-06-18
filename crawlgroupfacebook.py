#**********************************************************************
# Crawler Group Facebook: posts.csv(feed), Reactions.csv, comment.csv, share.csv 
# posts.csv
# - p_id
# - p_message
# - p_created_time
# comments.csv
# - p_id
# - parent_cid
# - cid
# - c_message
# - c_created_time
# - uid','uname
# reactions.csv
# - p_id
# - cid
# - type
# - uid
# - uname
# shares.csv
# - p_id
# - mgs
# - uid
# - uname
#
#* June, 18, 2017
#*
#* This lib was written by thangdn
#* Contact:thangdn.tlu@outlook.com
#*
#* Every comment would be appreciated.
#*
#* If you want to use parts of any code of mine:
#* let me know and
#* use it!
#**********************************************************************/
import time, os, sys, datetime
import requests
import json
import facebook
import unicodecsv as csv
from multiprocessing import Pool

group_id = '1098837086877710'
access_token = ''


graph = facebook.GraphAPI(access_token, version='2.7')

#create post csv
writerpost = csv.writer(open('posts.csv', 'ab+'), encoding='utf-8')
writerpost.writerow(['p_id','p_message','p_created_time'])

#create comment_post csv
writercomment = csv.writer(open('comments.csv', 'ab+'), encoding='utf-8')
writercomment.writerow(['p_id','parent_cid','cid','c_message','c_created_time','uid','uname'])

#create reaction csv
writerreaction = csv.writer(open('reactions.csv', 'ab+'), encoding='utf-8')
writerreaction.writerow(['p_id','cid','type','uid','uname'])

#create share.csv csv
writershare = csv.writer(open('shares.csv', 'ab+'), encoding='utf-8')
writershare.writerow(['p_id','mgs','uid','uname'])

#get posts

posts = graph.get_connections(group_id, "feed")

#get post's reactions
def getreactionsPost(p):
    reacpost = graph.get_connections(p['id'], "reactions")
    results = []
    while (True):
        if (len(reacpost['data']) == 0):
            break
        for re in reacpost['data']:
            results.append(
                [p['id'], 'x', re['type'], re['id'], re['name']])
        try:
            reacpost = requests.get(reacpost['paging']['next']).json()
        except:
            break
    return results
# get post's share
def getsharePost(p):
    sharepost = graph.get_connections(p['id'], "share")
    while (True):
        if (len(sharepost['data']) == 0):
            break
        for re in sharepost['data']:
            writerreaction.writerow(
                [p['id'], 'x', re['type'], re['id'], re['name']])
        try:
            sharepost = requests.get(sharepost['paging']['next']).json()
        except:
            break
def getreactionReplyCommnet(p,c,r):
    resultreaction=[]
    reaccomment = graph.get_connections(r['id'], "reactions")
    while (True):
        if (len(reaccomment['data']) == 0):
            break
        for rre in reaccomment['data']:
            resultreaction.append(
                [p['id'], c['id'], rre['type'], rre['id'], rre['name']])
        try:
            reaccomment = requests.get(reaccomment['paging']['next']).json()
        except:
            break
    return resultreaction

def getreplyComment(p,c):
    resultcomment=[]
    resultreaction=[]
    rcomment = graph.get_connections(c['id'], "comments")
    while (True):
        if (len(rcomment['data']) == 0):
            break
        for r in rcomment['data']:
            resultcomment.append(
                [p['id'], c['id'], r['id'], r['message'], r['created_time'], r['from']['id'],
                 r['from']['name']])
            # get reply comment's reactions
            resultreaction.extend(getreactionReplyCommnet(p,c, r))
        try:
            rcomment = requests.get(rcomment['paging']['next']).json()
        except:
            break
    return (resultcomment,resultreaction)

def getreactionComment(p,c):
    # get comment's reactions
    resultreaction = []
    reaccomment = graph.get_connections(c['id'], "reactions")
    while (True):
        if (len(reaccomment['data']) == 0):
            break
        for cre in reaccomment['data']:
            resultreaction.append(
                [p['id'], c['id'], cre['type'], cre['id'], cre['name']])
        try:
            reaccomment = requests.get(reaccomment['paging']['next']).json()
        except:
            break
    return resultreaction

def getcommentPost(p):
    resultcomment=[]
    resultreaction=[]
    com = graph.get_connections(p['id'], "comments")
    while (True):
        if (len(com['data']) == 0):
            break
        for c in com['data']:
            resultcomment.append(
                [p['id'], 'x', c['id'], c['message'], c['created_time'], c['from']['id'], c['from']['name']])
            # get comment's reactions
            resultreaction.extend(getreactionComment(p,c))
            # get reply comment
            kq = getreplyComment(p, c)
            resultcomment.extend(kq[0])
            resultreaction.extend(kq[1])
        try:
            com = requests.get(com['paging']['next']).json()
        except:
            break
    return (resultcomment,resultreaction)
def getPost(p):
    try:
        return [p['id'], p['message'], p['updated_time']]
    except:
        try:
            return [p['id'], p['story'], p['updated_time']]
        except:
            return [p['id'], 'x', p['updated_time']]

i = 0
while(True):
    i=i+len(posts['data'])
    if (len(posts['data']) == 0):
        break
    print(i)
    with Pool(4) as pool:
        resultspost=pool.map(getPost,posts['data'])
        resultReaction = pool.map(getreactionsPost, posts['data'])
        kq=pool.map(getcommentPost, posts['data'])
        pool.close()
        pool.join()
        for p in resultspost:
            writerpost.writerow(p)
        for p in resultReaction:
            for r in p:
                writerreaction.writerow(r)
        for (c,r) in kq:
            for ci in c:
                writercomment.writerow(ci)
            for ri in r:
                writerreaction.writerow(ri)

    try:
        posts = requests.get(posts['paging']['next']).json()
    except:
        break
