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
# tags.csv
# - p_id
# - c_id
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
import facebook
import unicodecsv as csv
from multiprocessing import Pool

group_id = 'YOUR GROUP ID'
access_token = 'YOUR ACCESS TOKEN'

#create post csv
writerpost = csv.writer(open('posts.csv', 'ab+'), encoding='utf-8')
writerpost.writerow(['p_id','type','p_message','p_created_time','p_updated_time','u_id','u_name','no_share'])

#create comment_post csv
writercomment = csv.writer(open('comments.csv', 'ab+'), encoding='utf-8')
writercomment.writerow(['p_id','parent_cid','cid','c_message','c_created_time','uid','uname'])

#create reaction csv
writerreaction = csv.writer(open('reactions.csv', 'ab+'), encoding='utf-8')
writerreaction.writerow(['p_id','cid','type','uid','uname'])

#create tags csv
writertag = csv.writer(open('tags.csv', 'ab+'), encoding='utf-8')
writertag.writerow(['p_id','cid','uid','uname'])

#set limit:
plimit=50
#get posts
#posts = graph.get_connections(group_id, "feed",limit=plimit)
#url = '/feed?fields=id,from,message,type,created_time,updated_time,story,shares,comments.limit('+str(plimit)+'){id,message,from,created_time}&limit='+str(plimit)

#url = 'https://graph.facebook.com/v2.7/'+group_id+'?fields=feed.limit('+str(plimit)+')%7Bid%2Cfrom%2Cmessage%2Ctype%2Ccreated_time%2Cupdated_time%2Cstory%2Cshares%2Creactions%2Ccomments%7Bid%2Cfrom%2Cmessage%2Creactions%2Ccomments%7Bid%2Cfrom%2Cmessage%2Creactions%2Ccreated_time%7D%2Ccreated_time%7D%7D&access_token='+access_token

url = 'https://graph.facebook.com/v2.10/1098837086877710?fields=feed%7Bid%2Ctype%2Cmessage%2Cmessage_tags%2Ccreated_time%2Cupdated_time%2Cfrom%2Cstory%2Cshares%2Creactions%2Ccomments%7Bid%2Cfrom%2Cmessage%2Cmessage_tags%2Ccreated_time%2Creactions%7D%7D&access_token='+access_token
print(url)

posts = requests.get(url).json()['feed']

def getreaction(p,a):
    tempreac = []
    try:
        reaction = a['reactions']
        while (True):
            if (len(reaction['data']) == 0):
                break
            for r in reaction['data']:
                tempreac.append(
                    [p['id'], a['id'], r['type'], r['id'], r['name']])
            try:
                reaction = requests.get(reaction['paging']['next']).json()
            except:
                break
    except:
        tempreac = []
    return tempreac

def gettag(p,a):
    temptag = []
    try:
        tag = a['message_tags']
        while (True):
            if (len(tag) == 0):
                break
            for t in tag:
                temptag.append(
                    [p['id'], a['id'], t['type'], t['id'], t['name']])
            try:
                tag = requests.get(tag['paging']['next']).json()
            except:
                break
    except:
        temptag = []
    return temptag

def getreplyComment(p,pc):
    resultcomment=[]
    resultreaction=[]
    resulttag=[]
    try:
        com = pc['comments']
        while (True):
            if (len(com['data']) == 0):
                break
            for c in com['data']:
                resultcomment.append(
                    [p['id'], pc['id'], c['id'], c['message'], c['created_time'], c['from']['id'], c['from']['name']])
                resultreaction.extend(getreaction(p,c))
                resulttag.extend(gettag(p,c))

            try:
                com = requests.get(com['paging']['next']).json()
            except:
                break
    except:
        resultcomment = []
    return (resultcomment,resultreaction,resulttag)

def getcommentPost(p):
    resultcomment=[]
    resultreaction = []
    resulttag =[]
    try:
        com = p['comments']
        while (True):
            if (len(com['data']) == 0):
                break
            for c in com['data']:#['p_id','parent_cid','cid','c_message','c_created_time','uid','uname']
                resultcomment.append(
                    [p['id'], 'x', c['id'], c['message'], c['created_time'], c['from']['id'], c['from']['name']])
                resultreaction.extend(getreaction(p, c))
                resulttag.extend(gettag(p,c))

                kq = getreplyComment(p, c)
                resultcomment.extend(kq[0])
                resultreaction.extend(kq[1])
            try:
                com = requests.get(com['paging']['next']).json()
            except:
                break
    except:
        resultcomment = []
    return (resultcomment,resultreaction,resulttag)

def getPost(p):#['p_id','p_message','p_created_time','p_updated_time','u_id','u_name','no_share'])
    msg = ''
    noshare = 0
    uid='x'
    uname='x'
    try:
        uid = p['from']['id']
    except:
        uid='x'

    try:
        uname = p['from']['id']
    except:
        uname='x'

    try:
        msg=p['message']
    except:
        try:
            msg=p['story']
        except:
            msg='x'
    try:
        noshare = p['shares']['count']
    except:
        noshare=0

    tempreac = []
    try:
        reaction = p['reactions']
        while (True):
            if (len(reaction['data']) == 0):
                break
            for r in reaction['data']:
                tempreac.append(
                    [p['id'], 'x', r['type'], r['id'], r['name']])
            try:
                reaction = requests.get(reaction['paging']['next']).json()
            except:
                break
    except:
        tempreac = []

    temptag = []
    try:
        tag = p['message_tags']
        while (True):
            if (len(tag) == 0):
                break
            for t in tag:
                temptag.append(
                    [p['id'], 'x', t['type'], t['id'], t['name']])
            try:
                tag = requests.get(tag['paging']['next']).json()
            except:
                break
    except:
        temptag = []
    return ([p['id'],p['type'], msg, p['created_time'], p['updated_time'], uid, uname,noshare],tempreac,temptag)


i = 0
while(True):
    i=i+len(posts['data'])
    if (len(posts['data']) == 0):
        break
    print(i)
    with Pool(6) as pool:
        resultspost=pool.map(getPost,posts['data'])
        kq=pool.map(getcommentPost, posts['data'])
        pool.close()
        pool.join()

        for (p,rs,ts) in resultspost:
            writerpost.writerow(p)
            for r in rs:
                writerreaction.writerow(r)
            for t in ts:
                writertag.writerow(t)
        for (cs,rs,ts) in kq:
            for c in cs:
                writercomment.writerow(c)
            for r in rs:
                writerreaction.writerow(r)
            for t in ts:
                writertag.writerow(t)

    try:
        posts = requests.get(posts['paging']['next']).json()
    except:
        break
