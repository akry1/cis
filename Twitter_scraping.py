# -*- coding: utf-8 -*-
"""
Created on Sun Apr 24 00:09:46 2016

@author: aravind
"""

import oauth2 as oauth
import requests,re, traceback
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from email.utils import parsedate_tz, mktime_tz
from selenium import webdriver

##code of twitter api access 

api_key = "jQ81Vz8Ejtwqk7X8Lfhgr8C94"
api_secret = "nUKGmZXCLJbuZc3ZVG0YgHwrH3Ou0gWkBIllMTu4cBBxZU2lOP"
access_token_key = "2790517464-3vspv3eBT019ac9Kxult8kNafK59BBKPApHSzHJ"
access_token_secret = "bYjvlWlb946a18HTxDT4TgcPDeh4EYxImNpmidnOf6T7Z"

oauth_token    = oauth.Token(key=access_token_key, secret=access_token_secret)
oauth_consumer = oauth.Consumer(key=api_key, secret=api_secret)

signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

def twitterreq(url, method='GET'):
    req = oauth.Request.from_consumer_and_token(oauth_consumer,
                                             token=oauth_token,
                                             http_method= method,
                                             http_url=url)

    req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)  
  
    if method == "GET":
        url = req.to_url()    

    return requests.get(url).json()
    
##end

    
def loadData(path):
    orig_data = pd.read_csv(path,header=0)
        
    data = pd.read_csv(path,header=1, dtype=str)
    
    result = pd.DataFrame(columns = orig_data.columns)
    #result.columns = orig_data.columns
    result.loc[0] = orig_data.loc[0]
    driver = webdriver.Firefox()
    for index,current in enumerate(data.iterrows()):
        try:
            postUrl = current[1]['PostUrl']
            #get consumer data
            id = postUrl.split('/')[-1]
            row = []
            fields = {}
            for i in xrange(21):
                fields[i] = current[1][i]
            
            
            
            parent_time = userInfo(id,fields)
            
            if(fields.get(22)):
                fields[24] = checkIfRetweeted(id, fields.get(1).lower())
            else:
                fields[24] = 'FALSE'
            
            #scrape the post to get reply id's
            scrape(postUrl, fields,driver)
            
            #TO DO field AB 
            text = fields.get(5)        
            firstWord = re.match('(\S*)\s',text).groups()[0].replace('@','')
            if firstWord.lower()==fields.get(1).lower():
                fields[27]='TO'                
            else:
                fields[27]='ABOUT'
    
            #Part 2 repplies 
    
            replies = getReplies(postUrl,driver)
           
            count = -1
            for j in replies:
                count += 1
                if j.get('screen_name').lower()==fields.get(1).lower():
                    getUserData(j.get('id'),row, fields.get(3).lower(), True, parent_time,driver)  
                    break            
            #Part3 
            if row:        
                for j in replies[count:]:
                    if j.get('screen_name').lower()==fields.get(3).lower():
                        getUserData(j.get('id'), row,fields.get(1).lower(), False, row[1],driver)              
                          

            
            
            finalRow = []
            for i in xrange(len(fields)):
                finalRow.append(fields.get(i))
                
            for i in row:
                finalRow.append(i)
                    
            for j in xrange(len(finalRow), len(result.columns)-6):
                finalRow.append('')
                
                
            #Part 4        
            searchHistory(fields.get(1).lower(), fields.get(3).lower(), finalRow,driver)
                
            #print finalRow
                
            result.loc[index+1] = finalRow
        except:
            print 'Error in row: ', index+1
            traceback.print_exc()
            

    result.to_csv('result.csv',index=None,encoding='utf-8')
    
    
    
    
def getReplies(url,driver):
    result = []
    #html = requests.get(url).text
    #soup = BeautifulSoup(html,'lxml')
    driver.get(url)
    soup = BeautifulSoup(driver.page_source,'html5lib') 
    replies = soup.find('div','replies-to')
    if replies:
        ol = replies.find('ol',id='stream-items-id')
        if ol:
            li = ol.find_all('li','stream-item')
            for i in li:
                tweet = i.find('div','tweet')
                if tweet:
                    result.append({ 'id': tweet['data-tweet-id'] , 'screen_name': tweet['data-screen-name'].lower()})
        else:
            tweet = i.find('div','tweet')
            if tweet:
                result.append({ 'id': tweet['data-tweet-id'] , 'screen_name': tweet['data-screen-name'].lower()})
    return result
        

def getUserData(id,row,name, isCompany, parent_time,driver):
    url = 'https://api.twitter.com/1.1/statuses/show.json?id='+id    
    
    response = twitterreq(url)
    user = response.get('user')
    
    post_url = 'https://twitter.com/'+user.get('screen_name')+ '/status/' + id
    html = requests.get(post_url).text
    soup = BeautifulSoup(html,'html5lib')
    #driver.get(url)
    #soup = BeautifulSoup(driver.page_source,'html5lib') 
    
    row.append(user.get('screen_name'))
    
    createdTime = response.get('created_at')
    timestamp = mktime_tz(parsedate_tz(createdTime))
    row.append(datetime.fromtimestamp(timestamp) )
    row.append(response.get('text'))
    #row.append('https://twitter.com/'+ user.get('screen_name'))
    
    row.append(post_url)
    if(isCompany):
        row.append(user.get('followers_count'))
        row.append(user.get('friends_count'))
        #location = user.get('location')
        #add dummy for country and state
        row.append('')
        row.append('')
        row.append(user.get('description'))
        row.append(user.get('name'))
        #parent_time = datetime.strptime(parent_time, '%m/%d/%Y %H:%M')
        #parent_time = parent_time.replace(second = 0)
        #print parent_time
    #add dummy for time diff
    #print datetime.fromtimestamp(timestamp)    
    time_diff = datetime.fromtimestamp(timestamp)  - parent_time    
    row.append(time_diff.total_seconds()/60)
    row.append(response.get('retweet_count')) 
    
    likes = soup.find('a','request-favorited-popup')    
    if likes:
        row.append(int(likes['data-tweet-stat-count']))
    else:
        row.append(0)
    
    hashtags = response.get('entities').get('hashtags')
    
    if hashtags:
        row.append('TRUE')
        row.append(len(hashtags))
    else:
        row.append('FALSE')
        row.append(0)
        
    mentions = response.get('entities').get('user_mentions')
    if mentions:
        row.append(len(mentions))
    else:
        row.append(1)
    
    emoji = soup.find('div','permalink-tweet').find('img','Emoji')
    if emoji:
        row.append('TRUE') 
    else:
        row.append('FALSE')
    
    #image
    img = soup.find('meta',property='og:image:user_generated')   

    if img and img['content'] == 'true':
        row.append('TRUE') 
        img_link = soup.find('meta',property='og:image') 
        row.append(img_link['content'])
    else:
        row.append('FALSE')
        row.append('')        
    
    if isCompany:    
        row.append(checkIfRetweeted(id,name))
        flag = False 
        if likes:            
            #likes_tag = soup.find('div','js-tweet-stats-container') 
            #if likes_tag:
            likes_tag1 = soup.find('li','avatar-row js-face-pile-container')
            if likes_tag1:
                likes_tag2 = likes_tag1.find_all('a')
                       
                for i in likes_tag2:
                    user = i['href'].split('/')[-1]
                    if user.lower()==name.lower():
                        #row.append('TRUE')
                        flag = True
                        break
    
        if flag: 
            row.append('True')
        else:
            row.append('FALSE')
            

def scrape(url, fields,driver):
    html = requests.get(url).text
    soup = BeautifulSoup(html,'html5lib')
#    driver.get(url)
#    soup = BeautifulSoup(driver.page_source,'html5lib') 
    
    likes = soup.find('a','request-favorited-popup')
    
    if(likes):
        fields[23] = int(likes['data-tweet-stat-count'])
    else:
        fields[23] = 0
    
    if fields.get(23)>0:
        #likes_tag = soup.find('div','js-tweet-stats-container')        
        #if likes_tag:
        likes_tag1 = soup.find('li','avatar-row js-face-pile-container')
        if likes_tag1:
            likes_tag2 = likes_tag1.find_all('a')
            
            for i in likes_tag2:
                name = i['href'].split('/')[-1]
                if name.lower()==fields.get(1).lower():
                    fields[25] = 'TRUE'
                    break
        
    if(not fields.get(25)):
        fields[25] = 'FALSE'
        
    isReplyTag = soup.find('div','permalink-in-reply-tos')
    
    if isReplyTag:
        fields[26] = 'TRUE'
    else:
        fields[26] = 'FALSE'
        
    emoji = soup.find('div','permalink-tweet').find('img','Emoji')
    if emoji:
        fields[31] = 'TRUE'
    else:
        fields[31] = 'FALSE'
        
        
    #TODO Field 32 33
    img = soup.find('meta',property='og:image:user_generated')   

    if img and img['content'] == 'true':
        fields[32]='TRUE'
        img_link = soup.find('meta',property='og:image') 
        if img_link:
            fields[33] = img_link['content']
    else:
        fields[32]='FALSE'
        fields[33] = None

        

def userInfo(id,fields):
    url = 'https://api.twitter.com/1.1/statuses/show.json?id='+id
    
    response = twitterreq(url)
    #print response
    user = response.get('user')    
    
    fields[21] = user.get('friends_count')
    fields[22] = response.get('retweet_count')
    #fields[23] = response.get('favourites_count')
    
    hashtags = response.get('entities').get('hashtags')
    
    if hashtags:
        fields[28] = 'TRUE'
        fields[29] = len(hashtags)
    else:
        fields[28] = 'FALSE'
        fields[29] = 0
    
    #TO DO check if company is not mentioned with @ but others are mentioned    
    mentions = response.get('entities').get('user_mentions')
    if mentions:
        fields[30] = len(mentions)
    else:
        fields[30] = 1
        
    createdTime = response.get('created_at')
    timestamp = mktime_tz(parsedate_tz(createdTime))
    return datetime.fromtimestamp(timestamp)  
    
def checkIfRetweeted(id, name):
    url = 'https://api.twitter.com/1.1/statuses/retweets/'+ id + '.json'    
    response = twitterreq(url)
    for i in response:
        user = i.get('user').get('screen_name')
        if (user.lower()==name):
            return 'TRUE'
            break
    return 'FALSE'


##Test
    
    
    
def searchHistory(companyName, userName, row,driver):
    
    #https://twitter.com/search?q=from%3Aluxtraveldiary%20%40amawaterways%20since%3A2015-07-01%20until%3A2015-12-31&src=typd
    buildUrl = 'https://twitter.com/search?q=from%3A{0}%20%40{1}%20since%3A{2}%20until%3A{3}'
    
    
    dateText = []    
    searchUrl = buildUrl.format(userName,companyName,'2015-07-01' , '2015-12-31')
    try:
        driver.get(searchUrl)
        soup = BeautifulSoup(driver.page_source,'html5lib')  
        responses = soup.find('ol',id='stream-items-id').find_all('li','stream-item')
        row.append(len(responses))
        for i in responses:
            content = i.find('div','content')
            dateText.append(content.find('a','tweet-timestamp').text)
            dateText.append(content.find('p','tweet-text').text)
    except:
        row.append('')
    
    
    searchUrl = buildUrl.format(companyName,userName,'2015-07-01' , '2015-12-31')
    try:
        driver.get(searchUrl)
        soup = BeautifulSoup(driver.page_source,'html5lib')  
        responses = soup.find('ol',id='stream-items-id').find_all('li','stream-item')
        row.append(len(responses))
        for i in responses:
            content = i.find('div','content')
            dateText.append(content.find('a','tweet-timestamp').text)
            dateText.append(content.find('p','tweet-text').text) 
    except:
        row.append('')
        
   
    row.append(';'.join(dateText))
    dateText=[]
    searchUrl = buildUrl.format(userName,companyName,'2016-02-01' , '2016-04-15')
    try:
        driver.get(searchUrl)
        soup = BeautifulSoup(driver.page_source,'html5lib')  
        responses = soup.find('ol',id='stream-items-id').find_all('li','stream-item')
        row.append(len(responses))
        for i in responses:
            content = i.find('div','content')
            time = content.find('a','tweet-timestamp')['title']
            date = time.split('-')[-1].strip()
            dateText.append(date)
            dateText.append(content.find('p','tweet-text').text)
    except:
        row.append('')
    
    searchUrl = buildUrl.format(companyName,userName,'2016-02-01' , '2016-04-15')
    try:
        driver.get(searchUrl)
        soup = BeautifulSoup(driver.page_source,'html5lib')  
        responses = soup.find('ol',id='stream-items-id').find_all('li','stream-item')
        row.append(len(responses))
        for i in responses:
            content = i.find('div','content')
            time = content.find('a','tweet-timestamp')['title']
            date = time.split('-')[-1].strip()
            dateText.append(date)
            dateText.append(content.find('p','tweet-text').text)
    except:
        row.append('')
    
    #dummy for date & text
    row.append(';'.join(dateText))
        
        
def minEditDistance(a,b,error=0.1):
    D = {(-1,-1):0}

    lena = range(len(a))
    lenb = range(len(b))
    for i in lena:
        D[(i,-1)] = i+1
    for j in lenb:
        D[(-1,j)] = j+1

    for i in lena:
        for j in lenb:
            if a[i] == b[j]: sub = 0
            else: sub = 1
            D[(i,j)] = min(D[(i-1,j)]+1, D[(i,j-1)]+1, D[(i-1,j-1)]+sub)    

    return True if D[(len(a)-1,len(b)-1)]/float(len(a)) < error else False

def main():
    import time
    
    start = time.time()    
    loadData("D:\CIS data\sample.csv")
    end = time.time()
    print (end-start)/60