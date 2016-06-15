import oauth2 as oauth
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from email.utils import parsedate_tz, mktime_tz

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
    
    for index,i in enumerate(data['PostURL']):
        #get consumer data
        id = i.split('/')[-1]
        row = [index+1]
        userInfo(id,row)
        
        #scrape the post to get reply id's
        replies = scrape(i)
        for j in replies:
            userInfo(j,row)
            
        for j in xrange(len(row), len(result.columns)):
            row.append('')
        result.loc[index+1] = row
            

    result.to_csv('result.csv',index=None,encoding='utf-8')

            

def scrape(url):
    result = []
    html = requests.get(url).text
    soup = BeautifulSoup(html,'lxml')
    replies = soup.find_all('div','replies-to')
    if replies:
        for i in replies[:2]:
            tweet = i.find('div','tweet')
            if tweet:
                result.append( tweet['data-tweet-id'])
#            reply = i.find('div','content')
#            if reply:
#                p = reply.find('p')
#                return p.text
    return result
        

def userInfo(id,row):
    url = 'https://api.twitter.com/1.1/statuses/show.json?id='+id
    
    response = twitterreq(url)
    user = response.get('user')
    row.append(user.get('screen_name'))
    
    createdTime = response.get('created_at')
    timestamp = mktime_tz(parsedate_tz(createdTime))
    row.append(datetime.fromtimestamp(timestamp) )
    row.append(response.get('text'))
    row.append('https://twitter.com/'+ user.get('screen_name'))
    post_url = 'https://twitter.com/'+user.get('screen_name')+ '/status/' + id
    row.append(post_url)
    row.append(user.get('followers_count'))
    
    location = user.get('location')
    #add dummy for country and state
    row.append('')
    row.append('')
    row.append(user.get('description'))
    row.append(user.get('name'))
    row.append(response.get('retweet_count'))
    row.append(user.get('favourites_count'))   

    
    
loadData("D:\CIS data\set1.csv")