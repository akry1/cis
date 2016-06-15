# -*- coding: utf-8 -*-
import topic_modeling as tm
import pandas as pd
import numpy as np

month_dict={''.join([str(j),str(l)]): i+12*k-5 for i,j in enumerate(xrange(1,13)) for k,l in enumerate([2013,2014]) if i+12*k > 5 and i+12*k-5<12}

def monthcode(time):
    try:
        dateSplit = time.strip().split()[0].split('/')
        return month_dict.get(''.join([dateSplit[0],dateSplit[2]]),0)
    except:
        return 0
        

def topics(inputfile, outputfile):
    data = pd.read_csv(inputfile,header=0,dtype=str)[['tweet','time','handle']]
    output = pd.read_csv(outputfile,header=0,dtype=str)
    
    handleCol = output['handle'].copy()
    
    output['handle'] = output['handle'].str.lower()
    
    users = output['handle']     
    
    data['handle'] = data['handle'].str.lower()
    data = data[data['handle'].isin(users)]
    data['monthcode'] = data['time'].apply(monthcode)    
    data = data[data['monthcode'] != 0]
    
    data['tweet'] = data['tweet'].str.lower()
    
    grouped = data.groupby(['handle','monthcode'])
    
    topicDF = pd.DataFrame(columns=['handle','month','TopicsCount'], index=np.arange(0,len(grouped)))       
       
    i=0
    for name,group in grouped:
        topics = tm.numberOfTopics(group['tweet'])
        topicDF.loc[i] = [name[0],str(name[1]),str(topics)]    
        i+=1
        print i, topics
    
    result = pd.merge(output,topicDF, how='left', on=['handle','month'])
    result['TopicsCount'].fillna(0,inplace=True)
    result['handle'] = handleCol
    result.to_csv('CompleteLongMay2015iwthHashtagsTopicsCount.csv',index=None)
        

if __name__== '__main__':
    topics('/media/aravind/New Volume/CIS data/SentimentedTweetsPartyAugMay.csv','/media/aravind/New Volume/CIS data/CompleteLongMay2015iwthHashtags.csv')
    #topics('D:/CIS data/SentimentedTweetsPartyAugMay.csv','D:/CIS data/CompleteLongMay2015iwthHashtags.csv')
    
 