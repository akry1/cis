# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

month_dict={''.join([str(j),str(l)]): i+12*k-5 for i,j in enumerate(xrange(1,13)) for k,l in enumerate([2013,2014]) if i+12*k > 5 and i+12*k-5<12}

def monthcode(time):
    try:
        dateSplit = time.strip().split()[0].split('-')
        month = int(dateSplit[1])
        if month == 5:
            if int(dateSplit[2]) > 24:
                return 0
        return month_dict.get(''.join([str(month),dateSplit[0]]),0)
    except:
        return 0
        

def sentimentcounts(inputfile, outputfile):
    data = pd.read_csv(inputfile,header=0,dtype=str)[[0,2,3]]
    data.columns = ['handle','time','sentiment']
    output = pd.read_csv(outputfile,header=0,dtype=str)
    
    handleCol = output['handle'].copy()
    
    output['handle'] = output['handle'].str.lower()
    
    users = output['handle']     
    
    data['handle'] = data['handle'].str.lower()
    data = data[data['handle'].isin(users)]
    data['monthcode'] = data['time'].apply(monthcode)    
    data = data[data['monthcode'] != 0]
    
    #data['handle'] = data['hand'].str.lower()
    
    grouped = data.groupby(['handle','monthcode'])
    
    sentimentDF = pd.DataFrame(columns=['handle','month','PositiveCount', 'NeutralCount','NegativeCount','TotalFlag'], index=np.arange(0,len(grouped)))       
       
    i=0
    for name,group in grouped:
        neutral = len(group[group['sentiment']=='3'])
        pos = len(group[group['sentiment']=='4']) + len(group[ group['sentiment']=='5' ])
        neg = len(group[group['sentiment']=='1']) + len(group[ group['sentiment']=='2' ])
        sentimentDF.loc[i] = [name[0],str(name[1]) ,str(pos), str(neutral), str(neg), str(pos+neutral+neg)]    
        i+=1
    
    result = pd.merge(output,sentimentDF, how='left', on=['handle','month'])
    result['PositiveCount'].fillna(0,inplace=True)
    result['NeutralCount'].fillna(0,inplace=True)
    result['NegativeCount'].fillna(0,inplace=True)
    
    orig_users = list(data['handle'])
    result['HandleFlag'] =  result['handle'].apply(lambda x: 0 if x in orig_users else 1)
    result['handle'] = handleCol
    #result['TotalDiff'] = result.apply(diff, axis=1)
    result['TotalFlag'] = result.apply(flag, axis=1)
    result.to_csv('CompleteLongMay2015iwthHashtagsTopicsSentimentCount.csv',index=None)
        
def flag(x):
    if x['TotalFlag'] is not np.NaN and x['tweet'] is not np.NaN:
        if x['TotalFlag'] == x['tweet']:
            return 0
        else:
            return 1
    elif x['TotalFlag'] is np.NaN and x['tweet'] is np.NaN:
        return 0
    else:
        return 1
        
        
def diff(x):
    if x['TotalFlag'] is not np.NaN and x['tweet'] is not np.NaN:
        return int(x['TotalFlag']) - int(x['tweet'])
    elif x['TotalFlag'] is np.NaN and x['tweet'] is np.NaN:
        return 0
    elif x['TotalFlag'] is not np.NaN:
        return x['TotalFlag']
    else:
        return -1*int(x['tweet'])
        
if __name__== '__main__':
    #sentimentcounts('/media/aravind/New Volume/CIS data/reps_113th_withsentimentscore.csv','CompleteLongMay2015iwthHashtagsTopicsCount.csv')
    sentimentcounts('D:/CIS data/reps_113th_withsentimentscore.csv','CompleteLongMay2015iwthHashtagsTopicsCount.csv')
    
 # -*- coding: utf-8 -*-

