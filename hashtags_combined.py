# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 19:46:33 2015

@author: aravind
"""

# -*- coding: utf-8 -*-
import pandas as pd
import re
  
    
def extractAll(path):
    pattern1 = re.compile(u'([@]\S+)')  
    pattern2 = re.compile(u'([#]\S+)') 
    data = pd.read_csv(path,header=None,dtype=str)
    newFileRetweets = pd.DataFrame()
    newFile = pd.DataFrame()
    newFileHashtags = pd.DataFrame()
    for row in data.iterrows():  
        for i in re.findall(pattern2,str(row[1][1])):
            row[1][3] = re.sub('^#|[.,:;]$','',i)
            newFileHashtags = newFileHashtags.append(row[1]) 
        for i in re.findall(pattern1,str(row[1][1])):
            row[1][3] = re.sub('^@|[.,:;]$','',i)
            if re.search('RT @',str(row[1][1])):
                #row[1][3] = row[1][3].replace(':','')
                newFileRetweets = newFileRetweets.append(row[1])
            else:
                newFile = newFile.append(row[1])  
    newFileHashtags.to_csv('houseofreps_113th_withhashtags.csv',index=False,header=None)
    newFile.to_csv('houseofreps_113th_withMentions.csv',index=False,header=None)    
    newFileRetweets.to_csv('houseofreps_113th_withRetweetMentions.csv',index=False,header=None)
    
    #just mentions
    newFile = newFile.drop(1,1)
    newFile[[0,3,2]].to_csv('houseofreps_113th_withMentions_notweet.csv',index=False,header= ['Name','Mentioned','Date'])
    
    newFileRetweets = newFileRetweets.drop(1,1)
    newFileRetweets[[0,3,2]].to_csv('houseofreps_113th_withRetweetMentions_notweet.csv',index=False,header = ['Name','Mentioned','Date'])   
	
if __name__ == '__main__':
    #extractHashtags('C:\\Users\\aravind\\Downloads\\tweets_reps.csv')
    #extractHashtags('/media/aravind/OS/Documents and Settings/aravind/Downloads/tweets_reps114th.csv')
    extractAll('/media/aravind/OS/Documents and Settings/aravind/Downloads/tweets_reps.csv')