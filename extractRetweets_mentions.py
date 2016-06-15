# -*- coding: utf-8 -*-
import pandas as pd
import re

def retweets_mentions(path):
    retweet_pattern = re.compile(u'^RT @(.*?):? .*')
    data = pd.read_csv(path,header=None,dtype=str)[[0,1]]
    newFileRetweets = pd.DataFrame()
    
    mention_pattern = re.compile(u'([#]\S+)') 
    newFileMentions = pd.DataFrame()
    for row in data.iterrows():
        search = re.search(retweet_pattern,str(row[1][1]))
        if search:
            row[1][2] = search.groups()[0]
            newFileRetweets = newFileRetweets.append(row[1]) 
            
        for i in re.findall(mention_pattern,str(row[1][1])):
            row[1][2] = re.sub('^#|[.,:;\?]$','',i)
            newFileMentions = newFileMentions.append(row[1])
    newFileRetweets.to_csv('houseofreps_113th_retweets.csv',index=False,header=None)
    newFileMentions.to_csv('houseofreps_113th_mentions.csv',index=False,header=None)

if __name__ == '__main__':
    retweets_mentions('D:\\CIS data\\reps_113th_withsentimentscore.csv')