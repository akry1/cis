# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 21:27:59 2015

@author: AravindKumarReddy
"""

import pandas as pd

def count(path):
    content = pd.read_csv(path)
    print 'Total tweets: ', len(content)
    print 'Avg per month: ', len(content)/24.0
    
    content.columns = ['name','tweet','time']
    
    byUser = content.groupby('name',sort=False)['tweet'].agg('count').reset_index()[[0,1]]    
    byUser.columns = ['name','tweetscount']    
    
    #byUser.to_csv('totalCount.csv',header=True,index=False)
    
    content['month'] = content['time'].apply(lambda x: x.split('-')[1] if x.find('-')!=-1 else '')
    content['year'] = content['time'].apply(lambda x: x.split('-')[0] if x.find('-')!=-1 else '')
    
    byMonth = content.groupby(['name','year','month'], sort=False)['tweet'].agg(['count']).reset_index()
    byMonth.columns = ['name','year','month','tweetscount']
    byMonth.to_csv('totalCountbyMonth.csv',header=True,index=False)
    #stats = byMonth.groupby(['name','month'], sort=False)['tweetscount'].agg(['mean','max','min']).reset_index()
    #stats.to_csv('stats.csv',header=True,index=False)
    print 'Max tweets per month: ', max(byMonth['month'])
    print 'Min tweets per month: ', min(byMonth['month'])
    #print stats
    
count('C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\tweets_reps.csv')