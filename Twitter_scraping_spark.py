# -*- coding: utf-8 -*-
"""
Created on Wed May 04 03:01:25 2016

@author: aravind
"""
import pandas as pd
import re, traceback
from selenium import webdriver
import Twitter_scraping as tc

import os
import sys

# Path for spark source folder
os.environ['SPARK_HOME']="C:/Users/aravind/Downloads/spark"

# Append pyspark  to Python Path
sys.path.append("C:/Users/aravind/Downloads/spark/python/")


from pyspark import SparkContext
from pyspark import SparkConf
sc = SparkContext('local[4]')


def loadData(path):
    orig_data = pd.read_csv(path,header=0)
        
    data = pd.read_csv(path,header=1, dtype=str)
    
    data = data.as_matrix()
    
    
    result = pd.DataFrame(columns = orig_data.columns) 
    #result.columns = orig_data.columns
    result.loc[0] = orig_data.loc[0]
    size = 10        
    splitLists = [ data[i:i+size] for i in xrange(0,len(data),size) ]     
    
    
    for index,i in enumerate(splitLists):
        rdd = sc.parallelize(i)
        #rdd = rdd.flatMap()
        partialResult = pd.DataFrame(rdd.map(retrieveAll).collect(), columns = orig_data.columns)
        #partialResult = pd.DataFrame(retrieveAll(i), columns = orig_data.columns)
        partialResult.to_csv('result'+str(index)+'.csv',index=None,encoding='utf-8')        
        result = pd.concat([result,partialResult])
        
                
    result.to_csv('resultFinal.csv',index=None,encoding='utf-8')
    
def ret(data):
    print type(data[0])
    return data    

def retrieveAll(current):    
    #result = []
    driver = webdriver.Firefox()
    #for index,current in enumerate(data):
    try:
        postUrl = current[7]
        #get consumer data
        id = postUrl.split('/')[-1]
        row = []
        fields = {}
        for i in xrange(21):
            fields[i] = current[i]           
        
        
        parent_time = tc.userInfo(id,fields)
        
        if(fields.get(22)):
            fields[24] = tc.checkIfRetweeted(id, fields.get(1).lower())
        else:
            fields[24] = 'FALSE'
        
        #scrape the post to get reply id's
        tc.scrape(postUrl, fields,driver)
        
        #TO DO field AB 
        text = fields.get(5)        
        firstWord = re.match('(\S*)\s',text).groups()[0].replace('@','')
        if firstWord.lower()==fields.get(1).lower():
            fields[27]='TO'                
        else:
            fields[27]='ABOUT'

        #Part 2 repplies 

        replies = tc.getReplies(postUrl,driver)
       
        count = -1
        for j in replies:
            count += 1
            if j.get('screen_name').lower()==fields.get(1).lower():
                tc.getUserData(j.get('id'),row, fields.get(3).lower(), True, parent_time,driver)  
                break            
        #Part3 
        if row:        
            for j in replies[count:]:
                if j.get('screen_name').lower()==fields.get(3).lower():
                    tc.getUserData(j.get('id'), row,fields.get(1).lower(), False, row[1],driver)              
                      

        
        
        finalRow = []
        for i in xrange(len(fields)):
            finalRow.append(fields.get(i))
            
        for i in row:
            finalRow.append(i)
                
        for j in xrange(len(finalRow), len(current)-6):
            finalRow.append('')
            
            
        #Part 4        
        tc.searchHistory(fields.get(1).lower(), fields.get(3).lower(), finalRow,driver)
            
        #print finalRow
    
        #result.append(finalRow)
    except:
        print 'Error in row: ', id
        traceback.print_exc()   
    
    driver.close()         

    return finalRow
    
    
import time

start = time.time()    
loadData("D:\CIS data\sample.csv")
end = time.time()
print (end-start)/60