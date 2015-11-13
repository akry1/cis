# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 08:54:51 2015

@author: AravindKumarReddy
"""

from bs4 import BeautifulSoup
import re
import csv

def twitterList(path):
    with open(path,'r') as file:
        content = file.read()        
    userHandleList = []        
    soup = BeautifulSoup(content)
    ol = soup.find('ol', id='stream-items-id')    
    li = ol.find_all('li')    
    for i in li:
        name = i.find('strong','fullname').text
        pattern = re.compile(u'^[U][ \.]?[S][\.]?|Rep[\.]?(?=[\sA-Z])|^Sen[\.]?(?=[\sA-Z])|Senator|Congressman|Congresswoman|Senate|Speaker|Cong[\.]?|Judge|[,]?\s[A-Z][a-z]?\.(?=\s)?')
        strippedname = re.sub(pattern,'',name).strip()
        handle = i.find('span','username').text
        handle = handle.replace('@','')
        userHandleList.append([name,strippedname, handle])    
    return userHandleList
    

def writeToCSV(lst):
    with open('congressman_handle.csv', 'wb') as file:
        writer = csv.writer(file)
        writer.writerow(['Twitter Username','Stripped Name','Handle'])
        writer.writerows(lst)
    
    
userHandlelist = twitterList('F:\Skydrive\Python\cis\memberlist_source.html')
writeToCSV(userHandlelist)