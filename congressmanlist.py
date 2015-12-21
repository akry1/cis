# -*- coding: utf-8 -*-
"""
Created on Wed Nov 11 22:28:30 2015

@author: AravindKumarReddy
"""
from bs4 import BeautifulSoup
import requests
import csv
import re

def scrapeHORs(url):
    congressmanlist = []
    resp = requests.get(url).text
    soup = BeautifulSoup(resp)
    table = soup.find('table','wikitable')
    for i in table.find_all('tr'):
        columns = i.find_all('td')
        if columns:
            name = re.sub('\[.*\]','',columns[1].text).encode('ascii','ignore')
            district = ' '.join(columns[3].text.split())
            congressmanlist.append( [name,columns[2].text,district])
    return congressmanlist
    
    
def scrapeSenators(url):
    congressmanlist = []
    resp = requests.get(url).text
    soup = BeautifulSoup(resp)
    table = soup.find('table','wikitable')
    for i in table.find_all('tr'):
        columns = i.find_all('td')
        if columns:
            columns= columns[0]
            tdtext = re.search('\((.*)\)',columns.text)            
            if tdtext:
                temp = tdtext.groups()[0].split('-')
            name = columns.find('a').text
            
            congressmanlist.append( [name,temp[0],temp[1]])
    return congressmanlist
    
def writeToCSV(lst,name):
    with open(name, 'wb') as file:
        writer = csv.writer(file)
        writer.writerow(['Name','Party','District'])
        writer.writerows(lst)
    
    
lst = scrapeHORs('https://en.m.wikipedia.org/wiki/List_of_United_States_Representatives_in_the_114th_Congress_by_seniority')

#slist = scrapeSenators('https://en.wikipedia.org/wiki/List_of_United_States_Senators_in_the_114th_Congress_by_seniority')

writeToCSV(lst,'HouseOfRepresentatives_114th.csv')
#writeToCSV(slist,'senators_114th.csv')