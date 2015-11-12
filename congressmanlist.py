# -*- coding: utf-8 -*-
"""
Created on Wed Nov 11 22:28:30 2015

@author: AravindKumarReddy
"""
from bs4 import BeautifulSoup
import requests
import csv
import re

def scrapeWiki(url):
    congressmanlist = []
    resp = requests.get(url).text
    soup = BeautifulSoup(resp)
    table = soup.find('table','wikitable')
    for i in table.find_all('tr'):
        columns = i.find_all('td')
        if columns:
            name = re.sub('\[.*\]','',columns[0].text)
            congressmanlist.append( [name,columns[1].text,columns[2].text])
    return congressmanlist
    
def writeToCSV(lst):
    with open('congressman_113th.csv', 'wb') as file:
        writer = csv.writer(file)
        writer.writerow(['Name','Party','District'])
        writer.writerows(lst)
    
    
lst = scrapeWiki('https://en.m.wikipedia.org/wiki/List_of_United_States_Representatives_in_the_113th_Congress_by_seniority')
writeToCSV(lst)        