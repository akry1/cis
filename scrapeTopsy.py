# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 15:23:41 2015

@author: AravindKumarReddy
"""

import json
import requests
import datetime
import csv
import time

#MINTIME = 1353844800
#MAXTIME = 1420027199

def retrieveAll(fname):
    try:
        rows = []
        mintime= 1353844800
        for x in xrange(1,21):
            mintime= mintime+ 3153600
            maxtime= mintime+ 3153600
            i=1
            while True:
                url=''.join(['http://otter.topsy.com/search.json?q=from%3A%40',fname,'&mintime=',str(mintime),'&maxtime=',str(maxtime),'&apikey=09C43A9B270A470B8EB8F2946A9369F3&perpage=100&page=',str(i)])
                try:
                    req= requests.get(url).text
                except requests.ConnectionError:
                    print "Connection Error"
                    break

                data = json.loads(req)

                lst = data['response']['list']
                if lst:
                    for j in lst:
                        author = j['trackback_author_nick']
                        if author:
                            content= j['content'].encode('ascii','ignore')
                            unixTimeStamp = j['firstpost_date']
                            postdate =  datetime.datetime.fromtimestamp(unixTimeStamp).strftime('%Y-%m-%d %H:%M:%S')
                            rows.append([ author, content, postdate])
                else: break                    
                i+=1
        return rows
    except:
        print "Error in JSON Parsing!!"



def main(filename,outputfile):
    start_time = time.time()
    output=[]
    with open(outputfile, 'wb') as output:
        writer = csv.writer(output)
        with open(filename,'r') as file:
            reader= csv.reader(file) 
            reader.next() #ignore header                    
            for row in reader:
                rows = retrieveAll(row[3])
                if rows and len(rows) > 0:
                    writer.writerows(rows)  #writing to csv per user as it is expensive to copy into a new list & also avoiding memory error
    print("--- %s seconds ---" % (time.time() - start_time))


def spark_main(filename,output):
    start_time = time.time()
    files = []
    with open(filename,'r') as file:
        reader= csv.reader(file)    
        reader.next() #ignore header               
        for row in reader:
            files.append(row[3])  
    size = 10        
    splitLists = [ files[i:i+size] for i in range(0,len(files),size) ]  
    with open(output, 'wb') as file:
        writer = csv.writer(file)
        for i in splitLists:
            print i
            rdd = sc.parallelize(i)
            result = rdd.map(retrieveAll).collect()
            for l in result:       
                writer.writerows(l)

    print("--- %s seconds ---" % (time.time() - start_time))


#main()
#spark_main('senators_handle_final.csv', 'C:\Users\AravindKumarReddy\Desktop\Newfolder\tweets_senator.csv')

main('senators_handle_final.csv', 'C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\tweets_senator.csv')