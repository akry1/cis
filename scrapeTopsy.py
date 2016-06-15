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

def retrieveAll(fname):
    try:
        rows = []
        mintime= 1353844800 # for 114th  1420243200
        for k in xrange(1,11):
            mintime= mintime+ 3153600
            maxtime= mintime+ 3153600
            i=1
            urlDict = {}
            while True:
                url=''.join(['http://otter.topsy.com/search.json?q=from%3A%40',fname,'&mintime=',str(mintime),'&maxtime=',str(maxtime),'&apikey=09C43A9B270A470B8EB8F2946A9369F3&perpage=100&page=',str(i)])
                try:
                    req= requests.get(url,timeout=120).text
                except requests.ConnectionError:                    
                    if urlDict.get(url,0)==0:
                        urlDict[url] = 1
                        print "Connection Error: ",url
                        continue
                    else:
                        break
                except requests.Timeout:
                    if urlDict.get(url,0)==0:
                        urlDict[url] = 1
                        print "Timeout: ",url
                        continue
                    else:
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
        pass



def main(filename,outputfile):
    start_time = time.time()
    with open(outputfile, 'wb') as output:
        writer = csv.writer(output)
        with open(filename,'r') as file:
            reader= csv.reader(file) 
            reader.next() #ignore header                    
            for row in reader:
                if row[3]:
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
            if row[3]:
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
                if l and len(l)>0:
                    writer.writerows(l)

    print("--- %s seconds ---" % (time.time() - start_time))


#main()
#spark_main('../../cis/senators_handle_final.csv', 'C:\Users\AravindKumarReddy\Desktop\Newfolder\tweets_senator.csv')
def list_main(outputfile):
    users = ['askgeorge', 'FrankPallone', 'EricCantor', 'CongCulberson', 'JudgeCarter','RepJohnCampbell','RepPaulBrounMD', 'DanMaffeiNY', 'RepGaramendi', 'RepTimGriffin', 
             'repjustinamash', 'repmichaelgrimm', 'RepHuffman', 'ElectRodneyIL', 'Castro4Congress', 'WeberforTexas']
             
    with open(outputfile, 'wb') as output:
        writer = csv.writer(output) 
        for row in users:
            if row:
                rows = retrieveAll(row)
                if rows and len(rows) > 0:
                    writer.writerows(rows)

#spark_main('../cis/HouseOfRepresentatives114th_handle_final.csv', 'C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\tweets_reps114th.csv')

#spark_main('../../cis/HouseOfRepresentatives_handle_final.csv', 'C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\tweets_reps.csv')

list_main('D:\\CIS data\\tweets_reps113th_newusers.csv')

