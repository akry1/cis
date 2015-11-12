import json
import requests
import datetime
import csv
import time

def retrieveAll(fname):
    try:
        rows = []
        mintime= 1154415600
        for i in xrange(1,91):
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

                list = data['response']['list']
                if list:
                    for j in list:
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



def main():
    start_time = time.time()
    output=[]
    with open('namesDataYili3201-36001.csv', 'wb') as output:
        writer = csv.writer(output)
        with open('F:\Skydrive\ASU\CISResearchAide/user_twitter14001-end1.csv','r') as file:
            reader= csv.reader(file)                   
            for row in reader:
                rows = retrieveAll(row[0])
                if rows and len(rows) > 0:
                    writer.writerows(rows)  #writing to csv per user as it is expensive to copy into a new list & also avoiding memory error
    print("--- %s seconds ---" % (time.time() - start_time))


def spark_main():
    start_time = time.time()
    files = []
    with open('F:\Skydrive\ASU\CISResearchAide/user_twitter14001-end1.csv','r') as file:
        reader= csv.reader(file)                   
        for row in reader:
            files.append(row[0])  
    size = 10        
    splitLists = [ files[i:i+size] for i in range(0,len(files),size) ]  
    with open('namesdatayili3201-36002.csv', 'wb') as file:
        writer = csv.writer(file)
        for i in splitLists: #xrange(0,len(files)/10+1):
            #if i == len(files)/10:
            #    end = min(i*10+10,len(files)%10) +i*10 +1
            #else : end = i*10+10
            #x = files[i*10:end]
            print i
            #rdd = sc.parallelize(x)
            #result = rdd.map(retrieveAll).collect()
            #for l in result:       
            #    writer.writerows(l)

    print("--- %s seconds ---" % (time.time() - start_time))


#main()
spark_main()


