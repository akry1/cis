import pandas as pd
import re
import numpy as np
import datetime
import csv

def getTweets(file1, file2):
    with open(file1,'r') as file:
        twitterDetails = pd.read_csv(file,dtype=str, header=None)
        twitterDetails.columns = ['name','tweet','time']

    with open(file2,'r') as file:
        jobinfo = pd.read_csv(file,dtype=str,header=0)
        handleList = jobinfo['twitter'].unique()


    filteredData = twitterDetails[ twitterDetails['name'].isin( handleList) ].copy()

    filteredData['time'] = filteredData['time'].apply(formatDate)
    filteredData.to_csv('C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\Jobinfo_twitter.csv',header=True,index=False)


def formatDate(d):
    if d and type(d)==str:
        x = re.search(u'[0]?(\d+)/\d+/(\d+)',d)
        if x:
            return '/'.join(x.groups())
        else:
            x = re.search(u'(\d+)-[0]?(\d+)-\d+',d)
            return '/'.join(i for i in reversed(x.groups()))
    else: return ''


def createOutput(file1, file2):
    twitterDetails = pd.read_csv(file1,dtype=str, header=None, names =['id','TotalTweets','month-year'])    
    jobinfo = pd.read_csv(file2,dtype=str,header=0)
    output =   twitterDetails.groupby(['id','month-year'],sort=False, as_index=True).aggregate(['count']).reset_index()

    # sort TwitterKevins file and filter any tweets before 2000

    #output['month'] = output['month-year'].apply(lambda x:x.split('/')[0] if x.find('/')!=-1 else x)
    #output['year'] = output['month-year'].apply(lambda x:x.split('/')[1] if x.find('/')!=-1 else x)
    #output.sort(['id','year','month'],inplace=True)
    #output = output[output['year'] >=2000]
    #output.drop(output.columns[[3,4]],1,inplace=True)

    output.columns  = ['id','month-year','TotalTweets']
    currentMonth = datetime.date.today().month + 1

    dates = [ str(i)+'/' +str(j) for j in xrange(2000,2016) for i in xrange(1,13)  if j!=2015 or i<=currentMonth ]
    usernames = jobinfo['twitter'].unique()

    # sort JobsInfo file and filter any tweets before 2000
    jobinfo['month'] = jobinfo['start_time'].apply(lambda x:int(str(x).split('/')[0]) if str(x).find('/')!=-1 else x)
    jobinfo['year'] = jobinfo['start_time'].apply(lambda x:int(str(x).split('/')[1]) if str(x).find('/')!=-1  else x)
    jobinfo.sort(['twitter','year','month'],inplace=True)
    jobinfo = jobinfo[jobinfo['year'] >=2000]
    jobinfo.drop(jobinfo.columns[[10,11]],1,inplace=True)

    joblist=[]
    prevUser = None
    for i in jobinfo.iterrows():
        currentuser = i[1][9]
        start = i[1][3]
        end = i[1][4]
        job = i[1][5]
        s = dates.index(start)
        e= dates.index(end)

        if prevUser==currentuser:
            joblist.append([currentuser, dates[s],job,1])
        else:
            prevUser = currentuser
        for i in range(s+1,e+1):
            joblist.append([currentuser, dates[i],job,0])

    temp = pd.DataFrame([ [i,j] for i in usernames for j in dates], columns=['id','month-year'])

    temp2 = pd.DataFrame(joblist,columns=['id','month-year','Job','JobSwitch'])


    result =  pd.merge(temp,output, how='left', on = ['id','month-year'])
    result.fillna(0,inplace=True)
    result =  pd.merge(result,temp2, how='left', on = ['id','month-year'])

    if np.isnan(result['JobSwitch'][0]) : 
            result['JobSwitch'][0] =0

    for i in range(1, len(result)-1):
        if np.isnan(result['JobSwitch'][i]):
            if result['id'][i-1] !=  result['id'][i]:
                result['JobSwitch'][i] = 0
            else:
                if np.isnan(result['JobSwitch'][i+1]):
                    result['JobSwitch'][i] = 0
                elif result['JobSwitch'][i+1] == 0 and result['JobSwitch'][i+1] == 0 and result['id'][i+1] ==  result['id'][i]:
                    result['JobSwitch'][i] = 0
                    result['JobSwitch'][i+1] = 1
                else:
                    result['JobSwitch'][i] = 0


    #result['JobSwitch'].fillna(method='ffill')
    #result['Job'].fillna('',inplace=True)





    #print result[result['id'] == '0rca']

    result.to_csv('C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\jobinfo-tweetscount.csv',index=False,header = True)




#Run below line to compare job_info.csv and TwitterKevinsProject.csv and get tweets for users in job_info.csv
getTweets('C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\TwitterKevinsProject.csv', 'F:\Skydrive\ASU\CISResearchAide\job_info_new.csv')

#Run the below file to get users monthly tweet count and job switches
createOutput('C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\Jobinfo_twitter.csv', 'F:\Skydrive\ASU\CISResearchAide\job_info_new.csv')