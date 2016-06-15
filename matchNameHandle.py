# -*- coding: utf-8 -*-

import pandas as pd



def mergeNameHandles(userlistpath,handlelistpath,output):
    userlist = pd.read_csv(userlistpath,header=0,dtype=str)
    handlelist = pd.read_csv(handlelistpath,header=0,dtype=str)
    
    #approach 1
    #result = pd.merge(userlist,handlelist,how='left',left_on='Name',right_on='Stripped Name')
    #match remaining using min edit distance  
        
    #approach 2
    nameHandle = handlelist[['Stripped Name','Handle']]
    nameHandle.index = nameHandle['Stripped Name']
    nameHandleDict =  nameHandle[[1]]
    #userlist['Handle2'] = userlist['Name'].apply(lambda x:findSimilarName(x,nameHandleDict))
    userlist['Handle'] = userlist['Name'].apply(lambda x:findSimilarLastname(x,nameHandleDict))    
    #print len(userlist[map(lambda x:True if x is None else False,userlist['Handle2'])])
    print len(userlist[map(lambda x:True if x is None else False,userlist['Handle'])])
    userlist.to_csv(output,index=False,header=True)
    
def findSimilarName(name,handles):
    if name:
        for i in handles.itertuples():
            if i and minEditDistance(name.lower(),i[0].lower()):
                return i[1]
        lastname = name.split(' ')[-1].lower()
        for i in handles.itertuples():
            if i[0].lower().find(lastname)!=-1:
                firstname = name.split(' ')[0].lower()
                if minEditDistance(firstname,i[0].lower().replace(lastname,'').strip(),0.5):
                    return i[1]
        
                
                
def findSimilarLastname(name,handles):
    if name:
        lastname = name.split(' ')[-1].lower()
        for i in handles.itertuples():
            if i[0].lower().find(lastname)!=-1:
                firstname = name.split(' ')[0].lower()
                if minEditDistance(firstname,i[0].lower().replace(lastname,'').strip(),0.5):
                    return i[1]
        for i in handles.itertuples():
            if i and minEditDistance(name.lower(),i[0].lower(),0.2):
                return i[1]   
    
def minEditDistance(a,b,error=0.1):
    D = {(-1,-1):0}

    lena = range(len(a))
    lenb = range(len(b))
    for i in lena:
        D[(i,-1)] = i+1
    for j in lenb:
        D[(-1,j)] = j+1

    for i in lena:
        for j in lenb:
            if a[i] == b[j]: sub = 0
            else: sub = 1
            D[(i,j)] = min(D[(i-1,j)]+1, D[(i,j-1)]+1, D[(i-1,j-1)]+sub)    

    return True if D[(len(a)-1,len(b)-1)]/float(len(a)) < error else False
    
mergeNameHandles('senators_114th.csv','senatesRAW114th_handle.csv','senators114th_handle.csv')
#mergeNameHandles('HouseOfRepresentatives_114th.csv','congressman114th_handle.csv','HouseOfRepresentatives114th_handle.csv')