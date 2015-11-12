import pandas as pd
import numpy as np
import re

def loadfile(f):
    with open(f,'rb') as input:
        content = pd.read_csv(input, dtype=str)
    rows =  content[['handle','time','#tagscount']]
    return pd.DataFrame.as_matrix(rows)

def mapper(f):
    rows = loadfile(f)
    month_dict={''.join([str(j),str(l)]): i+12*k-5 for i,j in enumerate(xrange(1,13)) for k,l in enumerate([2013,2014]) if i+12*k > 5}
    #print month_dict
    rdd = sc.parallelize(rows)
    res = (rdd
           .map(lambda x: ((x[0], month_dict.get(''.join(re.search('(\d+)/\d+/(\d+)',x[1]).groups()))),int(x[2])))
           .reduceByKey(lambda a,b:a+b)
           .collect())
           #.map(lambda (a,b):[a[0],a[1],b])
    return res

def merger(f1,f2):
    hashcounts = mapper(f1)
    hashcounts_dict = { i[0]:i[1] for i in hashcounts }
    #hashcounts_dict = {('RepMGriffith', 8): 4, ('RepChrisGibson', 9): 44, ('RepZoeLofgren', 8): 46, ('RepJoeCourtney', 3): 6, ('BillOwensNY', 9): 0, ('CongCulberson', 2): 3, ('BruceBraley', 9): 93, ('RepPhilGingrey', 4): 17, ('RepMcGovern', 5): 17, ('repdavidscott', 4): 38, ('collinsNY27', 6): 14, ('RepCuellar', 8): 90, ('LacyClayMO1', 11): 21, ('TomLatham', 1): 17, ('RepMikeMcIntyre', 10): 7}
    inorder = []
    with open(f2,'rb') as input:
        content = pd.read_csv(input, dtype=str)
    ids = list(content['handle'])

    prev = None
    for i in ids:
        if i!= prev: 
            j = 1
            prev = i
        inorder.append(hashcounts_dict.get((i,j),0))
        j+=1
    content['HashtagCount'] = inorder
    content.to_csv('HashtagCountResult.csv',header=True,index=False)

merger('F:\Skydrive\ASU\CISResearchAide\TweetsResult.csv','F:\Skydrive\ASU\CISResearchAide\CompleteLongMay2015.csv')