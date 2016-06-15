import pandas as pd

def countHashtags(friendListFile, contentFile):
    friendList = pd.read_csv(friendListFile, header=0,dtype=str)[['handle1','friend']]
    data = pd.read_csv(contentFile, header=0,dtype=str)

    data['HashtagCount']=data['HashtagCount'].apply(lambda x:int(x))

    content = data[['handle','HashtagCount']]

    #content['HashtagCount']=content['HashtagCount'].apply(lambda x:int(x))
    #content = content.stack(level=['HashtagCount'])
    #print content
    #peruser  = content.groupby('handle',as_index=False)[['handle','HashtagCount']]
    #print peruser.nth(0)
    #print peruser.apply(lambda x:x)
    #new = pd.DataFrame(peruser.get_group()
    #print new

    #hard coding group size = number of months

    groupSize = 11

    uniqueUsers = content['handle'].unique()

    Frhashtags = []

    for i in uniqueUsers:
        friends = list(friendList[friendList['handle1']==i]['friend'])
        filtered = content[content['handle'].isin(friends)]

        if not filtered.empty :
            friendgroups = filtered.groupby('handle',as_index=False)[['handle','HashtagCount']]

            for j in xrange(groupSize):
                Frhashtags.append(sum(friendgroups.nth(j)['HashtagCount']))
        else:
            for j in xrange(groupSize):
                Frhashtags.append(0)

    data['Frhashtags'] = Frhashtags
    data.to_csv('F:\Skydrive\ASU\CISResearchAide\CompleteLongMay2015iwthHashtags_new.csv',index=False,header=True)



countHashtags('F:\Skydrive\ASU\CISResearchAide\NetworkWide113Listed.csv','F:\Skydrive\ASU\CISResearchAide\CompleteLongMay2015iwthHashtags.csv')