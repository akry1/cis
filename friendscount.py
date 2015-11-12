import pandas as pd

def countHashtags(friendListFile, tweetContentFile):
    content = pd.read_csv(friendListFile, header=0,dtype=str)
    tweetcontent = pd.read_csv(tweetContentFile, header=0,dtype=str)
    
    tweetcontent['#tagscount']=tweetcontent['#tagscount'].apply(lambda x:int(x))
    tagscount  = tweetcontent.groupby('handle',as_index=True)['#tagscount'].sum()
    content['#tagscount'] = content['friend'].apply(lambda x:tagscount.get(x,0))
    
    content.to_csv('F:\Skydrive\ASU\CISResearchAide\NetworkWide113Listed_new.csv',index=False,header=True)



countHashtags('F:\Skydrive\ASU\CISResearchAide\NetworkWide113Listed.csv','F:\Skydrive\ASU\CISResearchAide\TweetsResult.csv')