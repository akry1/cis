
import pandas as pd
from nltk.corpus import stopwords 
from nltk.util import ngrams
import re,nltk, numpy as np
import csv

#read files
tweets = pd.read_csv('D:/CIS data/reps_113th_withsentimentscore.csv', dtype=str)
party = pd.read_csv('D:/CIS data/HouseOfRepresentatives_handle_final.csv', dtype=str,header=0)
sourcefile = pd.read_csv('CompleteLongMay2015iwthHashtagsTopicsSentimentCount.csv',header=0,dtype=str)

#process data
tweets.columns = ['Handle','tweet','time','sentiment']
tweets['Handle'] = tweets['Handle'].str.lower()
tweets['tweet'] = tweets['tweet'].str.lower()
party['Handle'] = party['Handle'].str.lower()
combined  = pd.merge(tweets,party, on='Handle', how='left')[['Party','tweet']]

democratTweets = combined[combined['Party']=='D']
republicanTweets = combined[combined['Party']=='R']

stops = stopwords.words('english')
nonan = re.compile(r'[^a-zA-Z ]')
shortword = re.compile(r'\W*\b\w{1,2}\b')
longword = re.compile(r'\W*\b\w{20,}\b')

def cleanTweet(text):
    clean_text = nonan.sub('',text)
    clean_text = longword.sub('',clean_text)
    words = nltk.word_tokenize(shortword.sub('',clean_text))        
    if n==1:
        words_groups = [ w for w in words if not w in stops]
    else:
        words_groups = ngrams(words,n)
    return words_groups
  
def processCorpus(corpus, n,total_counts={}):
    word_counts={}                    
    for i in corpus:
        for w in cleanTweet(str(i)):
            if n!=1:
                w = tuple(w)
            word_counts[w] = word_counts.get(w,0)+1
            if total_counts:
                total_counts[w] = total_counts.get(w,0)+1   
    
    if total_counts:    
        return (word_counts, total_counts)
    else:
        return word_counts
    
for n in range(1,4):    
    democrat_counts = processCorpus(democratTweets['tweet'],n)
    republican_counts, total_counts = processCorpus(republicanTweets['tweet'],n, democrat_counts.copy())
    
    #top 100  
    democrat_words = sorted(democrat_counts, key = democrat_counts.get, reverse=True)[:100]
    republican_words = sorted(republican_counts, key = republican_counts.get, reverse=True)[:100]
    combined_words = sorted(total_counts, key = total_counts.get , reverse=True)[:100]   
    
    democrat_words1 = [ [i] for i in democrat_words]
    republican_words1 = [ [i] for i in republican_words]
    combined_words1 = [ [i] for i in combined_words]    
    
    with open('democrat'+str(n)+'.csv','wb') as output:
        writer = csv.writer(output,lineterminator='\n',delimiter=' ')
        writer.writerows(democrat_words1)

     
    with open('republican'+str(n)+'.csv','wb') as output:
        writer = csv.writer(output,lineterminator='\n',delimiter=' ')
        writer.writerows(republican_words1)
       
    with open('combined'+str(n)+'.csv','wb') as output:
        writer = csv.writer(output,lineterminator='\n',delimiter=' ')
        writer.writerows(combined_words1)
    
#    pd.DataFrame(democrat_words).to_csv('democrat'+str(n)+'.csv',index=None, header=None)
#    pd.DataFrame(republican_words).to_csv('republican'+str(n)+'.csv',index=None, header=None)
#    pd.DataFrame(combined_words).to_csv('combined'+str(n)+'.csv',index=None, header=None)

    #inverse  
    democrat = { i:1.0/(j+1) for j,i in enumerate(democrat_words)}  
    republican = {i:1.0/(j+1) for j,i in enumerate(republican_words)}  
    combined = { i:1.0/(j+1) for j,i in enumerate(combined_words)}

    print democrat
    
    democrat_score = []
    republican_score = []
    combined_score = []
    tokenized_tweets = [] 
    
    for text in tweets['tweet']:
#        clean_text = nonan.sub('',str(text))
#        clean_text = longword.sub('',clean_text)
#        words = nltk.word_tokenize(shortword.sub('',clean_text))        
#        if n==1:
#            words_groups = [ w for w in words if not w in stops]
#        else:
#            words_groups = ngrams(words,n)
        
        tokenized_tweets.append(cleanTweet(str(text)))
        
    for i in tokenized_tweets:
        d_score = 0
        r_score = 0
        t_score = 0
        for word in i:
            if n!=1:
                word = tuple(word)
            d_score += democrat.get(word,0)
            r_score += republican.get(word,0)
            t_score += combined.get(word,0)
        democrat_score.append(d_score)
        republican_score.append(r_score)
        combined_score.append(t_score)
        
    tweets['democrat_score-'+str(n)+'gram'] = democrat_score
    tweets['republican_score-'+str(n)+'gram'] = republican_score
    tweets['combined_score-'+str(n)+'gram'] = combined_score

#tweets.to_csv('reps_113th_withwordscores.csv',index=None)

#merge with output
month_dict={''.join([str(j),str(l)]): i+12*k-5 for i,j in enumerate(xrange(1,13)) for k,l in enumerate([2013,2014]) if i+12*k > 5 and i+12*k-5<12}
def monthcode(time):
    try:
        dateSplit = time.strip().split()[0].split('-')
        month = int(dateSplit[1])
        if month == 5:
            if int(dateSplit[2]) > 24:
                return 0
        return month_dict.get(''.join([str(month),dateSplit[0]]),0)
    except:
        return 0
        
handleCol = sourcefile['handle'].copy()
sourcefile['handle'] = sourcefile['handle'].str.lower()
users = sourcefile['handle']

tweets['handle'] = tweets['Handle']
tweets = tweets[tweets['handle'].isin(users)]
tweets['monthcode'] = tweets['time'].apply(monthcode)    
tweets = tweets[tweets['monthcode'] != 0]

grouped = tweets.groupby(['handle','monthcode'])#['democrat_score','republican_score','combined_score']
newDF = pd.DataFrame(columns=['handle','month','democrat_score-1gram','republican_score-1gram','combined_score-1gram',
                              'democrat_score-2gram','republican_score-2gram','combined_score-2gram','democrat_score-3gram','republican_score-3gram','combined_score-3gram'], index=np.arange(0,len(grouped)))       
   
i=0
for name,group in grouped:
    newDF.loc[i] = [name[0],str(name[1]),str(sum(group['democrat_score-1gram'])),str(sum(group['republican_score-1gram'])),str(sum(group['combined_score-1gram'])),
              str(sum(group['democrat_score-2gram'])),str(sum(group['republican_score-2gram'])),str(sum(group['combined_score-2gram'])),
              str(sum(group['democrat_score-3gram'])),str(sum(group['republican_score-3gram'])),str(sum(group['combined_score-3gram']))]    
    i+=1

result = pd.merge(sourcefile,newDF, how='left', on=['handle','month'])
result['democrat_score-1gram'].fillna(0,inplace=True)
result['republican_score-1gram'].fillna(0,inplace=True)
result['combined_score-1gram'].fillna(0,inplace=True)
result['democrat_score-2gram'].fillna(0,inplace=True)
result['republican_score-2gram'].fillna(0,inplace=True)
result['combined_score-2gram'].fillna(0,inplace=True)
result['democrat_score-3gram'].fillna(0,inplace=True)
result['republican_score-3gram'].fillna(0,inplace=True)
result['combined_score-3gram'].fillna(0,inplace=True)
result['handle'] = handleCol
result.to_csv('CompleteLongMay2015iwthHashtagsWordScores.csv',index=None)
    
    
 