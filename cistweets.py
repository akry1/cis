import pandas as pd
import re
import sys
import time

def loadInput(dataPath):
    with open(dataPath, 'rb') as file:
        content = pd.read_csv(file, dtype = str)
        textdata = list(content['tweet'].str.strip())


    return content,textdata


def mapper(text):
    pattern = re.compile(u'(#\S*)')    
    return (text[0],len(re.findall(pattern,text[1])))

def mapReduce(dataPath):
    content, data = loadInput(dataPath)
    data = [ (i,j) for i,j in enumerate(data)]
    result = []
    rdd = sc.parallelize(data,4)
    result = rdd.map(mapper).collect()

    #for i in data:
        #result.append(mapper(str(i),terms))

    result = sorted(result, key = lambda x:x[0])    
    pd.DataFrame.insert(content,0,'#tagscount',map(lambda x:x[1],result)  )  
    content.to_csv('TweetsResult.csv',header=True,index=False)

def main(argv):
    start = time.time()
    mapReduce('F:\Skydrive\ASU\CISResearchAide\SentimentedTweetsPartyAugMay.csv')
    #mapReduce('YahooData.csv', 'MedicalTerms.csv')
    print (time.time()-start)/60

if __name__ == "__main__":
    main(sys.argv)