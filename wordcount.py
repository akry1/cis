import pandas as pd

def loadInput(dataPath, termsPath):
    with open(dataPath, 'rb') as file:
        content = pd.read_csv(file, dtype = str)
        textdata = list(content['ans_content'].str.strip().str.lower())

    with open(termsPath, 'rb') as file:
        terms_raw = pd.read_csv(file,names=['term'],header=None, dtype=str)
        terms = list(terms_raw['term'].str.strip().str.lower())

    return content[0:100],textdata[0:100], terms


def mapper(text,terms):
    count = 0
    for i in terms:
        if str(text[1]).find(i) != -1 :
            count +=1
    return (text[0],count)

def mapReduce(dataPath, termsPath):
    content, data, terms = loadInput(dataPath,termsPath)
    data = [ (i,j) for i,j in enumerate(data)]
    result = []
    rdd = sc.parallelize(data,4)
    result = rdd.map(lambda x: mapper(x,terms)).collect()

    #for i in data:
        #result.append(mapper(str(i),terms))

    sorted(result, key = lambda x:x[0])    
    content['MedicalTerms'] = map(lambda x:x[1],result)    
    content.to_csv('result.csv',header=True)

import time

def main():
    start = time.time()
    mapReduce('F:\Skydrive\ASU\CISResearchAide\YahooData.csv', 'F:\Skydrive\ASU\CISResearchAide\MedicalTerms.csv')
    #mapReduce('YahooData.csv', 'MedicalTerms.csv')
    print time.time()-start

main()