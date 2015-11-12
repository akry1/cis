import pandas as pd

def loadInput(dataPath, termsPath):
    with open(dataPath, 'rb') as file:
        content = pd.read_csv(file, dtype = str)
        textdata = list(content['ans_content'].str.strip().str.lower())

    with open(termsPath, 'rb') as file:
        terms_raw = pd.read_csv(file,names=['term'],header=None, dtype=str)
        terms = list(terms_raw['term'].str.strip().str.lower())

    return textdata, terms


def mapper(text,terms):
    count = 0
    for i in terms:
        if text.find(i) != -1 :
            count +=1
    return (text,count)


def mapReduce(dataPath, termsPath):
    data, terms = loadInput(dataPath,termsPath)
    result = []
    for i in data[:10]:
        result.append(mapper(i,terms))
    return result

mapReduce('F:\Skydrive\ASU\CISResearchAide\YahooData.csv', 'F:\Skydrive\ASU\CISResearchAide\MedicalTerms.csv')