# -*- coding: utf-8 -*-
import re, nltk
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import wordnet, stopwords
from gensim import corpora, models, similarities, matutils
import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
import pandas as pd

lmtzr = WordNetLemmatizer()
stops = stopwords.words('english')
#html = re.compile(r'\<[^\>]*\>')
nonan = re.compile(r'[^a-zA-Z ]')
shortword = re.compile(r'\W*\b\w{1,2}\b')
#copyright = 'copyright npr see visit httpwwwnprorg'

tag_to_type = {'J': wordnet.ADJ, 'V': wordnet.VERB, 'R': wordnet.ADV}
def get_wordnet_pos(treebank_tag):
    return tag_to_type.get(treebank_tag[:1], wordnet.NOUN)

def clean(text):
    clean_text = nonan.sub('',text)
    words = nltk.word_tokenize(shortword.sub('',clean_text))
    filtered_words = [w for w in words if not w in stops]
    tags = nltk.pos_tag(filtered_words)
    return ' '.join(
        lmtzr.lemmatize(word, get_wordnet_pos(tag[1]))
        for word, tag in zip(filtered_words, tags)
    )

#with open('nprarticles.txt','r') as f:
#    with open('corpus.txt','w') as f2:
#        text = []
#        for line in f:
#            text.append(line)
#        f2.truncate()
#        for line in text:
#            text = clean(line)
#            f2.write(text.replace(copyright,'') +'\n')
            
# Define KL function
            
def sym_kl(p,q):
    return np.sum([stats.entropy(p,q),stats.entropy(q,p)])

def gen_dictionary(corpus):    
    dictionary = corpora.Dictionary(line.split() for 
        line in corpus)
    once_ids = [tokenid for tokenid, docfreq in 
        dictionary.dfs.iteritems() if docfreq == 1]
    dictionary.filter_tokens(once_ids)
    dictionary.filter_extremes(no_above=5,keep_n=100000)
    dictionary.compactify()
    return dictionary
    
class MyCorpus(object):    
    def __init__(self,corpus,dictionary):
        self.corpus = corpus
        self.dictionary = dictionary
    def __iter__(self):
        for line in self.corpus:
            yield self.dictionary.doc2bow(line.split())

def arun(corpus,dictionary,max_topics,min_topics=1,step=1):
    kl = []
    docs = [ doc for doc in corpus]
    l = np.array([sum(cnt for _, cnt in doc) for doc in docs])
    for i in range(min_topics,max_topics,step):
        try: 
            lda = models.ldamodel.LdaModel(corpus= docs, id2word=dictionary,num_topics=i)
            m1 = lda.expElogbeta
            U,cm1,V = np.linalg.svd(m1)
            #Document-topic matrix
            lda_topics = lda[docs]
            m2 = matutils.corpus2dense(lda_topics, lda.num_topics).transpose()
            cm2 = l.dot(m2)
            cm2 += 0.0001
            cm2norm = np.linalg.norm(l)
            cm2 = cm2/cm2norm                   
            kl.append((sym_kl(cm1,cm2),i))
        except:
            break
    return kl
    
# Plot kl divergence against number ,of topics
#plt.plot(kl)
#plt.ylabel('Symmetric KL Divergence')
#plt.xlabel('Number of Topics')
#plt.savefig('kldiv.png', bbox_inches='tight')

def numberOfTopics(data):
    corpus = data.apply(clean)
    dictionary = gen_dictionary(corpus)
    my_corpus = MyCorpus(corpus,dictionary)    
    kl= arun(my_corpus,dictionary,max_topics=100)    
    if kl:
        return max(kl,key=lambda x:x[0])[1]
    else:
        return 0
    