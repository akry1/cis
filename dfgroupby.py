import pandas as pd
import numpy as np


df = pd.DataFrame([ [23,24,25,25,23], 
                     [23,34,24,452,12], 
                   [243,3252,np.NaN,2,21]], columns=['H','F','m1','m2','m3'])


grouped = df.groupby('H',as_index=False)
newdf = grouped['F'].sum()

def transform(group,colname):
    return ((group['F']*group[colname])/group['F'].sum()).sum()

for i in xrange(2,len(df.count(0))):
    colname = df.columns[i]
    newdf[colname] = grouped.apply(lambda x:transform(x,colname))

newdf
#result.apply(transform).groupby('H', as_index=False)
#return pd.DataFrame({ 'H':x['H'],'F':x['F'], 'm*f':(x['F']*x['m'])/x['F'].sum()}))groupby('H',as_index=False).sum()


