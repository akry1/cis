import pandas as pd

month_dict = { 'January':'1', 'February':'2', 'March':'3', 'April':'4','May':'5','June':'6','July':'7', \
                  'August':'8','September':'9','October': '10','November':'11', 'December':'12', 'Current':'10' }

def loadAndConvert(path):
    with open(path,'r') as file:
        content = pd.read_csv(file,dtype=str,header=0)

    content['start_time'] = content['start_time'].apply(formatDate)
    content['end_time'] = content['end_time'].apply(lambda x: formatDate(x,False))
    content['twitter'] = content['twitter'].apply(lambda x:x.split('/')[-1])

    content.to_csv('F:\Skydrive\ASU\CISResearchAide\job_info_new.csv', header=True,index=False)



def formatDate(d, isStart=True):
    try:
        x = str(d).split(' ')
        
        if len(x) == 2:
            return '/'.join([month_dict.get(x[0]), x[1]])
        elif len(x) == 3:
            return '/'.join([month_dict.get(x[1]), x[2]])
        elif len(x) == 1:
            if x[0].strip()=='Current' or  x[0].strip()=='2015':
                return '/'.join(['1' if isStart else '10', '2015'])
            elif x[0] != 'nan' and len(x[0])==4:
                return '/'.join(['1' if isStart else '12', x[0]])
        else:
            return None
    except:
        return None



#def formatDate(d, isStart=True):
#    x = d.split('-')

#    if len(x) == 2:
#        if int(x[1]) in xrange(16):
#            year = ''.join(['20',x[1]])
#        else:
#            year = ''.join(['19',x[1]])
#        return '/'.join([month_dict.get(x[0]), year])
#    elif len(x) == 1:
#        return '/'.join(['1' if isStart else '12', x[0]])
#    else:
#        x = d.split(' ')
#        if isStart:
#            return x[0].replace('-','/')
#        else:
#            return x[-1].replace('-','/')

 #month_dict = { 'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9, \
 #                   'Oct': 10,'Nov':11, 'Dec':12, 'Current':10 }

loadAndConvert('F:\Skydrive\ASU\CISResearchAide\job_info.csv')