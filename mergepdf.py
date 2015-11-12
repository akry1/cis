import pandas as pd
import csv

def merge(file):

        #header = ['handle','tweet','time']
        #writer.writerow(header)
    with open(file,'r+') as inpt:
        content = pd.read_csv(inpt,header=False)

    #Remove the first if needed
    content.drop(content.columns[[0]],axis=1,inplace= True)

    with open('C:\Users\AravindKumarReddy\Desktop\Newfolder\TwitterKevinsProject.csv','ab+') as output:
        writer = csv.writer(output)
        writer.writerows(pd.DataFrame.as_matrix(content))


merge('C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\namesDataYili1-100.csv')

merge('C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\namesDataYili101-300.csv')

merge('C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\namesDataYili1601-1800.csv')

merge('C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\namesDataYili1801-2000.csv')

merge('C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\namesDataYili2001-2400.csv')

merge('C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\namesDataYili2401-2800.csv')

merge('C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\namesDataYili2801-3200.csv')

merge('C:\\Users\\AravindKumarReddy\\Desktop\\Newfolder\\namesdatayili3201-36003.csv')