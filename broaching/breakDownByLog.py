import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import pyodbc
from datetime import *
#from readBroachData import *
#from corrCoeff_broaching import *
db = "BR607_05April"
tbl = "Broach_Data_New"
database = 'BR607_APR'
table = 'Broach_Data_New'
def readData(date1, date2):
    conn = pyodbc.connect(
        'Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=BR607_05April;Trusted_Connection=yes; TrustServerCertificate=yes'
    )
    df = pd.read_sql_query(f'SELECT * FROM {db}.[dbo].{tbl} WHERE Record_Collection_Time BETWEEN ? AND ?', conn, params=[date1, date2])
    df.rename(columns = {'Commanded Speed ':'CommandedSpeed', 'Commanded Pressure':'CommandedPressure', 'Feedback Speed':'FeedbackSpeed', 'Feedback Pressure':'FeedbackPressure'}, inplace = True)
    
    # date_query = pd.read_sql_query(f"SELECT DISTINCT CONVERT(varchar(10), Record_Collection_Time, 111) as dates \
    # FROM [{db}].[dbo].[{table}] order by dates", conn)
    return df

def getCycleDuration(df):
    
    dict = df['Data_Set_ID'].value_counts().to_dict()
    print(dict)
    for index, row in df.iterrows():
        if row['Data_Set_ID'] in dict:
            dfr = df.rolling(dict.get(row['Data_Set_ID']), win_type='gaussian').mean(std=df.std().mean())#should be odd number
        


def getByLog():
    toBeReturned = pd.DataFrame()
    loggedChanges =['2023-01-31', '2023-02-01','2023-02-15', '2023-02-16']#,'2023-02-17', '2023-02-22', '2023-02-27','2023-03-16']
    for i in range(0, len(loggedChanges)-1):
        print("DATE RANGE: ",loggedChanges[i], "-", loggedChanges[i+1])
        df = readData(loggedChanges[i], loggedChanges[i+1])
        print(df)
        toBeReturned = pd.concat([toBeReturned, df])
        #getCycleDuration(df)
    return toBeReturned
        
#df = getByLog()
#print(df)
#readData()

