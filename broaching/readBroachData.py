import pyodbc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

def readData():
    conn = pyodbc.connect(
        'Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=BR607;Trusted_Connection=yes; TrustServerCertificate=yes'
    )
    cursor = conn.cursor()
    #cursor.execute('SELECT * FROM dbo.Broach_Data')

    # for i in cursor:
    #     print(i)


    df = pd.read_sql_query('SELECT * FROM BR607_12April.dbo.Broach_Data_New', conn)
    df.rename(columns = {'Speed L/m ':'Speed_Lm', 'Pressure Bar':'Pressure_Bar', 'Distance mm':'Distance_mm'}, inplace = True)
    return df
    
#FIXME
def resampledBySec():
    
    for i in listDates:
        #print(i.year)
        query = """
        SELECT * FROM dbo.Broach_Data WHERE 
        YEAR(Record_Collection_Time)=? 
        AND MONTH(Record_Collection_Time)=?
        AND DAY(Record_Collection_Time)=?
        """
        temp =  pd.read_sql_query(query, conn, params=[i.year, i.month, i.day])
        temp.rename(columns = {'Speed L/m ':'Speed_Lm', 'Pressure Bar':'Pressure_Bar', 'Distance mm':'Distance_mm'}, inplace = True)
        #print(temp)

        # occurrence =  """
        # select num_sets.count_sets, count(num_sets.count_sets) as num_occurences from
        # (SELECT Data_Set_ID, COUNT(Data_Set_ID) as count_sets FROM [BR607].[dbo].[Broach_Data]
        # group by Data_Set_ID) as num_sets group by num_sets.count_sets order by num_sets.count_sets
        # """
        # occ = pd.read_sql_query(occurrence, conn)

        # maxCycles = temp.loc[temp['Data_Set_ID'].idxmax()]
        # print(int(maxCycles['Data_Set_ID']))
    
        temp.plot(x='Record_Collection_Time', y='Pressure_Bar')
        temp.resample('S', on='Record_Collection_Time').max().plot(y =temp["Pressure_Bar"], style="-o", figsize=(15, 5))
        plt.xticks(rotation='vertical')
        plt.xlabel("Resamples By seconds")
        plt.ylabel("Pressure")
        plt.savefig(i.strftime("%Y-%m-%d")+ ".png")


def groupedByCycleAndDay():
    for i in listDates:
        #print(i.year)
        query = """
        SELECT * FROM dbo.Broach_Data WHERE 
        YEAR(Record_Collection_Time)=? 
        AND MONTH(Record_Collection_Time)=?
        AND DAY(Record_Collection_Time)=?
        """
        temp =  pd.read_sql_query(query, conn, params=[i.year, i.month, i.day])
        temp.rename(columns = {'Speed L/m ':'Speed_Lm', 'Pressure Bar':'Pressure_Bar', 'Distance mm':'Distance_mm'}, inplace = True)
        print(temp)

        fig, axs = plt.subplots(figsize=(15, 5))
        temp.groupby(temp["Record_Collection_Time"].dt.second, "Data_Set_ID")["Pressure_Bar"].mean().plot(
            x='Record_Collection_Time', y= "Pressure_Bar", style="-o"
            )

        plt.xticks(rotation='vertical')
        plt.xlabel("Grouped By Cycle and day")
        plt.ylabel("Pressure")
        plt.savefig(i.strftime("%Y-%m-%d")+ ".png")

def cycleAndDayAttempt2():
       for i in listDates:
        #print(i.year)
        query = """
        SELECT * FROM dbo.Broach_Data WHERE 
        AND (YEAR(Record_Collection_Time)=?
        AND MONTH(Record_Collection_Time)=?
        AND DAY(Record_Collection_Time)=?)
        """
        temp =  pd.read_sql_query(query, conn, params=[i.year, i.month, i.day, ])#here lies the damn issue
        temp.rename(columns = {'Speed L/m ':'Speed_Lm', 'Pressure Bar':'Pressure_Bar', 'Distance mm':'Distance_mm'}, inplace = True)
        print(temp)



        # fig, axs = plt.subplots(figsize=(15, 5))
        # temp.groupby(temp["Record_Collection_Time"].dt.second, "Data_Set_ID")["Pressure_Bar"].mean().plot(
        #     x='Record_Collection_Time', y= "Pressure_Bar", style="-o"
        #     )

        # plt.xticks(rotation='vertical')
        # plt.xlabel("Grouped By Cycle and day")
        # plt.ylabel("Pressure")
        # plt.savefig(i.strftime("%Y-%m-%d")+ ".png")



df = readData()
numDates = pd.to_datetime(df['Record_Collection_Time']).dt.date.nunique()
print(numDates)
listDates = []

listDates = pd.to_datetime(df['Record_Collection_Time']).dt.date.unique().tolist()
print(listDates)
#groupedByCycleAndDay()