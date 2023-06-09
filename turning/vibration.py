import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import time
import numpy as np
import warnings
import scipy.stats as stats
from scipy.fft import fft
from sklearn.cluster import KMeans
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from sqlite3 import connect
import os
import datetime
from datetime import timedelta
# from query import get_fanuc_query
from dataclasses import dataclass
#from vibration import matchCycles, makeCycles, cleanData, makeplot, readFanuc, readVib
import numpy.ma as ma
from cycles import Cycle, get_cycles

@dataclass
class Cycle:
    id: int
    start: datetime
    end: datetime
#path = "Turning/"
# os.chdir(path)
# database = 'FanucData_03May2023'
# database2 = 'FanucData_10May2023'
# vibDatabase = 'VibrationData_17May2023'
# vibDatabase2 = 'VibrationData_10May2023'
# vibtable = 'Panel1'
# fanuctable = 'OP10'
# ft2 = 'Trial1'
plt.rcParams["figure.figsize"] = (20,10)
warnings.filterwarnings('ignore')
date = "2023-05-02"
dateSplit = date.split('-')


def main():    
    # print("1. Combine Vibration Tables\n2. Combine Faunc Tables\n3. Clean Data\n4. Merge Vib and Fanuc\n5. Find Frequency\n6. Generate Vib-X Plots\n8. Write To SQL File\n9. Exit\n")
    # choice = int(input("Enter Opperation Number: "))
    # result = pd.DataFrame()
    # vibData = pd.DataFrame()
    # fanucData = pd.DataFrame()
    # while choice != 9:
    #     if choice == 1:
    #         vibData = combineVibs()
    #         print(vibData.head)
    #         denoiseVib(vibData)
    #         vibData['date_only'] = pd.to_datetime(vibData['TimeLogged'])
    #         date_only = vibData['date_only'].dt.date
    #         num_days = len(date_only.unique())
    #         print(num_days)
    #     elif choice ==2 :
    #         fanucData = combineFanuc()
    #         fanucData['date_only'] = pd.to_datetime(fanucData['TimeLogged'])
    #         date_only = fanucData['date_only'].dt.date
    #         num_days = len(date_only.unique())
    #         print(num_days)
    #         print(fanucData.head)
    #     elif choice ==3 :
    #         result = cleanData(result)
    #     elif choice ==4 :
    #         vibData = readVib('VibrationData_17May2023', 'Panel1', dateSplit[0], dateSplit[1], dateSplit[2])
    #         fanucData = readFanuc('FanucData_17May2023', 'OP10',  dateSplit[0], dateSplit[1], dateSplit[2])
    #         result = mergeAll(fanucData, vibData)
            
    #         print(result.dtypes)
    #     elif choice ==5 :
    #         mirrorDf(result)
    #     elif choice ==6 :
    #         pass
    #     elif choice ==7 :
    #         pass
    #     elif choice ==8 :
            
    #         fanucData = readFanuc('FanucData_17May2023', 'OP10',  dateSplit[0], dateSplit[1], dateSplit[2])
    #         makeCycles(fanucData)
    #     elif choice ==9:
    #         break
         
    #     print("1. Combine Vibration Tables\n2. Combine Faunc Tables\n3. Clean Data\n4. Merge Vib and Fanuc\n5. Find Frequency\n6. Generate Vib-X Plots\n7. Run Algorithm (TBM)\n8. Write To SQL File\n9. Exit\n")
    #     choice = int(input("Enter Opperation Number: "))
    fanucData = readFanuc('FanucData_17May2023', 'OP10',  dateSplit[0], dateSplit[1], dateSplit[2])
    vibData = readVib('VibrationData_17May2023', 'Panel1', dateSplit[0], dateSplit[1], dateSplit[2])
    cycles = makeCycles(fanucData)
    result = matchCycles(cycles, vibData, fanucData)
    result = cleanData(result)
    mirrorDf(result)
    #makeplot(cycles, result)


def matchCycles(cycles, df, df2):
    j = 0
    k=0
    tmp = pd.DataFrame()
    for i in cycles:
        mask = df[(df['TimeLogged'] > i.start) & (df['TimeLogged'] < i.end)]
        if  len(mask) > 0 :
            tmp = tmp.append(mask)
            k+=1
        else:
            j+=1
    # print(len(cycles))
    # print(len((df['TimeLogged'])))
    # print(k, j)
    print(tmp)
    
    result = pd.merge(df2, tmp, on='TimeLogged', how='outer').reset_index(drop = True)
    print(result)
    return result

def denoiseVib(df):
    
    #df_grouped = df.groupby(by='TimeLogged').mean()
    df = df.rolling(10)#, win_type='gaussian').mean(std=df.std().mean())
    #pass

def denoiseFanuc(df):
    
    #df_grouped = df.groupby(by='TimeLogged').mean()
    df = df.rolling(4)#, win_type='gaussian').mean(std=df.std().mean())
    #pass  

def readVib(vibDatabase, vibtable,year, month, day):
    conn_string2 = f"Driver=ODBC Driver 18 for SQL Server;\
                            Server=localhost; \
                            Database={vibDatabase}; \
                            TrustServerCertificate=yes; \
                            Trusted_Connection=yes;"

    conn = pyodbc.connect(conn_string2)
        
    vibData =  pd.read_sql_query(f"SELECT * FROM[{vibDatabase}].[dbo].[{vibtable}]\
                                  WHERE YEAR(Record_Collection_Time)={year} AND\
                                MONTH(Record_Collection_Time)={month} AND\
                                DAY(Record_Collection_Time)={day}\
                                ORDER BY Record_Collection_Time", conn)

    vibData.rename(columns={"Record_Collection_Time": "TimeLogged"}, inplace = True)

    conn.close()
    
    denoiseVib(vibData)
    return vibData

def combineVibs():

    vib1 = readVib('VibrationData_17May2023', 'Panel1')
    vib2 = readVib('VibrationData_10May2023', 'Panel1')
    
    frames = [vib1, vib2]
    vibData = pd.concat(frames)

    return vibData

def readFanuc(fanucDatabase, fanucTable, year, month, day):

    conn2 = pyodbc.connect(
        f"Driver=ODBC Driver 18 for SQL Server;Server=localhost;Database={fanucDatabase};Trusted_Connection=yes; TrustServerCertificate=yes"
        )
    cursor = conn2.cursor()

    fanuc2 = f"SELECT [TableIndex], \
        [TimeLogged], \
        CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED] as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_LOAD]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_LOAD \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_1_X]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_1_X \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_1]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_1 \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_COUNT_1]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_COUNT_1 \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_1_Y]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_1_Y \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_1_R]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_1_R \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_3_X]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_3_X \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_3_Y]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_3_Y \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_3_R]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_3_R \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_5_X]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_5_X \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_5_Y]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_5_Y \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_5_R]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_5_R \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_7_X]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_7_X \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_7_Y]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_7_Y \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_7_R]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_7_R \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_3]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_3 \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_5]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_5 \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_7]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_7 \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_COUNT_3]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_COUNT_3\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_COUNT_5]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_COUNT_5 \
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_COUNT_7]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_COUNT_7\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_1_X]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_1_X\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_SPEED]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_SPEED\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_LOAD]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_LOAD\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_1_Y]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_1_Y\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_1_R]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_1_R\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_3_X]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_3_X\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_3_Y]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_3_Y\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_3_R]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_3_R\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_5_X]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_5_X\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_5_Y]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_5_Y\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_5_R]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_5_R\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_7_X]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_7_X\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_7_Y]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_7_Y\
        ,CAST([OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_7_R]as float) as OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_7_R\
    FROM [{fanucDatabase}].[dbo].[{fanucTable}] \
    WHERE YEAR(TimeLogged)={year} AND\
    MONTH(TimeLogged)={month} AND\
    DAY(TimeLogged)={day}\
    ORDER BY TimeLogged"
    fanucData = pd.read_sql_query(fanuc2, conn2)

    conn2.close()
    
    denoiseFanuc(fanucData)
    return fanucData

def combineFanuc():
    fanuc1 = readFanuc('FanucData_03May2023', 'Trial1')
    fanuc2 = readFanuc('FanucData_10May2023', 'OP10')

    frames = [fanuc1, fanuc2]
    fanucData = pd.concat(frames)

    return fanucData

def cleanData(df):
    # this makes it so that the null fields duplicate the last value until
    # its updated to the new time
    df.fillna(method='ffill', inplace=True)
    df.fillna(method='bfill', inplace=True)
    print(df.head)
    # count = 0
    # if len(np.where(pd.isnull(df))[0]) > 0:
    #     df = df.dropna(axis=0, how="all", thresh=None, subset=None, inplace=False)
    #     count+=1
    # print("-----Nulls: ",count, "-----")
    #df = df.rolling(10).mean()
    df = df.set_index('TimeLogged')
    df = df.drop(['TableIndex'], axis=1)
    
    return df

def makeCycles(df):
    cycle_duration = timedelta(seconds=80)
    end_day = datetime.datetime.strptime(f'{date} 23:59:59', "%Y-%m-%d %H:%M:%S")
    id = 0
    cycles = []
    start_time = df.TimeLogged[0]
    while start_time < end_day:
        search = (start_time + cycle_duration).strftime("%Y-%m-%d %H:%M:%S")
        i = df.TimeLogged.searchsorted(search)
        #match = df[i:].query('OP10_MACHINE_A_LA633_SPINDLE_1_SPEED == 0').head(1)
        match = df[i:].query('OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED == 0').head(1)
        if match.size == 0: break
        i = match.index[0]
        end_time = match.TimeLogged.iloc[0]
        cycles.append(Cycle(id, start_time, end_time))
        start_time = end_time
        id += 1
        #print(cycles)
    # for i in cycles:
    #     print(i.id)
    return cycles

def mirrorDf(df):
    reversed = df.iloc[::-1]
    print(reversed.head)
    frames = [df, reversed]
    freqDf = pd.concat(frames)
    
    # freqDf.fillna(method='ffill', inplace=True)
    # freqDf.fillna(method='bfill', inplace=True)

    print("Mirrored:")
    print(freqDf.head)
    
    freqArr = freqDf['Sensor1'].to_numpy()
    print(np.sort(freqArr))

    freqArr2 = freqDf['Sensor2'].to_numpy()
    print(freqArr2)
    
    freqArr = freqDf['Sensor3'].to_numpy()
    print(freqArr)

    freqArr2 = freqDf['Sensor4'].to_numpy()
    print(np.sort(freqArr2))
    
    calcFreq(freqArr, 'senor1')    
    calcFreq(freqArr2, 'senor2')
    calcFreq(freqArr, 'senor3')    
    calcFreq(freqArr2, 'senor4')

def makeplot(cycles, result):
    for i in range(0, len(cycles)):
        start = cycles[i].start
        end = cycles[i].end
        spindle = result.between_time(str(start.time()), str(end.time()), inclusive='both')
        print(spindle)
        vibration = result.between_time(str(start.time()), str(end.time()), inclusive='both')
        print(vibration)
        fig, ax = plt.subplots()
        ax2 = ax.twinx()
        ax.set_xlabel('Time')
        ax.set_ylabel('Spindle Speed')
        ln = ax.plot(spindle.OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED, alpha=0.15, color='blue', label='Spindle Load')
        ax2.set_ylabel('Vibration')
        ln2 = ax2.plot(vibration.Sensor1, alpha=0.15, color='red', label='Vibration')
        leg = ln + ln2
        labs = [l.get_label() for l in leg]
        ax2.legend(leg, labs, loc=0)
        opath = f'Turning/charts/{date}'
        if not os.path.isdir(opath):
            os.makedirs(opath)
        name = f'{opath}/{i}'
        fig.savefig(name)
        # fig, ax = plt.subplots()
        # ax2 = ax.twinx()
        # ax.set_xlabel('Time')
        # ax.set_ylabel('Spindle Speed')
        # ln = ax.plot(result.OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED, alpha=0.15, color='blue', label='Spindle Load')
        # ax2.set_ylabel('Vibration')
        # ln2 = ax2.plot(result.Sensor1, alpha=0.15, color='red', label='Vibration')
        # leg = ln + ln2
        # labs = [l.get_label() for l in leg]
        # ax2.legend(leg, labs, loc=0)
        # opath = f'Turning/charts/{date}'
        # if not os.path.isdir(opath):
        #     os.makedirs(opath)
        # name = f'{opath}/fullday'
        # fig.savefig(name)
def reject_outliers(data, m = 2.):
    d = np.abs(data - np.median(data))
    mdev = np.median(d)
    s = d/mdev if mdev else np.zeros(len(d))
    return data[s<m]
def calcFreq(arr, sensorNum):
    # toRemove = []
    # for i in range(0,len(arr0)):
    #     if arr0[i] > 134:
    #         toRemove.append(i)
    # print(toRemove)
    # arr =  np.delete(arr0, toRemove)
    # print(np.sort(arr))
    
    arr = reject_outliers(arr, 2)
    vol = fft(arr)
    N = len(vol)
    n = np.arange(N)
    samplingRate = 1/10
    T = N*samplingRate
    freq = n/T
    freq = freq[:N//2]
    y = (2/N)*np.abs(vol[0:int(N/2)])
    plt.figure()
    plt.plot(freq,y)
    # # Get the one-sided specturm
    # # n_oneside = N//2
    # # # get the one side frequency
    # # f_oneside = freq[:n_oneside]

    
    # # plt.plot(f_oneside, np.abs(vol[:n_oneside]), 'b')
    plt.xlabel('Freq (Hz)')
    plt.ylabel('FFT Amplitude |X(freq)|')

    # f = filename.split('.')
    pltName = sensorNum + 'merged_freqPlot.png'
    plt.savefig('Turning/freqPlots/' +pltName)

def writeToSQL(df, database):
    conn_string2 = f"Driver=ODBC Driver 18 for SQL Server;\
                            Server=localhost; \
                            Database={database}; \
                            TrustServerCertificate=yes; \
                            Trusted_Connection=yes;"

    print("connected")
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_string2})

    engine = create_engine(connection_url)
    curr = engine.connect(conn_string2)
    print(curr)

    curr.execute('CREATE TABLE IF NOT EXISTS [vibDataMerged] (TimeLogged datetime64, \
             [OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED]  float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_LOAD] float\
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_1_X] float)  \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_1] float\
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_COUNT_1] float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_1_Y] float\
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_1_R] float\
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_3_X] float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_3_Y] float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_3_R] float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_5_X] float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_5_Y] float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_5_R] float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_7_X] float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_7_Y] float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_OFFSET_7_R] float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_3] float\
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_5] float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_7] float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_COUNT_3] float\
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_COUNT_5] float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_COUNT_7] float\
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_1_X] float\
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_SPEED] float  \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_LOAD] float \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_1_Y] float)\
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_1_R] float) \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_3_X] float) \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_3_Y] float)\
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_3_R] float)\
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_5_X] float)\
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_5_Y] float)\
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_5_R] float) \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_7_X] float)\
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_7_Y] float) \
        ,[OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_2_TOOL_OFFSET_7_R] float)\
        ,Data_Set_ID NUMBER\
        ,Sensor1 float \
        ,Sensor2 float \
        ,Sensor3 float \
        ,Sensor4 float')
     # commit the query
    curr.commit()
    
    curr.execute(f'SELECT * FROM {database}.[dbo].[vibDataMerged]')
    with engine.begin() as connection:
    # # write the DataFrame to a table in the sql database
        df.to_sql("vibDataMerged", con=connection, if_exists='append', schema='dbo') 
    # # # fetch all the records from the table
    
    # with engine.connect() as conn:
    #     curr.execute(f'SELECT * FROM {database}.[dbo].[vibDataMerged]').fetchall()
main()