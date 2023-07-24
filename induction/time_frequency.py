import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
from dataclasses import dataclass
import time
import numpy as np
import warnings
from datetime import datetime
import datetime

database = 'BR607_UPD'
table = 'Broach_Data_New'
plt.rcParams["figure.figsize"] = (20,10)
warnings.filterwarnings('ignore')
conn_string = f"Driver={{ODBC Driver 17 for SQL Server}};\
                    Server=localhost\SQLEXPRESS; \
                    Database={database}; \
                    Trusted_Connection=yes;"
conn = pyodbc.connect(conn_string)
ranges = []
indexes = []
# Distinct datetimes pulled from the db
date_query = pd.read_sql_query(f"SELECT DISTINCT CONVERT(varchar(10), Record_Collection_Time, 111) as dates \
                            FROM [{database}].[dbo].[{table}] order by dates", conn)
dates = [datetime.datetime.strptime(curr, '%Y/%m/%d').date() for curr in date_query.dates]
index = np.arange(0, 24, 1)

for date in dates:
    df = pd.read_sql_query(f"SELECT * FROM [{database}].[dbo].[{table}] as day_data inner join \
        (SELECT Data_Set_ID as counts_d_id, count_sets FROM \
        (SELECT Data_Set_ID, COUNT(Data_Set_ID) as count_sets \
        FROM [{database}].[dbo].[{table}] \
        GROUP BY Data_Set_ID) AS cs \
        WHERE count_sets = 457) as clean_data on day_data.Data_Set_ID = clean_data.counts_d_id \
        WHERE \
        YEAR(Record_Collection_Time)={date.year} AND \
        MONTH(Record_Collection_Time)={date.month} AND \
        DAY(Record_Collection_Time)={date.day} \
        ORDER BY Record_Collection_Time", conn)
    frame = df.Data_Set_ID.groupby(pd.to_datetime(df.Record_Collection_Time).dt.hour).count()
    range = [0] * 24
    for key in frame.keys():
        range[key] = frame[key]
    ranges.append(range)
    indexes.append(index)
    plt.bar(index, range, width=0.8)
    plt.title(f'Hourly Activity ({date})')
    plt.xlabel('Hour')
    plt.ylabel('Data Points')
    plt.xticks(index)
    plt.grid()
    plt.text(.01, .99, f"Total:{sum(range)}", ha='left', va='top', transform=plt.gca().transAxes)
    plt.savefig(f'./fig/broach/activity/hourly/{date}.png')
    plt.clf()
