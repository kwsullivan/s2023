import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
from dataclasses import dataclass
import time
import numpy as np
import warnings
from datetime import datetime
from dataclasses import dataclass

@dataclass
class IH_Max:
    date: str
    left_energy: float
    right_energy: float

warnings.filterwarnings('ignore')

conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};\
                    Server=localhost\SQLEXPRESS; \
                    Database=IH605_UPD; \
                    Trusted_Connection=yes;")

# Distinct datetimes pulled from the db
date_query = pd.read_sql_query("SELECT DISTINCT CONVERT(varchar(10), Record_Collection_Time, 111) as dates \
                            FROM [IH605_UPD].[dbo].[HeatTreat_Data_New] order by dates", conn)

dates = [datetime.strptime(curr, '%Y/%m/%d').date() for curr in date_query.dates]
for curr in date_query.dates:
    date_obj = datetime.strptime(curr, '%Y/%m/%d').date()
    dates.append(date_obj)
IH_arr = []
for date in dates:
    print(date)
    df = pd.read_sql_query(f"SELECT * FROM [IH605_UPD].[dbo].[HeatTreat_Data_New] as day_data inner join \
        (SELECT Data_Set_ID as counts_d_id, count_sets FROM \
        (SELECT Data_Set_ID, COUNT(Data_Set_ID) as count_sets \
        FROM [IH605_UPD].[dbo].[HeatTreat_Data_New] \
        GROUP BY Data_Set_ID) AS cs \
        WHERE count_sets = 457) as clean_data on day_data.Data_Set_ID = clean_data.counts_d_id \
        WHERE YEAR(Record_Collection_Time)={date.year} AND \
        MONTH(Record_Collection_Time)={date.month} AND \
        DAY(Record_Collection_Time)={date.day} \
        ORDER BY Record_Collection_Time", conn)
    ids = pd.unique(df['Data_Set_ID'])
    for curr in ids:
        time = df.loc[df['Data_Set_ID'] == curr].tail(1).Record_Collection_Time.iloc[0]
        left_energy = df.loc[df['Data_Set_ID'] == curr].tail(1).Left_Energy_Count.iloc[0]
        right_energy = df.loc[df['Data_Set_ID'] == curr].tail(1).Right_Energy_Count.iloc[0]
        IH = IH_Max(time, left_energy, right_energy)
        IH_arr.append(IH)


for curr in IH_arr:
    f.write(f"{str(curr)} {curr.left_energy} {curr.right_energy}")