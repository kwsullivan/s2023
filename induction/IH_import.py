import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
from dataclasses import dataclass
import time
import numpy as np
import warnings

warnings.filterwarnings('ignore')

@dataclass
class EnergyLevel:
    id: int
    time: str
    servo_position: float
    left_primary_quench: float
    left_energy_count: float
    right_primary_quench: float
    right_energy_count: float

datasets = []

conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};\
                    Server=localhost\SQLEXPRESS; \
                    Database=IH605; \
                    Trusted_Connection=yes;")

data = pd.read_sql_query("SELECT * FROM [IH605].[dbo].[HeatTreat_Data] \
                        WHERE Record_Collection_Time \
                        BETWEEN '2023-01-16 06:00:00' \
                        AND '2023-01-16 17:00:00' \
                        ORDER BY Record_Collection_Time", conn)

for dataset_id in range(0, (data.Data_Set_ID[len(data.Data_Set_ID) - 1] - 1)):
    dataset_query = pd.read_sql_query(f"SELECT * FROM [IH605].[dbo].[HeatTreat_Data] \
                        WHERE Record_Collection_Time \
                        BETWEEN '2023-01-16 06:00:00' \
                        AND '2023-01-16 17:00:00' \
                        AND Data_Set_ID = {dataset_id + 1} \
                        ORDER BY Record_Collection_Time", conn)
    if len(dataset_query) == 457:
        print(f'ADDING DATASET {dataset_id + 1}')
        dataset = []
        for i in range(0, len(dataset_query)):
            dataset.append(EnergyLevel(dataset_query.Data_Set_ID[i],
            dataset_query.Record_Collection_Time[i],
            dataset_query.Servo_Position[i],
            dataset_query.Left_Primary_Quench_Lm[i],
            dataset_query.Left_Energy_Count_KWs[i],
            dataset_query.Right_Primary_Quench_Lm[i],
            dataset_query.Right_Energy_Count_KWs[i]))
        
        datasets.append(dataset)
        print(f'ADDED DATASET {dataset_id + 1}')


ec_arr = []
for dataset in datasets:
    ec = []
    for data in dataset:
        ec.append(data.energy_count)
    ec_arr.append(ec)

x = np.arange(522)
max_ec = []
for max in ec_arr:
    max_ec.append(max[len(max) - 1])
    
# current_id = data.Data_Set_ID[0]
# count = 0
# i = 0
# for i in range(0, len(data.ID)):
#     dataset = []
#     if data.Data_Set_ID[i] == current_id:

#         count += 1
#     else:
#         current_id = data.Data_Set_ID[i]
#         print(f'current_id: {current_id}, count: {count}')
#         count = 0