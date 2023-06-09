import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import time
import numpy as np
import datetime
from datetime import datetime as dt
from datetime import timedelta
import os
from querylist import get_fanuc_query
from dataclasses import dataclass

@dataclass
class Cycle:
    id: int
    start: datetime
    end: datetime

@dataclass
class ToolTime:
    series: pd.core.series.Series
    start: pd._libs.tslibs.timestamps.Timestamp
    end: pd._libs.tslibs.timestamps.Timestamp

def get_spindle_data(date):
    split = date.split('-')

    database = 'FanucData_GOOD'
    table = 'OP10'
    query = get_fanuc_query(database, table, split[0], split[1], split[2])

    conn_string = f"Driver={{ODBC Driver 17 for SQL Server}};\
                        Server=localhost\SQLEXPRESS; \
                        Database={database}; \
                        Trusted_Connection=yes;"

    conn = pyodbc.connect(conn_string)
    df = pd.read_sql_query(query, conn)
    df['Timestamp'] = pd.to_datetime(df['TimeLogged'], format = '%Y-%m-%d %H:%M:%S.%f')
    return df

def get_vibration_data(date):
    split = date.split('-')
    database = 'VibrationData_GOOD'
    table = 'Sensor1'
    vib_query = get_vibration_query(database, table, split[0], split[1], split[2])

    conn_string = f"Driver={{ODBC Driver 17 for SQL Server}};\
                        Server=localhost\SQLEXPRESS; \
                        Database={database}; \
                        Trusted_Connection=yes;"

    conn = pyodbc.connect(conn_string)
    df = pd.read_sql_query(query, conn)
    df = pd.read_sql_query(vib_query, conn)
    df = df[(df.Sensor1 >= 0) & (df.Sensor1 < 100)]
    df['Timestamp'] = pd.to_datetime(df['Record_Collection_Time'], format = '%Y-%m-%d %H:%M:%S.%f')
    return df

def get_cycles(df):
    cycle_duration = timedelta(seconds=80)
    date = (str(df.Timestamp[0])).split(' ')[0]
    # end_day = datetime.datetime.strptime(f'{date} 23:59:59', "%Y-%m-%d %H:%M:%S")
    end_day = datetime.datetime.strptime(f'{date} 23:59:59', "%Y-%m-%d %H:%M:%S")
    id = 0
    cycles = []
    start_time = df.TimeLogged[0]
    length = len(df)
    # While still in current day
    while start_time < end_day:
        # Search from current time to threshold below minimum cycle length (80s)
        search = (start_time + cycle_duration).strftime("%Y-%m-%d %H:%M:%S")
        i = df.TimeLogged.searchsorted(search)
        # From results, match to the first 0 (should be actual end/start of a cycle)
        match = df[i:].query('OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED == 0').head(1)
        # Condition met when cycle time goes beyond the end of the day
        if match.size == 0: break
        i = match.index[0]
        end_time = match.TimeLogged.iloc[0]
        cycles.append(Cycle(id, start_time, end_time))
        start_time = end_time
        id += 1
    return cycles

def get_cycles_new(df):
    cycle_duration = timedelta(seconds=80)
    date = (str(df.Timestamp[0])).split(' ')[0]
    # end_day = datetime.datetime.strptime(f'{date} 23:59:59', "%Y-%m-%d %H:%M:%S")
    end_day = datetime.datetime.strptime(f'{date} 23:59:59', "%Y-%m-%d %H:%M:%S")
    id = 0
    cycles = []
    start_time = df.TimeLogged[0]
    length = len(df)
    # While still in current day
    while start_time < end_day:
        # Search from current time to threshold below minimum cycle length (80s)
        search = (start_time + cycle_duration).strftime("%Y-%m-%d %H:%M:%S")
        i = df.TimeLogged.searchsorted(search)
        # From results, match to the first 0 (should be actual end/start of a cycle)
        match = df[i:].query('OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED == 0').head(1)
        j = match.index[0]
        while(df[j:].OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED.iloc[0] == 0.0): j+=1
        # Condition met when cycle time goes beyond the end of the day
        if match.size == 0: break
        i = match.index[0]
        end_time = df.iloc[j].TimeLogged
        cycles.append(Cycle(id, start_time, end_time))
        start_time = end_time
        id += 1
    return cycles

def build_charts(df, dfv, cycles):
    for i in range(0, len(cycles)):
        date = str(cycles[i].start).split()[0]
        start = cycles[i].start
        end = cycles[i].end
        spindle = spindle_stamp.between_time(str(cycles[i].start.time()), str(cycles[i].end.time()), inclusive='both')
        vibration = vibration_stamp.between_time(str(cycles[i].start.time()), str(cycles[i].end.time()), inclusive='both')
        t1_end = (start + timedelta(seconds=30))
        t2_end = (t1_end + timedelta(seconds=10))
        t3_end =  (t2_end + timedelta(seconds=43))
        tc1 = ToolTime(spindle.OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_1, start, t1_end)
        tc2 = ToolTime(spindle.OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_3, t1_end, t2_end)
        tc3 = ToolTime(spindle.OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_5, t2_end, t3_end)
        tc4 = ToolTime(spindle.OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_TOOL_COUNT_7, t3_end, end)
        fig,ax = plt.subplots()
        ax.set_title(f'Load and Vibration {cycles[i].id} - {date}')
        ax.set_xlabel('Time')
        ax.set_ylabel('Spindle Load')
        ax2 = ax.twinx()
        ax2.set_ylabel('Vibration')
        fig.autofmt_xdate(rotation=30)
        ln = ax.plot(spindle.OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_LOAD, alpha=0.5, color='blue', label='Spindle Load')
        ln2 = ax2.plot(vibration.Sensor1, alpha=0.5, color='red', label='Vibration')
        ax2.axvspan(tc1.start, tc1.end, color='purple', alpha=0.1)
        ax2.axvspan(tc2.start, tc2.end, color='blue', alpha=0.1)
        ax2.axvspan(tc3.start, tc3.end, color='purple', alpha=0.1)
        ax2.axvspan(tc4.start, tc4.end, color='blue', alpha=0.1)
        fig.text(.01, .99, f"TC1:{tc1.series.mode()[0]}\nTC2:{tc2.series.mode()[0]}\nTC3:{tc3.series.mode()[0]}\nTC4:{tc4.series.mode()[0]}\n", ha='left', va='top', transform=plt.gca().transAxes)
        opath = f'./turning/charts/loadvibration/{date}'
        if not os.path.isdir(opath):
            os.makedirs(opath)
        fig.savefig(f'{opath}/{cycles[i].id}.png')
        plt.close('all')