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
import warnings
from querylist import get_fanuc_query, get_fanuc_query_v2, get_vibration_query
from dataclasses import dataclass
from pprint import pprint
import matplotlib.dates as mdates
from scipy.fft import fft

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

date='2023-05-02'
split = date.split('-')

database = 'FanucData_GOOD'
table = 'OP10'
query = get_fanuc_query('FanucData_GOOD', 'OP10', split[0], split[1], split[2])
vib_query = get_vibration_query('VibrationData_GOOD', 'Panel1', split[0], split[1], split[2])

database = 'FanucDataV2'
table = 'Data'
# query = get_fanuc_query_v2(database, table, split[0], split[1], split[2])


plt.rcParams["figure.figsize"] = (20,10)
warnings.filterwarnings('ignore')

conn_string = f"Driver={{ODBC Driver 17 for SQL Server}};\
                    Server=localhost\SQLEXPRESS; \
                    Database={database}; \
                    Trusted_Connection=yes;"

conn = pyodbc.connect(conn_string)



df = pd.read_sql_query(query, conn)
dfv = pd.read_sql_query(vib_query, conn)
dfv = dfv[(dfv.Sensor1 >= 0) & (dfv.Sensor1 < 120)]
dfv['Timestamp'] = pd.to_datetime(dfv['Record_Collection_Time'], format = '%Y-%m-%d %H:%M:%S.%f')

# df['Timestamp'] = df['TimeLogged'].dt.time
# df['Timestamp'] = df['Timestamp'].astype(str)
df['Timestamp'] = pd.to_datetime(df['TimeLogged'], format = '%Y-%m-%d %H:%M:%S.%f')

## MIRROR DFV



reversed = dfv.iloc[::-1]
print(reversed.head)
frames = [dfv, reversed]
freqDf = pd.concat(frames)
dfr = dfv.rolling(10)

# freqDf.fillna(method='ffill', inplace=True)
# freqDf.fillna(method='bfill', inplace=True)

print("Mirrored:")
print(freqDf.head)

freqArr = freqDf['Sensor1'].to_numpy()
print(np.sort(freqArr))

vol = fft(freqArr)
N = len(vol)
n = np.arange(N)
samplingRate = 1/10
T = N*samplingRate
freq = n/T
freq = freq[:N//2]
y = (2/N)*np.abs(vol[0:int(N/2)])



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

###





spindle_stamp = df.set_index('Timestamp')
vibration_stamp = dfv.set_index('Timestamp')

cycle_duration = timedelta(seconds=80)
end_day = dt.strptime(f'{date} 23:59:59', "%Y-%m-%d %H:%M:%S")
id = 0
cycles = []
start_time = df.TimeLogged[0]
# While still in current day
while start_time < end_day:
    # Search from current time to threshold below minimum cycle length (80s)
    search = (start_time + cycle_duration).strftime("%Y-%m-%d %H:%M:%S")
    i = df.TimeLogged.searchsorted(search)
    # From results, match to the first 0 (should be actual end/start of a cycle)
    match = df[i:].query('OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED == 0').head(1)
    if match.size == 0: break
    j = match.index[0]
    # match = df[i:].query('OP10_MACHINE_A_LA633_SPINDLE_1_SPEED == 0')
    while(df[j:].OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED.iloc[0] == 0.0): j+=1
    # Condition met when cycle time goes beyond the end of the day
    i = match.index[0]
    end_time = df.iloc[j].TimeLogged
    cycles.append(Cycle(id, start_time, end_time))
    start_time = end_time
    id += 1

########################################################
cycle_duration = timedelta(seconds=80)
end_day = dt.strptime(f'{date} 23:59:59', "%Y-%m-%d %H:%M:%S")
id = 0
cycles = []
start_time = new.index[0]
while start_time < end_day:
    search = (start_time + cycle_duration).strftime("%Y-%m-%d %H:%M:%S")
    i = new.index.searchsorted(search)
    match = new[i:].query('OP10_MACHINE_A_LA633_SPINDLE_1_SPEED == 0').head(1)
    # match = df[i:].query('OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED == 0').head(1)
    if match.size == 0: break
    i = match.index[0]
    end_time = match.index[0]
    cycles.append(Cycle(id, start_time, end_time))
    start_time = end_time
    id += 1
########################################################


# Build graph
# (start) 0-30, 30-40, 40-1:23 1:23-1:35 (end)
for i in range(0, len(cycles)):
    start = cycles[i].start
    end = cycles[i].end
    spindle = new.between_time(str(cycles[i].start.time()), str(cycles[i].end.time()), inclusive='both')
    if i == 3: break
    vibration = # Change this to vibration df -> new.between_time(str(cycles[i].start.time()), str(cycles[i].end.time()), inclusive='both')
    fig,ax = plt.subplots()
    ax.set_xlabel('Time')
    ax.set_ylabel('Spindle Speed')
    ax2.set_ylabel('Vibration')
    ax2 = ax.twinx()
    ln = ax.plot(spindle.OP10_MACHINE_A_LA633_SPINDLE_1_LOAD, alpha=0.15, color='blue', label='Spindle Load')
    # ln2 = ax2.plot(vibration.Sensor1, alpha=0.15, color='red', label='Vibration')
    # leg = ln + ln2
    # labs = [l.get_label() for l in leg]
    # ax2.legend(leg, labs, loc=0)
    opath = f'./turning/charts/tool_life/{date}'
    if not os.path.isdir(opath):
        os.makedirs(opath)
    fig.savefig(opath)
t1_end = (start + timedelta(seconds=30))
t2_end = (t1_end + timedelta(seconds=10))
t3_end =  (t2_end + timedelta(seconds=43))
tc1 = ToolTime(spindle.OP10_MACHINE_A_LA633_SPINDLE_1_TOOL_COUNT_1, start, t1_end)
tc2 = ToolTime(spindle.OP10_MACHINE_A_LA633_SPINDLE_1_TOOL_COUNT_3, t1_end, t2_end)
tc3 = ToolTime(spindle.OP10_MACHINE_A_LA633_SPINDLE_1_TOOL_COUNT_5, t2_end, t3_end)
tc4 = ToolTime(spindle.OP10_MACHINE_A_LA633_SPINDLE_1_TOOL_COUNT_7, t3_end, end)


print(tc1.end - tc1.start)
print(tc2.end - tc2.start)
print(tc3.end - tc3.start)
print(tc4.end - tc4.start)

fig,ax = plt.subplots()
ax.set_xlabel('Time')
ax.set_ylabel('Spindle Speed')
ax2 = ax.twinx()
# ax.xaxis.set_major_locator(plt.MaxNLocator(10))
fig.autofmt_xdate(rotation=30)
ln = ax.plot(spindle.OP10_MACHINE_A_LA633_SPINDLE_1_LOAD, alpha=0.75, color='blue', label='Spindle Load')
ln2 = ax2.plot(tc1.series, alpha=0.30, color='red', label='Tool 1 Count')
ln3 = ax2.plot(tc2.series, alpha=0.30, color='yellow', label='Tool 2 Count')
ln4 = ax2.plot(tc3.series, alpha=0.30, color='green', label='Tool 3 Count')
ln5 = ax2.plot(tc4.series, alpha=0.30, color='pink', label='Tool 4 Count')
# ax2.axvspan(mdates.date2num(tc1.start), mdates.date2num(tc1.end), color='purple', alpha=0.2)
ax2.axvspan(tc1.start, tc1.end, color='purple', alpha=0.2)
ax2.axvspan(tc2.start, tc2.end, color='blue', alpha=0.1)
ax2.axvspan(tc3.start, tc3.end, color='purple', alpha=0.2)
ax2.axvspan(tc4.start, tc4.end, color='blue', alpha=0.1)
fig.text(mdates.date2num(tc1.start), 0, 'hello', fontsize=22, bbox=dict(facecolor='black', alpha=0.5))
fig.text(mdates.date2num(tc1.start), .99, f"TC1:{tc1.series.mode()}", ha='left', va='top')
ax2.set_ylabel('Tool Count')


for c in cycles:
    if (c.end - c.start) > timedelta(seconds=120):
        print(c.end - c.start)