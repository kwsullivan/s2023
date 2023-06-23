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
# from query import get_fanuc_query
from dataclasses import dataclass

@dataclass
class Cycle:
    id: int
    start: datetime
    end: datetime

def get_cycles(df):
    cycle_duration = timedelta(seconds=80)
    id = 0
    cycles = []
    start_time = df.TimeLogged[0]
    length = len(df)
    for i in range(0, length):
        search = (start_time + cycle_duration).strftime("%Y-%m-%d %H:%M:%S")
        i = df.TimeLogged.searchsorted(search)
        match = df[i:].query('OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED == 0').head(1)
        i = match.index[0]
        end_time = match.TimeLogged.iloc[0]
        cycles.append(Cycle(id, start_time, end_time))
        start_time = end_time
        id += 1
    return cycles

'''
def get_cycles(df):
    cycle_duration = timedelta(seconds=95)
    in_cycle = False
    id = 0
    cycles = []
    for i in range(0, len(df)):
        if df.OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED[i] == 0 and in_cycle is False:
            while df.OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED[i] == 0: i += 1
            start_time = df.TimeLogged[i]
            in_cycle = True
        if in_cycle is True:
            # iterate forward to end of min cycle time
            while df.TimeLogged[i] < (start_time + cycle_duration):
                i += 1
                # edge check for end of array
                if(i >= len(df)):
                    i = len(df) - 1
                    break
            # iterate forward to closest 0 to signify end of cycle
            while df.OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED[i] != 0:
                i += 1
                # edge check for end of array
                if(i >= len(df)):
                    i = len(df) - 1
                    break
            end_time = df.TimeLogged[i]
            in_cycle = False
            cycles.append(Cycle(id, start_time, end_time))
            id == end_time
            id += 1
    return cycles

def get_cycles(df):
    cycle_duration = timedelta(seconds=95)
    in_cycle = False
    id = 0
    cycles = []
    start_time = df.TimeLogged[0]
    length = len(df)
    for i in range(0, length):
        # iterate forward to end of min cycle time
        while df.TimeLogged[i] < (start_time + cycle_duration):
            i += 1
            # edge check for end of array
            if(i >= length):
                i = length - 1
                break
        # iterate forward to closest 0 to signify end of cycle
        while df.OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_SPEED[i] != 0:
            i += 1
            # edge check for end of array
            if(i >= length):
                i = length - 1
                break
            
        end_time = df.TimeLogged[i]
        cycles.append(Cycle(id, start_time, end_time))
        start_time = end_time
        id += 1
    return cycles
    '''