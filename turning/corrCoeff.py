import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from vibration import *

plt.rcParams["figure.figsize"] = (20,10)
warnings.filterwarnings('ignore')

date = "2023-05-02"
dateSplit = date.split('-')

fanucData = readFanuc('FanucData_17May2023', 'OP10',  dateSplit[0], dateSplit[1], dateSplit[2])
vibData = readVib('VibrationData_17May2023', 'Panel1', dateSplit[0], dateSplit[1], dateSplit[2])

print(fanucData)
print("--------------------------")
print(vibData)
cycles = makeCycles(fanucData)
result = matchCycles(cycles, vibData, fanucData)
result = cleanData(result)
print("----------------------------")
print(result)


def coeffMatrix():
    coeff = result.corr()
    coeff.fillna(-1)
    print('----------------------------')
    print(coeff)
        
    sns.heatmap(coeff)
    #plt.show()
    plt.savefig("corrCoeff_heatmap_vib.png")

def coeffOverTime():
    print("**************************************")
    count = 0
    coeffs = []
    for i in cycles:
        if i.id == count:
            mask = (result.index > i.start) & (result.index <= i.end)
            df = result.loc[mask]
            currCoeff = df['OP10_MACHINE_A_OP10_MACHINE_A_SPINDLE_1_LOAD'].corr(df['Sensor1'])
            coeffs.append(currCoeff)
        count+=1 
    print(coeffs)
    plt.figure()
    x=coeffs
    y=coeffs.index
    plt.plot(coeffs, "o")
    # # plt.plot(f_oneside, np.abs(vol[:n_oneside]), 'b')
    plt.title("Correlation Coeff Over Time")
    plt.xlabel('Cycle ID')
    plt.ylabel('Correlation Coeffcient')
    plt.show()
    plt.savefig('corrCoeffOverTime.png')	


        

coeffMatrix()
coeffOverTime()