import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from readBroachData import *

result = readData()
print('----------------------------')
print(result)
    
coeff = result.corr()
coeff.fillna(-1)
print('----------------------------')
print(coeff)
    
sns.heatmap(coeff)
plt.show()
plt.savefig("corrCoeff_heatmap_broach.png")