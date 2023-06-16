import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
from dataclasses import dataclass
import time
import numpy as np
import warnings
from datetime import datetime
import scipy.stats as stats
from sklearn.cluster import KMeans

database = 'BR607_12April'
table = 'Broach_Data_New'
plt.rcParams["figure.figsize"] = (20,10)
warnings.filterwarnings('ignore')

conn_string = f"Driver=ODBC Driver 18 for SQL Server;\
                    Server=localhost; \
                    Database={database}; \
                    TrustServerCertificate=yes; \
                    Trusted_Connection=yes;"

conn = pyodbc.connect(conn_string)

date_query = pd.read_sql_query(f"SELECT DISTINCT CONVERT(varchar(10), Record_Collection_Time, 111) as dates \
                            FROM [{database}].[dbo].[{table}] order by dates", conn)
dates = [datetime.strptime(curr, '%Y/%m/%d').date() for curr in date_query.dates]

# WEEKS
weeks = []
weeks.append(dates[0])
for d in dates:
    if d.weekday() == 0:
        weeks.append(d)

weeks.append(dates[len(dates) - 1])

for i in range(0, len(weeks)):
    if i == len(weeks) - 1: break
    print(weeks[i], weeks[i+1])

for i in range(0, len(weeks)):

    if i == len(weeks) - 1: break
    df = pd.read_sql_query(f"SELECT * FROM [{database}].[dbo].[{table}] as day_data inner join \
    (SELECT Data_Set_ID as counts_d_id, count_sets FROM \
    (SELECT Data_Set_ID, COUNT(Data_Set_ID) as count_sets \
    FROM [{database}].[dbo].[{table}] \
    GROUP BY Data_Set_ID) AS cs \
    WHERE count_sets = 457) as clean_data on day_data.Data_Set_ID = clean_data.counts_d_id \
    WHERE \
    Record_Collection_Time BETWEEN \'{str(weeks[i])}\' AND \'{str(weeks[i+1])}\' \
    ORDER BY Record_Collection_Time", conn)
    grouping = 'Mean'
    distances = df.groupby(by='Data_Set_ID').mean()
    # maxx = df.groupby(by='Data_Set_ID').max()
    # minn = df.groupby(by='Data_Set_ID').min()
    # distances['minn'] = minn[grouping]
    # distances['maxx'] = maxx[grouping]
    
    print(distances)
    #bx = distances.rolling(100, win_type='gaussian').mean(std=distances.std().mean())#should be odd number
    bx = distances.rolling(100, win_type='gaussian')#should be odd number
    print(bx)
    print(type(bx))
    bx = distances.rolling(100, win_type='gaussian').mean(std=distances.std().mean())#should be odd number
    
    x= bx['Feedback Speed']
    y = bx['Distance']
    z = bx['Feedback Pressure']

    #Applying mask for outliers
    pcd=np.column_stack((x,y,z))
    mask=z>np.mean(z)
    spatial_query=pcd[z>np.mean(z)]

    #plotting the results 3D
    ax = plt.axes(projection='3d')
    ax.scatter(x[mask], y[mask], z[mask], s=0.1)
#    plt.show()

    
    X=np.column_stack((x[mask], y[mask], z[mask]))
    # wcss = [] 
    # for i in range(1, 20):
    #     kmeans = KMeans(n_clusters = i, init = 'k-means++', random_state = 42)#42 is arbitary here (the asnwer to all of life's questions)
    #     kmeans.fit(X)
    #     wcss.append(kmeans.inertia_)

    # plt.plot(range(1, 20), wcss)
    # plt.xlabel('Number of clusters')
    # plt.ylabel('WCSS') 
    # plt.show()

    kmeans = KMeans(n_clusters=3).fit(X)
    fig = plt.figure()
    ax = fig.gca(projection='3d')
    ax.scatter(x[mask], y[mask],z[mask],  c=kmeans.labels_)


    #plt.show()



    #PLOTTING
    # bx = distances.rolling(100, win_type='gaussian').mean(std=distances.std().mean()).plot(x='counts_d_id', y=grouping, color='black', figsize=(20, 10), label=grouping)
    # plt.fill_between(distances['counts_d_id'], distances['minn'], distances['maxx'], color='blue', alpha = 0.2)
    # # plt.yticks(np.arange(0, 1, step=0.1))
    # daymin = distances['minn'].min()
    # daymax = distances['maxx'].max()
    # plt.axhline(y=distances[grouping].mean(), color='green', linestyle=':')
    # plt.text(.01, .99, f"Mean:{distances[grouping].mean()}", ha='left', va='top', transform=plt.gca().transAxes)
    ax.legend()
    
    ax.set_xlabel(x)
    ax.set_ylabel(y) 
    ax.set_zlabel(z)
    plt.title(f'{weeks[i]}-{weeks[i+1]}')
    plt.savefig(f'./broaching/clustering/weeks/{grouping}/{weeks[i]}.png')