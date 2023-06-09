import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits import mplot3d
from sklearn.cluster import KMeans
import pyodbc
import pandas as pd

conn = pyodbc.connect(
    'Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=BR607_12April;Trusted_Connection=yes; TrustServerCertificate=yes'
)
cursor = conn.cursor()

April12 = """
    SELECT 
        [Data_Set_ID]
        ,[Record_Collection_Time]
        ,[Feedback Speed]
        ,[Feedback Pressure]
        ,[Distance]
    FROM [BR607_12April].[dbo].[Broach_Data_New]
"""


df = pd.read_sql_query(April12, conn)

x= df['Feedback Speed']
y = df['Distance']
z = df['Feedback Pressure']

plt.subplot(1, 2, 1)
plt.scatter(x, z,  s=0.05)
plt.axhline(y=np.mean(z), color='r', linestyle='-')
plt.title("First view")
plt.xlabel('X-axis ')
plt.ylabel('Z-axis ')

#same but for y,z 
plt.subplot(1, 2, 2)
plt.scatter(y, z,  s=0.05)
plt.axhline(y=np.mean(z), color='r', linestyle='-')
plt.title("Second view")
plt.xlabel('Y-axis ')
plt.ylabel('Z-axis ')
#plt.show()
plt.clf()

#Applying mask for outliers
pcd=np.column_stack((x,y,z))
mask=z>np.mean(z)
spatial_query=pcd[z>np.mean(z)]

#plotting the results 3D
ax = plt.axes(projection='3d')
ax.scatter(x[mask], y[mask], z[mask], s=0.1)

plt.show()

#In this first case, let us create a feature space holding only the X, Y features after masking

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

kmeans = KMeans(n_clusters=2).fit(X)
ax = plt.axes(projection='3d')
ax.scatter(x[mask], y[mask],z[mask], s=0.1,  c=kmeans.labels_)

plt.show()
