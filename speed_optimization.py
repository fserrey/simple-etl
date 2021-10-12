import pandas as pd
import numpy as np
import time

#Params
_path = './dataset_optimization.csv'
df = pd.read_csv(_path)

# Function 
def function_ (lat, lon):
    a = np.sin(lat/2)**2 + np.cos(lat) * np.cos(lon) * np.sin(lon/2)**2
    return a

# Original
start_time = time.time()
results = []
for i in range(0,len(df)):
    r = function_(df.iloc[i]['latitude'] ,df.iloc[i]['longitude'])
    results.append(r)
    
df['distance'] = results
print("--- %s seconds ---" % (time.time() - start_time)) 
#--- 12.396187782287598 seconds ---

# Option 1: apply
 df = pd.read_csv(_path)

start_time = time.time()       
df['distance'] = df[['latitude','longitude']].apply(lambda x : function_(*x), axis=1)

print("--- %s seconds ---" % (time.time() - start_time))
# --- 0.673447847366333 seconds ---

#Option 2: numpy vectors
df = pd.read_csv(_path)

start_time = time.time()       
df['distance'] = function_(df['latitude'].values , df['longitude'].values)

print("--- %s seconds ---" % (time.time() - start_time))
# --- 0.005124568939208984 seconds ---