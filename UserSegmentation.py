
# coding: utf-8

# In[1]:
import os
import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import json
import requests
sys.path.append('/home/twqc64/sandbox')
from tappy.module import *
from tappy.util import *
contextAPI = 'http://localhost:8090/contexts'
jobAPI = 'http://localhost:8090/jobs'

resp = createContext("guru-context")
print resp


# In[4]:

conf = """
FileReader {
    inputFile = "gs://guru-examples/data/applaunch/*"
    format="CSV"
    delimiter=","
    output0="applaunch"
}
"""
resp = run("guru-context", "tap.engine.FileReader", conf)
assert(resp['status']=='OK')
print resp['result']


# In[5]:

conf = """
SummaryStats {
    input0 = "applaunch"
}
"""
resp = run("guru-context", "tap.engine.SummaryStats", conf)
assert(resp['status']=='OK')
print resp['result']


# In[6]:

conf = """
Scale {
    input0="applaunch"
    output0="preppeddata"
    withMean=false
}
"""
resp = run("guru-context", "tap.engine.Scale", conf)
assert(resp['status']=='OK')
print resp['result']


# In[7]:

conf = """
SummaryStats {
    input0 = "preppeddata"
}
"""
resp = run("guru-context", "tap.engine.SummaryStats", conf)
assert(resp['status']=='OK')
print resp['result']


# In[8]:

conf = """
ClusterAnalysis {
    input0 = "preppeddata"
    method = "KMeans"
    numClusters = 6
    withClusterSizes = true
}
"""
resp = run("guru-context", "tap.engine.ClusterAnalysis", conf, True)
#assert(resp['status']=='STARTED')
#jobId = resp['result']['jobId']
print resp


# In[9]:

#resp = getJobOutput(jobId, timeout=300)
#assert(resp['status']=='OK')
centers = resp['result']['centers']
centersArray = createNumpyArray(centers)
clusterSizes = resp['result']['clusterSizes']
print centersArray
print clusterSizes


# In[10]:

data = {}
for i in range(0, len(centersArray)):
    data[i] = [[]]
    data[i][0] = list(centersArray[i])
spoke_labels = ["call", "text", "IM", "email", "social", "web", "camera", "gaming"]
theta = radar_factory(len(spoke_labels), frame='polygon')
fig = plt.figure(figsize=(12, 8))
fig.subplots_adjust(wspace=0.35, hspace=0.20, top=0.85, bottom=0.05)

for n in data.keys():
    ax = fig.add_subplot(2, 3, n+1, projection='radar')
    ax.autoscale(False)
    ax.set_rmax(4)
    plt.rgrids([1, 2, 3, 4])
    ax.set_title("cluster" + str(n), weight='bold', size='medium', position=(0.5, 1.1),
                horizontalalignment='center', verticalalignment='center')
    for d in data[n]:
        ax.plot(theta, d, color='r')
        ax.fill(theta, d, facecolor='b', alpha=0.25)
    ax.set_varlabels(spoke_labels)

plt.savefig('segments.png', bbox_inches='tight')


# In[7]:

resp = deleteContext("guru-context")
print resp

