# -*- coding: utf-8 -*-
import gzip
import csv
import numpy as np
import pandas as pd
data1 = pd.read_csv("part-00000-of-00500.csv.gz.csv",compression='gzip',error_bad_lines=False,header=None) #read the data from .gz file
data2 = pd.read_csv("part-00001-of-00500.csv.gz.csv",compression='gzip',error_bad_lines=False,header=None)
data = data1.append(data2) #connect the files
print(data1.shape)
print(data2.shape)
print(data.shape)
data.columns = ["timestamp","missinginfo","jobID","taskindex​-withinthejob","machineID","eventtype","username","schedulingclass","priority","CPU","RAM","local disk space","different-machine-constraint"]
#corresponding columns name: we find them from google cluster' doc
data = data.drop(["missinginfo","machineID","eventtype","username","schedulingclass","different-machine-constraint"],axis = 1)
#we drop some columns that are not related to our goal
data = data[(~data["CPU"].isin(["NaN"]))&(~data["RAM"].isin(["NaN"]))&(~data["local disk space"].isin(["NaN"]))] #drop the missing data
np.savetxt("input.txt",data.values, fmt='%s')
print(list(data))