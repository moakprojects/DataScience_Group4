## IMPORT ##

import sys
import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import gzip
import locale
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructField, StructType, StringType

## CONFIG ##

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
locale.getdefaultlocale()
locale.getpreferredencoding()

conf = SparkConf().set('spark.driver.host', '127.0.0.1')
sc = SparkContext(master='local', appName='Rank - multiple categories', conf=conf)
spark = SparkSession.builder.appName("Rank - multiple categoriesh").getOrCreate()


## PROCESS ##

# Initialisation
r1 = [] #Aim to contain the percentage of 1/5 rank for each category
r2 = []
r3 = []
r4 = []
r5 = [] #Aim to contain the percentage of 5/5 rank for each category

# Function that get the data we want for one category
# Input: path of the file containing the data of the category
# Output: list r1...r5 completed
def dataFile(path):

    #Data
    data = sc.textFile(path)
    df = spark.read.json(data)

    # Group the data ranking
    total = df.count()
    dataGraph = df.groupby('overall').count()

    # To panda RDD
    dataGraphP = dataGraph.toPandas()
    dataGraphP = dataGraphP.sort_values(by=['overall'])

    # Add to the lists
    r1.append((dataGraphP.iloc[0].loc["count"]/total)*100)
    r2.append((dataGraphP.iloc[1].loc["count"]/total)*100)
    r3.append((dataGraphP.iloc[2].loc["count"]/total)*100)
    r4.append((dataGraphP.iloc[3].loc["count"]/total)*100)
    r5.append((dataGraphP.iloc[4].loc["count"]/total)*100)


#Path to each category file
files = ['hdfs://namenode:9000/subsets/Video_Games_5.json.gz','hdfs://namenode:9000/subsets/Digital_Music_5.json.gz','hdfs://namenode:9000/subsets/Cell_Phones_and_Accessories_5.json.gz']
for fil in files:
    dataFile(fil)

#Plot
n = len(files)
ind = np.arange(n)

plt.bar(ind, r1, color='#A20101', edgecolor='white')
plt.bar(ind, r2, bottom=r1, color='#F44E54', edgecolor='white')
plt.bar(ind, r3, bottom=[i+j for i,j in zip(r1,r2)], color='#FF9904', edgecolor='white')
plt.bar(ind, r4, bottom=[i+j+k for i,j,k in zip(r1,r2,r3)], color='#FDDB5E', edgecolor='white')
plt.bar(ind, r5, bottom=[i+j+k+l for i,j,k,l in zip(r1,r2,r3,r4)], color='#BAF1A1', edgecolor='white')


l1 = mpatches.Patch(color='#A20101', label='1/5')
l2 = mpatches.Patch(color='#F44E54', label='2/5')
l3 = mpatches.Patch(color='#FF9904', label='3/5')
l4 = mpatches.Patch(color='#FDDB5E', label='4/5')
l5 = mpatches.Patch(color='#BAF1A1', label='5/5')


plt.ylabel('Percentage')
plt.title('Rating per category')
plt.xticks(ind, ('Video Games', 'Digital Music','Cell Phones and Accessories'))
plt.yticks(np.arange(0, 101, 10))
plt.legend(loc='upper left', handles=[l1, l2, l3, l4, l5])
plt.savefig('categories.png')


print("Process finished!")

