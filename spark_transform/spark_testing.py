#!python3

import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SprakContext


#create the pandas Dataframe

pandasDF = pd.DataFrame(data, coloumns = ['Name', 'Age'])

def spark_conn(app, config):
    master = config['ip']
    try:
        spark = SparkSession.builder \
            .master(master) \
                .appName(app) \
                    .getOrCreate()

        print(f"[INFO] Success Connect SPARK ENGINE.....")
        return spark
    except:
        print(f"[INFO] Error Connect SPARK ENGINE.....")

spark = spark_conn('Testing',{"ip":"spark://HPG732-005.kpk.go.id:7077"})

sparkDF = spark.createDataFrame(pandasDF)
da = sparkDF.toPandas()