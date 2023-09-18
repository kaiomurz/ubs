#On the sample data, remove duplicates on the combination of Name and age
# and store data in a data dictionary.
import pickle

import pandas as pd
from pyspark.sql import SparkSession

def dedup_pandas(file:str)->None:
    try:
        df = pd.read_csv('1_data.csv')
        dedup_df = df.drop_duplicates(subset=["Name","Age"])
        df_dict = dedup_df.to_dict()
        print("pandas df_dict")
        with open('1_pd_dict.pkl','wb') as f:
            pickle.dump(df_dict,f)
    except Exception as e:
        raise e

def dedup_spark(file:str)->None:
    try:
        spark = SparkSession.builder.getOrCreate()

        df = spark.read.options(header='True').csv('1_data.csv')
        df = df.dropDuplicates(["Name","Age"])
        
        df_dict = {"Name":{}, "Age":{}, "Location":{}}

        for i, row in enumerate(df.collect()):
            df_dict["Name"][i] = row.Name
            df_dict["Age"][i] = row.Age
            df_dict["Location"][i] = row.Location
        
        print(df_dict)
        
        with open('1_spark_dict.pkl','wb') as f:
            pickle.dump(df_dict,f)

    except Exception as e:
        raise e  

if __name__=="__main__":
    file = '1_data.csv'
    dedup_pandas(file)
    dedup_spark(file)
    