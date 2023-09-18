#On the sample data, remove duplicates on the combination of Name and age
# and store data in a data dictionary.
import pandas as pd
import pickle

def dedup_pandas(file:str)->None:
    df = pd.read_csv('1_data.csv')
    dedup_df = df.drop_duplicates(subset=["Name","Age"])
    df_dict = dedup_df.to_dict()
    with open('1_pd_dict.pkl','wb') as f:
        pickle.dump(df_dict,f)
    

if __name__=="__main__":
    file = '1_data.csv'
    dedup_pandas(file)
    