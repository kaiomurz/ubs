import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor


df = pd.DataFrame({"number":range(100)})

try:
    os.mkdir('4_files')
except FileExistsError:
    print("directory already exists")
    
mini_dfs = []
for i in range(0,100,10):
    #the first part of the tuple will be the file name
    mini_dfs.append((f"{i}_to_{i+10}",df[i:i+10].copy()))
    
def save_as_parquet(mini_df):
    file_name, df = mini_df
    df.to_parquet(f"4_files/{file_name}.parquet")
    
with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(save_as_parquet, mini_dfs)