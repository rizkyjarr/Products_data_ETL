import pandas as pd
import os

def extract_csv(file_path:str):
    return pd.read_csv(file_path)

all_dfs = []

def extract_multi_csvs(directory_path:str, files_list):
    for files in files_list:
        files_path = os.path.join(directory_path, files)
        df_temporary = pd.read_csv(files_path)
        all_dfs.append(df_temporary)
    return pd.concat(all_dfs, ignore_index=True)


