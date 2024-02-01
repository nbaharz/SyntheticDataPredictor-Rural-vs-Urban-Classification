import pandas as pd

def extract_data(file_name):
    df = pd.read_csv(file_name)

    # DataFrame'i liste formatına çevir
    raw_data_list = df.to_dict(orient='records')

    return raw_data_list

