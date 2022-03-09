import pandas as pd
import requests
import boto3
import os
from time import time

''' 
   I want to create two lists. offset will be the index of getting the data, 
   while limit will be the index of where to stop getting the data for the csv.
   i will start off at 0 for offset. The limit will be 10000. 
   For example if a row count has 82,426 each csv file should have 10,000 rows 
   leading up to 82,426. The last file should have 2,426.
   '''

def main():
    credentials_path = "/Users/httran/Documents/Programming/Credentials_Repo/"
    credentials = pd.read_csv(credentials_path + 'user_credentials.csv')
    aws_key = str(credentials['Access key ID'][0])
    aws_secret_key = credentials['Secret access key'][0]

    s3 = boto3.client('s3', region_name='us-east-1', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret_key)

    '''Create the bucket'''
    #s3.create_bucket(Bucket='htran-covid-project')

    '''Get the count of rows for the dataset. Get the limit and offset parameters. 
    Get the limit and offset parameter for each url. Extract that and convert to a csv.
    Upload that csv to the s3 bucket. Continue until we reach the end of the list'''

    num_rows_url = 'https://data.cdc.gov/resource/9bhg-hcku.json?$select=count(state)'
    num_rows_result = requests.get(num_rows_url)
    num_rows_data = num_rows_result.json()
    num_rows = int(num_rows_data[0]['count_state'])

    # I don't like the way this is formatted but we can work on it later
    i = 0
    offset = []
    limit = 10000
    while i < num_rows:
        offset.append(i)
        i += 10000
    offset[-1] = num_rows

    #example url: https://soda.demo.socrata.com/resource/earthquakes.json?$limit=5&$offset=0$order=data_as_of
    for index_pos in offset:
        t0 = time()
        url = f'https://data.cdc.gov/resource/9bhg-hcku.json?$limit={limit}&$offset={index_pos}&$order=data_as_of'
        result = requests.get(url)
        data = result.json()
        df = pd.DataFrame(data)
        if "month" not in df.columns:
            df['month'] = None
        if "year" not in df.columns:
            df['year'] = None
        sorted_cols = sorted(df.columns)
        df = df[sorted_cols]
        print(df.columns)
        file_name = f'covid19data_row{index_pos}.csv'
        df.to_csv(file_name)
        s3.upload_file(file_name, Bucket='htran-covid-project', Key=file_name)
        t1 = time()
        total_time = round((t1 - t0),2)
        print(f'Total time to upload {file_name} was {total_time} seconds.')
        if os.path.exists(file_name):
            os.remove(file_name)
        else:
            continue


if __name__ == '__main__':
    t_start = time()
    main()
    t_finish = time()
    total_overall = round(t_finish - t_start,2)
    print(f'Successfully uploaded all files. Total time to finish the script was {total_overall} seconds')