from logging import exception
import boto3
import os
import json
import logging
files_list = []
import pandas as pd
if not os.path.isdir('downloads'):
    os.makedirs('downloads')
def main():
    for roots,files in os.walk('data',topdown=True):
        for file in files:
            if '.json' in file:
                files_list.append(roots+'/'+file)
    # print(files_list)          
    for a,i in enumerate(files_list):
            with open(i) as file:
                data = json.load(file)
            # print(data)
            x = pd.json_normalize(data)
            x.to_csv('downloads/file{0}.csv'.format(a+1))
           


if __name__ == '__main__':
    main()
    