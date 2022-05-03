
from distutils.log import error
from numpy import extract
import requests
import os
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen


download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip'
]
if not os.path.isdir('downloads'):
    os.makedirs('downloads')
def main(download_uris):
    for p,i in enumerate(download_uris):
        try:
            data = requests.get(i)   
            zipfile = ZipFile(BytesIO(data.content)) 
            zipfile.extractall('downloads')          
        except :
            print('error in uri')    
if __name__ == '__main__':
    main(download_uris)
    