import boto3
import requests

def main():
    BUCKET_NAME = 'commoncrawl'
    KEY = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    s3 = boto3.resource('s3')
   
    s3.Bucket(BUCKET_NAME).download_file(KEY, 's3://commoncrawl/crawl-data/CC-MAIN-2022-05')
 



if __name__ == '__main__':
    main()
