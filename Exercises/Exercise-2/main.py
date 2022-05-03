import requests
import pandas as pd
import re


def main(url):
    data  = requests.get(url)
    html_data = data.content.decode('utf-8')
    x = re.findall('href.*2022-02-07 14:03',html_data)[0]
    a = x.split('"')[1]
    new_url = url + a
    csv_data = requests.get(new_url).content.decode('utf-8')
    with open('csv_data.csv','w') as file:
        file.write(csv_data)
    df = pd.read_csv('csv_data.csv')
    m = df['HourlyDryBulbTemperature'].max()
    print(m)


if __name__ == '__main__':
    url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
    main(url)
