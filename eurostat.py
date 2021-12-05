from pyjstat import pyjstat
import requests
import os
os.makedirs('data', exist_ok=True)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/une_rt_m?s_adj=SA&lastTimePeriod=60&age=TOTAL&unit=PC_ACT&sex=T')
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='time', columns='geo', values='value')
df_new.to_csv('data/Eurostatledighet.csv', index=True)
