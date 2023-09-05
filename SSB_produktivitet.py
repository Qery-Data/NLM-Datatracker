from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import pandas as pd
locale.setlocale(locale.LC_TIME, 'nb_NO')
os.makedirs('data', exist_ok=True)
access_token = os.getenv('DW_TOKEN')

#Produktivitet i ulike næringer sist år gV6yJ
ssburl = 'https://data.ssb.no/api/v0/no/table/09174/'
query = {
  "query": [
    {
      "code": "NACE",
      "selection": {
        "filter": "vs:NRNaeringPubAgg",
        "values": [
          "nr23_6",
          "pub2X01_02",
          "pub2X03",
          "pub2X05",
          "nr2X06_09",
          "nr23ind",
          "pub2X35",
          "pub2X41_43",
          "pub2X45_47",
          "pub2X49A_52",
          "pub2X55_56",
          "pub2X58_63",
          "pub2X64_66",
          "pub2X68A",
          "pub2X69_75",
          "pub2X77_82",
          "nr23_6fn",
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Timeverk"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          1
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')

ssburl = 'https://data.ssb.no/api/v0/no/table/09170/'
query = {
  "query": [
    {
      "code": "NACE",
      "selection": {
        "filter": "vs:NRNaeringPubAgg",
        "values": [
          "nr23_6",
          "pub2X01_02",
          "pub2X03",
          "pub2X05",
          "nr2X06_09",
          "nr23ind",
          "pub2X35",
          "pub2X41_43",
          "pub2X45_47",
          "pub2X49A_52",
          "pub2X55_56",
          "pub2X58_63",
          "pub2X64_66",
          "pub2X68A",
          "pub2X69_75",
          "pub2X77_82",
          "nr23_6fn",
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "BNPB2"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          1
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df2 = dataset.write('dataframe')
df_new=df.pivot(index='næring', columns='statistikkvariabel',values='value')
df_new2=df2.pivot(index='næring', columns='statistikkvariabel',values='value')
df_new3= pd.concat([df_new, df_new2], axis=1)
GDP_per_hour = df_new3.iloc[:,1] / df_new3.iloc[:,0]
df_new4 = pd.concat([df_new3, GDP_per_hour],axis=1)
df_new4.columns = ['Utførte timeverk','BNP i basisverdi','BNP per timeverk']
df_new4.to_csv('data/SSB_produktivitet_sistaar.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
title_date=str(df.iloc[0,2])
date_string = 'Bruttoprodukt i kroner per utførte timeverk. Tall for ' + title_date +'.' 
#Update DW
chartid = 'gV6yJ'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)