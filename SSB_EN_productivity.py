from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import pandas as pd
os.makedirs('data', exist_ok=True)
access_token = os.getenv('DW_TOKEN')

#Productivity by industry last year 7ymZd
ssburl = 'https://data.ssb.no/api/v0/en/table/09174/'
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
resultat = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df = dataset.write('dataframe')

ssburl = 'https://data.ssb.no/api/v0/en/table/09170/'
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
resultat = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df2 = dataset.write('dataframe')

df_new=df.pivot(index='industry', columns='contents',values='value')
df_new2=df2.pivot(index='industry', columns='contents',values='value')
df_new3= pd.concat([df_new, df_new2], axis=1)
BNP_per_timeverk = df_new3.iloc[:,1] / df_new3.iloc[:,0]
df_new4 = pd.concat([df_new3, BNP_per_timeverk],axis=1)
df_new4.columns = ['Hours worked','Value added','Value added per hour worked']
df_new4.to_csv('data_EN/SSB_productivity_lastyear.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato=str(df.iloc[0,2])
date_string = 'Value added per hour worked. Data for ' + dato +'.' 
#Update DW
chartid = '7ymZd'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
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
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/publish/'
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }

response = requests.request("POST", url, headers=headers)