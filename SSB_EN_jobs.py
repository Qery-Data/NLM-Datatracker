from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import pandas as pd
os.makedirs('data_EN', exist_ok=True)
access_token = os.getenv('DW_TOKEN')

#Jobs total Hs2JG
ssburl = 'https://data.ssb.no/api/v0/en/table/13126/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "item",
        "values": [
          "00-99"
        ]
      }
    },
    {
      "code": "ForelopigEndelig",
      "selection": {
        "filter": "item",
        "values": [
          "02"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
            "AntLonnstakSesong",
          "AntArbForholdSesong"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
            63
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
df_new = df.pivot(index='month', columns='contents', values='value')
df_new.to_csv('data_EN/SSB_jobs_total.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
chartid = 'Hs2JG'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
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

#Jobs monthly change ibFMZ
ssburl = 'https://data.ssb.no/api/v0/en/table/13126/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "item",
        "values": [
          "00-99"
        ]
      }
    },
    {
      "code": "ForelopigEndelig",
      "selection": {
        "filter": "item",
        "values": [
          "02"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "AntArbForholdSesong"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "Top",
        "values": [63]
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
df['change'] = df.loc[:, 'value'].diff()
df['change in pct'] = df.loc[:, 'value'].pct_change()*100
df_new = df[37:62]
df_new.to_csv('data_EN/SSB_jobs_total_change.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object['updated']
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
chartid = 'ibFMZ'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
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