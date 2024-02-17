from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import pandas as pd
os.makedirs('data_EN', exist_ok=True)
access_token = os.getenv('DW_TOKEN')

#Share participated in formal or non formal learning activity by industry and type (q9d8g)
ssburl = 'https://data.ssb.no/api/v0/en/table/12865/'
query = {
  "query": [
    {
      "code": "UtdanningOppl",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02",
          "03",
          "04",
          "05"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0"
        ]
      }
    },
    {
      "code": "NACE2007",
      "selection": {
        "filter": "item",
        "values": [
          "00-99",
          "01-03",
          "05-39",
          "41-43",
          "45-47",
          "49-53",
          "55-56",
          "58-63",
          "64-66",
          "68-82",
          "84",
          "85",
          "86-88",
          "90-99"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "DeltakereProsent"
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
df_new = df.pivot(index='formal and non-formal education', columns='industry (SIC2007)', values='value')
df_new.to_csv('data_EN/SSB_learning_industry.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
# chartid = 'q9d8g'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Share participated by learning activity (HpaHo)
ssburl = 'https://data.ssb.no/api/v0/en/table/12864/'
query = {
  "query": [
    {
      "code": "UtdanningOppl",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02",
          "03",
          "04",
          "05"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0"
        ]
      }
    },
    {
      "code": "Alder",
      "selection": {
        "filter": "vs:AlleAldre53b",
        "values": [
          "Ialt"
        ]
      }
    },
    {
      "code": "ArbStyrkStatus",
      "selection": {
        "filter": "item",
        "values": [
          "1uS"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "DeltakereProsent"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2021",
          "2022"
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
df_new = df.pivot(index='formal and non-formal education', columns='year', values='value')
df_new.to_csv('data_EN/SSB_learning_type.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
# chartid = 'HpaHo'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)