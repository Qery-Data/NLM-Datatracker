from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import pandas as pd
os.makedirs('data', exist_ok=True)
access_token = os.getenv('DW_TOKEN')

#Number of unemployed 7A6DR
ssburl = 'https://data.ssb.no/api/v0/en/table/13760/'
query = {
  "query": [
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
        "filter": "item",
        "values": [
          "15-74"
        ]
      }
    },
    {
      "code": "Justering",
      "selection": {
        "filter": "item",
        "values": [
          "T"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Arbeidsledige"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [61]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df.to_csv('data_EN/SSB_unemployment.csv', index=False)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid='7A6DR'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Unemplyment rate XJ4kj
ssburl = 'https://data.ssb.no/api/v0/en/table/13760/'
query = {
  "query": [
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
        "filter": "item",
        "values": [
          "15-74"
        ]
      }
    },
    {
      "code": "Justering",
      "selection": {
        "filter": "item",
        "values": [
          "T"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "ArbledProsArbstyrk"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [61]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df.to_csv('data_EN/SSB_unemployment_rate.csv', index=False)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 'XJ4kj'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Unemployment by age 2AQaL
ssburl = 'https://data.ssb.no/api/v0/en/table/13760/'
query = {
  "query": [
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
        "filter": "item",
        "values": [
          "15-24",
          "25-74"
        ]
      }
    },
    {
      "code": "Justering",
      "selection": {
        "filter": "item",
        "values": [
          "T"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "ArbledProsArbstyrk"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [61]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='age', columns='month', values='value')
df_new.to_csv('data_EN/SSB_unemployment_age_rate.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = '2AQaL'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Unemployment women and men TTkJt
ssburl = 'https://data.ssb.no/api/v0/en/table/13760/'
query = {
  "query": [
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "1",
          "2"
        ]
      }
    },
    {
      "code": "Alder",
      "selection": {
        "filter": "item",
        "values": [
          "15-74"
        ]
      }
    },
    {
      "code": "Justering",
      "selection": {
        "filter": "item",
        "values": [
          "T"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "ArbledProsArbstyrk"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [61]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='sex', columns='month', values='value')
df_new.to_csv('data_EN/SSB_unemployment_gender_rate.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 'TTkJt'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)