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

#Antall arbeidsledige oF7tM
ssburl = 'https://data.ssb.no/api/v0/no/table/13760/'
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
        "values": [60]
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
df.to_csv('data/SSB_arbeidsledige.csv', index=False)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
# #Update DW
# chartid='oF7tM'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel arbeidsledige bd63e
ssburl = 'https://data.ssb.no/api/v0/no/table/13760/'
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
df = dataset.write('dataframe')
df.to_csv('data/SSB_arbeidsledige_pst.csv', index=False)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
# chartid = 'bd63e'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel arbeidsledige etter alder UE4Of
ssburl = 'https://data.ssb.no/api/v0/no/table/13760/'
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
df = dataset.write('dataframe')
df_new = df.pivot(index='alder', columns='måned', values='value')
df_new.to_csv('data/SSB_arbeidsledige_alder_pst.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
# #Update DW
# chartid = 'UE4Of'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel arbeidsledige etter kjønn qGKlW
ssburl = 'https://data.ssb.no/api/v0/no/table/13760/'
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
df = dataset.write('dataframe')
df_new = df.pivot(index='kjønn', columns='måned', values='value')
df_new.to_csv('data/SSB_arbeidsledige_kjønn_pst.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
# #Update DW
# chartid = 'qGKlW'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)