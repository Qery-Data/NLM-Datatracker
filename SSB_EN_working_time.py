from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import pandas as pd
os.makedirs('data', exist_ok=True)
access_token = os.getenv('DW_TOKEN')

#Share full-time and part-time ilOme
ssburl = 'https://data.ssb.no/api/v0/en/table/09790/'
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
      "code": "Yrke",
      "selection": {
        "filter": "item",
        "values": [
          "0-9"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "SysselsHeltidProsent",
          "SysselsDeltidProsent"
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
df_new = df.pivot(index='contents', columns='year', values='value')
df_new.to_csv('data_EN/SSB_working_time_full_part.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
description_text = 'As % of all employed persons 15-74 years.' + '<br><br> <a target="_self" href="https://datawrapper.dwcdn.net/ilOme/" style="border-bottom: 2px solid #555;color:#000000;font-weight:600;  cursor:pointer;" rel="nofollow noopener noreferrer">Total</a> &nbsp; <a target="_self" href="https://datawrapper.dwcdn.net/uqYxa/" style=" color:#000000; font-weight:400; cursor:pointer;" rel="nofollow noopener noreferrer">Women</a> &nbsp; <a target="_self" href="https://datawrapper.dwcdn.net/IaDJs/" style=" color:#000000; font-weight:400; cursor:pointer;" rel="nofollow noopener noreferrer">Men</a> &nbsp;'
#Update DW
chartid = 'ilOme'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": description_text}}}
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

#Share full-time and part-time women uqYxa
ssburl = 'https://data.ssb.no/api/v0/en/table/09790/'
query = {
  "query": [
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "2"
        ]
      }
    },
    {
      "code": "Yrke",
      "selection": {
        "filter": "item",
        "values": [
          "0-9"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "SysselsHeltidProsent",
          "SysselsDeltidProsent"
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
df_new = df.pivot(index='contents', columns='year', values='value')
df_new.to_csv('data_EN/SSB_working_time_women_full_part.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
description_text = 'As % of all employed persons 15-74 years.' + '<br><br> <a target="_self" href="https://datawrapper.dwcdn.net/ilOme/" style=" color:#000000; font-weight:400; cursor:pointer;" rel="nofollow noopener noreferrer">Total</a> &nbsp; <a target="_self" href="https://datawrapper.dwcdn.net/uqYxa/" style="border-bottom: 2px solid #555;color:#000000;font-weight:600;  cursor:pointer;" rel="nofollow noopener noreferrer">Women</a> &nbsp; <a target="_self" href="https://datawrapper.dwcdn.net/IaDJs/" style=" color:#000000; font-weight:400; cursor:pointer;" rel="nofollow noopener noreferrer">Men</a> &nbsp;'
#Update DW
chartid = 'uqYxa'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": description_text}}}
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

#Share full-time and part-time men IaDJs
ssburl = 'https://data.ssb.no/api/v0/en/table/09790/'
query = {
  "query": [
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "1"
        ]
      }
    },
    {
      "code": "Yrke",
      "selection": {
        "filter": "item",
        "values": [
          "0-9"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "SysselsHeltidProsent",
          "SysselsDeltidProsent"
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
df_new = df.pivot(index='contents', columns='year', values='value')
df_new.to_csv('data_EN/SSB_working_time_men_full_part.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
description_text = 'As % of all employed persons 15-74 years.' + '<br><br> <a target="_self" href="https://datawrapper.dwcdn.net/ilOme/" style=" color:#000000; font-weight:400; cursor:pointer;" rel="nofollow noopener noreferrer">Total</a> &nbsp; <a target="_self" href="https://datawrapper.dwcdn.net/uqYxa/" style="color:#000000;font-weight:400;  cursor:pointer;" rel="nofollow noopener noreferrer">Women</a> &nbsp; <a target="_self" href="https://datawrapper.dwcdn.net/IaDJs/" style="border-bottom: 2px solid; #555; color:#000000; font-weight:600; cursor:pointer;" rel="nofollow noopener noreferrer">Men</a> &nbsp;'
#Update DW
chartid = 'IaDJs'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": description_text}}}
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

#Share working full-time and part-time industry sDD1A
ssburl = 'https://data.ssb.no/api/v0/en/table/09790/'
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
      "code": "Yrke",
      "selection": {
        "filter": "item",
        "values": [
          "0-9",
          "1",
          "2",
          "3",
          "4",
          "5",
          "6",
          "7",
          "8",
          "Andre"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "SysselsHeltidProsent",
          "SysselsDeltidProsent"
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
df_new = df.pivot(index='occupation', columns='contents', values='value')
df_new.to_csv('data_EN/SSB_working_time_industry_full_part.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato=df.iloc[0,3]
date_string = 'Data for ' + dato + '. Share of employed persons working:' 
#Update DW
chartid = 'sDD1A'
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

#Share full-time industry women and men elsZ5
ssburl = 'https://data.ssb.no/api/v0/en/table/09790/'
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
      "code": "Yrke",
      "selection": {
        "filter": "item",
        "values": [
          "0-9",
          "1",
          "2",
          "3",
          "4",
          "5",
          "6",
          "7",
          "8",
          "Andre"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "SysselsHeltidProsent"
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
df_new = df.pivot(index='occupation', columns='sex', values='value')
df_new.to_csv('data_EN/SSB_working_time_industry_women_men_full.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato=df.iloc[0,3]
date_string = 'Data for ' + dato + '. Share of employed persons working:' 
#Update DW
chartid = 'elsZ5'
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

#Average working-time by industry 2Ca6C
ssburl = 'https://data.ssb.no/api/v0/en/table/09790/'
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
      "code": "Yrke",
      "selection": {
        "filter": "item",
        "values": [
          "0-9",
          "1",
          "2",
          "3",
          "4",
          "5",
          "6",
          "7",
          "8",
          "Andre"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "ArbeidstidPerUke"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [10
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
df_new = df.pivot(index='occupation', columns='year', values='value')
df_new.to_csv('data_EN/SSB_working_time_industry_average_weekly.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')+ '.'
#Update DW
chartid = '2Ca6C'
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

