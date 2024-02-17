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

#Andel heltid deltid y6GJE
ssburl = 'https://data.ssb.no/api/v0/no/table/09790/'
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df_new = df.pivot(index='statistikkvariabel', columns='år', values='value')
df_new.to_csv('data/SSB_arbeidstid_heltid_deltid.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
# chartid = 'y6GJE'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel heltid deltid kvinner WvdXZ
ssburl = 'https://data.ssb.no/api/v0/no/table/09790/'
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df_new = df.pivot(index='statistikkvariabel', columns='år', values='value')
df_new.to_csv('data/SSB_arbeidstid_heltid_deltid_kvinner.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
# chartid = 'WvdXZ'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel heltid deltid menn PhYrg
ssburl = 'https://data.ssb.no/api/v0/no/table/09790/'
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df_new = df.pivot(index='statistikkvariabel', columns='år', values='value')
df_new.to_csv('data/SSB_arbeidstid_heltid_deltid_menn.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
# chartid = 'PhYrg'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel yrke heltid deltid 2lEAx
ssburl = 'https://data.ssb.no/api/v0/no/table/09790/'
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df_new = df.pivot(index='yrke', columns='statistikkvariabel', values='value')
df_new.to_csv('data/SSB_arbeidstid_heltid_deltid_yrker.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
title_date=df.iloc[0,3]
date_string = 'Tall for ' + title_date + '. Prosent av sysselsatte (15-74 år) som jobber:' 
#Update DW
# chartid = '2lEAx'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"describe": {"intro": date_string}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel yrke heltid kvinner og menn nVuzM
ssburl = 'https://data.ssb.no/api/v0/no/table/09790/'
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df_new = df.pivot(index='yrke', columns='kjønn', values='value')
df_new.to_csv('data/SSB_arbeidstid_heltid_deltid_yrker_kjonn.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
title_date=df.iloc[0,3]
date_string = 'Tall for ' + title_date + '. Prosent av sysselsatte (15-74 år) som jobber heltid etter kjønn.' 
#Update DW
# chartid = 'nVuzM'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"describe": {"intro": date_string}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Gjennomsnittlig arbeidstid QFijl
ssburl = 'https://data.ssb.no/api/v0/no/table/09790/'
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df_new = df.pivot(index='yrke', columns='år', values='value')
df_new.to_csv('data/SSB_arbeidstid_snitt_yrker.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')+ ' *Prosess- og maskinoperatører, transportarbeidere mv. '
# #Update DW
# chartid = 'QFijl'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)