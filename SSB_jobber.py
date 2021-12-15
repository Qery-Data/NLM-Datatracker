from pyjstat import pyjstat
import requests
import os
import datawrapper
import json
import datetime
import locale
from datawrapper import Datawrapper

os.makedirs('data', exist_ok=True)

ssburl = 'https://data.ssb.no/api/v0/no/table/13126/'
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
df_new = df.pivot(index='måned', columns='statistikkvariabel', values='value')
df_new.to_csv('data/SSB_jobber_totalt.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
dw = Datawrapper(access_token = os.getenv('DW_TOKEN'))
dw.refresh_data('5YMDQ')
properties = {
  'annotate' : {
    'notes': riktig_dato,
  }
}
dw.update_metadata('5YMDQ', properties)

from pyjstat import pyjstat
import requests
ssburl = 'https://data.ssb.no/api/v0/no/table/13126/'
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
          "AntArbForhold"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "Top",
        "values": [25]
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
df["endring"] = df["value"].diff()
df = df[:-1]
df.to_csv('data/SSB_jobber_totalt_endring.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
dw = Datawrapper(access_token = os.getenv('DW_TOKEN'))
dw.refresh_data('t8TNy')
properties = {
  'annotate' : {
    'notes': riktig_dato,
  }
}
dw.update_metadata('t8TNy', properties)


ssburl = 'https://data.ssb.no/api/v0/no/table/13126/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "item",
        "values": [
          "01-03",
          "05-09",
          "10-33",
          "35-39",
          "41-43",
          "45-47",
          "49-53",
          "55-56",
          "58-63",
          "64-66",
          "68-75",
          "77-82",
          "84",
          "85",
          "86-88",
          "90-99",
          "00"
        ]
      }
    },
    {
      "code": "ForelopigEndelig",
      "selection": {
        "filter": "item",
        "values": [
          "01"
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
        "filter": "top",
        "values": [
          "1"
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
df.to_csv('data/SSB_jobber_naring.csv', index=True)

ssburl = 'https://data.ssb.no/api/v0/no/table/13126/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "item",
        "values": [
          "01-03",
          "05-09",
          "10-33",
          "35-39",
          "41-43",
          "45-47",
          "49-53",
          "55-56",
          "58-63",
          "64-66",
          "68-75",
          "77-82",
          "84",
          "85",
          "86-88",
          "90-99",
          "00"
        ]
      }
    },
    {
      "code": "ForelopigEndelig",
      "selection": {
        "filter": "item",
        "values": [
          "01"
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
        "filter": "top",
        "values": [2]
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
df_new = df.pivot(index='næring (SN2007)', columns='måned', values='value')
df_new["endring"] = df_new.iloc[:,1]-df_new.iloc[:,0]
df_new.to_csv('data/SSB_jobber_naring_endring.csv', index=True)


