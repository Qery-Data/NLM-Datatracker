from pyjstat import pyjstat
import requests
import os
import datawrapper
import json
import datetime
from datawrapper import Datawrapper
import locale
locale.setlocale(locale.LC_TIME, 'nb_NO')
os.makedirs('data', exist_ok=True)

#Jobber antall utvikling
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
dw.refresh_data('nzFUM')
properties = {
  'annotate' : {
    'notes': riktig_dato,
  }
}
dw.update_metadata('nzFUM', properties)

#Jobber mnd endring i antall
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
        "values": [27]
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
df = df[1:26]
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

#Jobber antall endring per næring
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
          "90-99"
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
        "filter": "top",
        "values": [
          "62"
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
df_new = df.pivot(index='næring (SN2007)', columns='måned', values='value')
df_new2 = df_new.iloc[:,[0,24,48,59,60]]
antall = df_new2.iloc[:,4]
tittel_dato = (antall.name)
antall.name = 'antall'
Endring_mnd = df_new2.iloc[:,4] - df_new2.iloc[:,3]
Endring_12 = df_new2.iloc[:,4] - df_new2.iloc[:,2]
Endring_3 = df_new2.iloc[:,4] - df_new2.iloc[:,1]
Endring_5 = df_new2.iloc[:,4] - df_new2.iloc[:,0]
import pandas as pd
df_new3 = pd.concat([antall, Endring_mnd, Endring_12, Endring_3, Endring_5], axis=1)
df_new3.to_csv('data/SSB_jobber_naring.csv', index=True)
date_string = tittel_dato.replace("M","")
from datetime import datetime
date_string2 = datetime.strptime(date_string, "%Y%m")
date_string3 = date_string2.strftime ('%B %Y') +', sesongjusterte tall.'
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
dw = Datawrapper(access_token = os.getenv('DW_TOKEN'))
dw.refresh_data('S6QM8')
properties = {
  'annotate' : {
    'notes': riktig_dato,
  }
}
dw.update_metadata('S6QM8', properties)
dw.update_description('S6QM8', intro=date_string3)

#Jobber pst endring per næring
Endring_mnd_pst = ((df_new2.iloc[:,4] - df_new2.iloc[:,3]) / df_new2.iloc[:,3]*100)
Endring_12_pst = ((df_new2.iloc[:,4] - df_new2.iloc[:,2]) / df_new2.iloc[:,2]*100)
Endring_3_pst = ((df_new2.iloc[:,4] - df_new2.iloc[:,1]) / df_new2.iloc[:,1]*100)
Endring_5_pst = ((df_new2.iloc[:,4] - df_new2.iloc[:,0]) / df_new2.iloc[:,0]*100)
df_new4 = pd.concat([antall, Endring_mnd_pst, Endring_12_pst, Endring_3_pst, Endring_5_pst], axis=1)
df_new4.to_csv('data/SSB_jobber_naring_endring.csv', index=True)
date_string = tittel_dato.replace("M","")
from datetime import datetime
date_string2 = datetime.strptime(date_string, "%Y%m")
date_string3 = date_string2.strftime ('%B %Y') +', sesongjusterte tall.'
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
dw = Datawrapper(access_token = os.getenv('DW_TOKEN'))
dw.refresh_data('96bMF')
properties = {
  'annotate' : {
    'notes': riktig_dato,
  }
}
dw.update_metadata('96bMF', properties)
dw.update_description('96bMF', intro=date_string3)