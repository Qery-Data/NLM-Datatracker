from pyjstat import pyjstat
import requests
import os
import json
import datetime
import locale
locale.setlocale(locale.LC_TIME, 'nb_NO')
os.makedirs('data', exist_ok=True)
access_token = os.getenv('DW_TOKEN')

#Jobber antall utvikling nzFUM
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
#Update DW
url = "https://api.datawrapper.de/v3/charts/nzFUM/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Jobber mnd endring i antall t8TNy
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
#Update DW
url = "https://api.datawrapper.de/v3/charts/t8TNy/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Jobber antall endring per næring S6QM8
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
date_string3 = 'Sesongjusterte tall for ' + date_string2.strftime ('%B %Y')
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
url = "https://api.datawrapper.de/v3/charts/S6QM8/"
payload = {
    "metadata": {"annotate": {"notes": riktig_dato}},
    "metadata": {"describe": {"intro": date_string3}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Jobber pst endring per næring 96bMF
Endring_mnd_pst = ((df_new2.iloc[:,4] - df_new2.iloc[:,3]) / df_new2.iloc[:,3]*100)
Endring_12_pst = ((df_new2.iloc[:,4] - df_new2.iloc[:,2]) / df_new2.iloc[:,2]*100)
Endring_3_pst = ((df_new2.iloc[:,4] - df_new2.iloc[:,1]) / df_new2.iloc[:,1]*100)
Endring_5_pst = ((df_new2.iloc[:,4] - df_new2.iloc[:,0]) / df_new2.iloc[:,0]*100)
df_new4 = pd.concat([antall, Endring_mnd_pst, Endring_12_pst, Endring_3_pst, Endring_5_pst], axis=1)
df_new4.to_csv('data/SSB_jobber_naring_endring.csv', index=True)
date_string = tittel_dato.replace("M","")
from datetime import datetime
date_string2 = datetime.strptime(date_string, "%Y%m")
date_string3 = 'Sesongjusterte tall for ' + date_string2.strftime ('%B %Y')
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
url = "https://api.datawrapper.de/v3/charts/96bMF/"
payload = {
    "metadata": {"annotate": {"notes": riktig_dato}},
    "metadata": {"describe": {"intro": date_string3}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#VIZ Utvikling i antall jobber J2g1N
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
df_new.to_csv('data/SSB_jobber_naring_utvikling.csv', index=True)
#Update DW
from datetime import datetime
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
url = "https://api.datawrapper.de/v3/charts/J2g1N/"
payload = {
    "metadata": {"annotate": {"notes": riktig_dato}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)