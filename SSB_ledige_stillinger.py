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

#Antall ledige stillinger ZARIr
ssburl = 'https://data.ssb.no/api/v0/no/table/11587/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "vs:NACE2007ledstillNN3",
        "values": [
          "01-96"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "LedigeStillinger"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [21]
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
df.to_csv('data/SSB_ledige_stillinger.csv', index=False)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
url = "https://api.datawrapper.de/v3/charts/ZARIr/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel ledige stillinger I4KmU
ssburl = 'https://data.ssb.no/api/v0/no/table/11587/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "vs:NACE2007ledstillNN3",
        "values": [
          "01-96"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "LedigeStillingerPros"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [21]
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
df.to_csv('data/SSB_ledige_stillinger_pst.csv', index=False)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
url = "https://api.datawrapper.de/v3/charts/I4KmU/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Næring ledige stillinger antall IwkIc og andel EpqxL
ssburl = 'https://data.ssb.no/api/v0/no/table/11587/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "vs:NACE2007ledstillNN3",
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
          "68",
          "69-75",
          "77-82",
          "84",
          "85",
          "86",
          "87",
          "88",
          "90-93",
          "94-96"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "LedigeStillinger",
          "LedigeStillingerPros"
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
df_new = df.pivot(index='næring (SN2007)', columns='statistikkvariabel', values='value')
df_new.to_csv('data/SSB_ledige_stillinger_naring.csv', index=True)
antall = df.iloc[0,2]
tittel_dato = (antall)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
date_string2 = tittel_dato[-1:]
date_string3 = tittel_dato[0:4]
date_string4 = 'Tall for ' + date_string2 + '.kvartal ' + date_string3
date_string5 = 'Tall for ' + date_string2 + '.kvartal ' + date_string3 + ' I pst. av totalt antall stillinger.'
#Update DW IwkIc
url = "https://api.datawrapper.de/v3/charts/IwkIc/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/IwkIc/"
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
#Update DW EpqxL
url = "https://api.datawrapper.de/v3/charts/EpqxL/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/EpqxL/"
payload = {"metadata": {"describe": {"intro": date_string5}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Endring i antall ledige stillinger etter næring SIbyZ
ssburl = 'https://data.ssb.no/api/v0/no/table/11587/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "vs:NACE2007ledstillNN3",
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
          "68",
          "69-75",
          "77-82",
          "84",
          "85",
          "86",
          "87",
          "88",
          "90-93",
          "94-96"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "LedigeStillinger"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [13]
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
df_new = df.pivot(index='næring (SN2007)', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[0,8,11,12]]
Endring_siste_kvartal = df_new2.iloc[:,3] - df_new2.iloc[:,2]
Endring_12 = df_new2.iloc[:,3] - df_new2.iloc[:,1]
Endring_3 = df_new2.iloc[:,3] - df_new2.iloc[:,1]
df_new3 = pd.concat([Endring_siste_kvartal, Endring_12, Endring_3], axis=1, keys=['Endring siste kvartal','Endring siste år','Endring siste 3 år'])
df_new3.to_csv('data/SSB_ledige_stillinger_naring_endring_antall.csv', index=True)
antall = df_new2.iloc[:,3]
tittel_dato = (antall.name)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
date_string2 = tittel_dato[-1:]
date_string3 = tittel_dato[0:4]
date_string4 = 'Sammenlignet med tall for ' + date_string2 + '.kvartal ' + date_string3
#Update DW
url = "https://api.datawrapper.de/v3/charts/SIbyZ/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/SIbyZ/"
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)