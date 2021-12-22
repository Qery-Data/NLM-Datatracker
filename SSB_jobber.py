from pyjstat import pyjstat
import requests
import os
import json
import datetime
import locale
locale.setlocale(locale.LC_TIME, 'nb_NO')
os.makedirs('data', exist_ok=True)
access_token = os.getenv('DW_TOKEN')

#Jobber antall utvikling totalt nzFUM
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
df["endring"] = df["value"].diff()
df["endring i pst"] = df["value"].pct_change()*100
df = df[1:62]
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

#Jobber antall per næring og endring per næring S6QM8
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
Endring_covid = df_new2.iloc[:,4] - df_new['2020M02']
Endring_3 = df_new2.iloc[:,4] - df_new2.iloc[:,1]
Endring_5 = df_new2.iloc[:,4] - df_new2.iloc[:,0]
import pandas as pd
df_new3 = pd.concat([antall, Endring_mnd, Endring_12, Endring_covid,Endring_3, Endring_5], axis=1, keys=['Antall','Endring sist mnd','Endring sist år','Endring fra feb.20','Endring siste 3 år','Endring siste 5 år'])
df_new3.to_csv('data/SSB_jobber_naring.csv', index=True)
date_string = tittel_dato.replace("M","")
from datetime import datetime
date_string2 = datetime.strptime(date_string, "%Y%m")
date_string3 = 'Sesongjusterte tall for ' + date_string2.strftime ('%B %Y')
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW S6QM8
url = "https://api.datawrapper.de/v3/charts/S6QM8/"
payload = {
    "metadata": {"describe": {"intro": date_string3}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/S6QM8/"
payload = {
    "metadata": {"annotate": {"notes": riktig_dato}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
#Update DW Wf007 (Totalt antall sist mnd)
url = "https://api.datawrapper.de/v3/charts/Wf007/"
payload = {
    "metadata": {"describe": {"intro": date_string3}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/Wf007/"
payload = {
    "metadata": {"annotate": {"notes": riktig_dato}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
#Update DW R5eLv (Endring fra feb.20)
url = "https://api.datawrapper.de/v3/charts/R5eLv/"
payload = {
    "metadata": {"describe": {"intro": date_string3 + " sammenlignet med februar 2020."}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/R5eLv/"
payload = {
    "metadata": {"annotate": {"notes": riktig_dato}}
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
Endring_covid = ((df_new2.iloc[:,4] - df_new['2020M02'])/ df_new['2020M02']*100)
Endring_3_pst = ((df_new2.iloc[:,4] - df_new2.iloc[:,1]) / df_new2.iloc[:,1]*100)
Endring_5_pst = ((df_new2.iloc[:,4] - df_new2.iloc[:,0]) / df_new2.iloc[:,0]*100)
df_new4 = pd.concat([antall, Endring_mnd_pst, Endring_12_pst, Endring_covid, Endring_3_pst, Endring_5_pst], axis=1, keys=['Antall','Endring sist mnd','Endring sist år','Endring fra feb.20','Endring siste 3 år','Endring siste 5 år'])
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
    "metadata": {"describe": {"intro": date_string3}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/96bMF/"
payload = {
    "metadata": {"annotate": {"notes": riktig_dato}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#VIZ Utvikling i antall jobber [flere]
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

#VIZ Jobber detaljert næring (88) SrULZ
ssburl = 'https://data.ssb.no/api/v0/no/table/11656/'
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
          "999A"
        ]
      }
    },
    {
      "code": "NACE2007",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02",
          "03",
          "05",
          "06",
          "07",
          "08",
          "09",
          "10",
          "11",
          "13",
          "14",
          "15",
          "16",
          "17",
          "18",
          "19",
          "20",
          "21",
          "22",
          "23",
          "24",
          "25",
          "26",
          "27",
          "28",
          "29",
          "30",
          "31",
          "32",
          "33",
          "35",
          "36",
          "37",
          "38",
          "39",
          "41",
          "42",
          "43",
          "45",
          "46",
          "47",
          "49",
          "50",
          "51",
          "52",
          "53",
          "55",
          "56",
          "58",
          "59",
          "60",
          "61",
          "62",
          "63",
          "64",
          "65",
          "66",
          "68",
          "69",
          "70",
          "71",
          "72",
          "73",
          "74",
          "75",
          "77",
          "78",
          "79",
          "80",
          "81",
          "82",
          "84",
          "85",
          "86",
          "87",
          "88",
          "90",
          "91",
          "92",
          "93",
          "94",
          "95",
          "96",
          "97"
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
df_new = df.pivot(index='næring (SN2007)', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[0,8,16,20]]
antall = df_new2.iloc[:,3]
tittel_dato = (antall.name)
antall.name = 'antall'
Endring_12 = df_new2.iloc[:,3] - df_new2.iloc[:,2]
Endring_3 = df_new2.iloc[:,3] - df_new2.iloc[:,1]
Endring_5 = df_new2.iloc[:,3] - df_new2.iloc[:,0]
import pandas as pd
df_new3 = pd.concat([antall, Endring_12, Endring_3, Endring_5], axis=1, keys=['Antall','Endring sist år','Endring siste 3 år','Endring siste 5 år'])
df_new3.to_csv('data/SSB_jobber_naring_detaljert.csv', index=True)
date_string2 = tittel_dato[-1:]
date_string3 = tittel_dato[0:4]
date_string4 = 'Tall for ' + date_string2 + '. kvartal ' + date_string3
from datetime import datetime
date_string2 = tittel_dato[-1:]
date_string3 = tittel_dato[0:4]
date_string4 = 'Tall for ' + date_string2 + '. kvartal ' + date_string3
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
url = "https://api.datawrapper.de/v3/charts/SrULZ/"
payload = {
    "metadata": {"describe": {"intro": date_string4}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/SrULZ/"
payload = {
    "metadata": {"annotate": {"notes": riktig_dato}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Jobber kvartal sektor OWk86 og u4F4z
ssburl = 'https://data.ssb.no/api/v0/no/table/11653/'
query = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Landet4",
        "values": [
          "Ialt"
        ]
      }
    },
    {
      "code": "Sektor",
      "selection": {
        "filter": "item",
        "values": [
          "ABDEFX",
          "6500",
          "6100"
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
df_new = df.pivot(index='sektor', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[0,4,8,12,16,20]]
df_new3 = df_new2.sort_values(df_new2.columns[5])
antall = df_new2.iloc[:,5]
tittel_dato = (antall.name)
df_new3.to_csv('data/SSB_jobber_sektor_kvartal.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
date_string2 = tittel_dato[-1:]
date_string3 = tittel_dato[0:4]
date_string4 = 'Tall for ' + date_string2 + '. kvartal de siste seks årene'
#Update DW OWk86
url = "https://api.datawrapper.de/v3/charts/x/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/OWk86/"
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
#Update DW u4F4z
url = "https://api.datawrapper.de/v3/charts/x/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/u4F4z/"
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

# Antall jobber fylke CM5AJ
ssburl = 'https://data.ssb.no/api/v0/no/table/11657/'
query = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:FylkerFastIkkeFast",
        "values": [
          "30",
          "03",
          "34",
          "38",
          "42",
          "11",
          "46",
          "15",
          "50",
          "18",
          "54"
        ]
      }
    },
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
        "filter": "top",
        "values": [5]
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
df_new = df.pivot(index='region', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[0,4]]
Endring_antall = df_new2.iloc[:,1] - df_new2.iloc[:,0]
Endring_prosent = Endring_antall / df_new2.iloc[:,0]*100
import pandas as pd
df_new3 = pd.concat([df_new2.iloc[:,1],Endring_antall,Endring_prosent], axis=1, keys=['Antall','Endring i antall', 'Endring i prosent'])
antall = df_new2.iloc[:,1]
tittel_dato = (antall.name)
df_new3.to_csv('data/SSB_jobber_fylke_kvartal.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
date_string2 = tittel_dato[-1:]
date_string3 = tittel_dato[0:4]
date_string4 = 'Tall for ' + date_string2 + '.kvartal ' + date_string3 + '. ' + 'Endring fra samme kvartal året før i antall og prosent.'
#Update DW
url = "https://api.datawrapper.de/v3/charts/CM5AJ/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/CM5AJ/"
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel jobber fylke XW1f9 [IKKE I BRUK]
ssburl = 'https://data.ssb.no/api/v0/no/table/11653/'
query = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:FylkerFastIkkeFast",
        "values": [
          "30",
          "03",
          "34",
          "38",
          "42",
          "11",
          "46",
          "15",
          "50",
          "18",
          "54"
        ]
      }
    },
    {
      "code": "Sektor",
      "selection": {
        "filter": "item",
        "values": [
          "ABDEFX",
          "6500",
          "6100"
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
        "filter": "top",
        "values": [1
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
df_new = df.pivot(index='region', columns='sektor', values='value')
antall = df.iloc[0,3]
tittel_dato = (antall)
df_new.to_csv('data/SSB_jobber_fylke_andel.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
date_string2 = tittel_dato[-1:]
date_string3 = tittel_dato[0:4]
date_string4 = 'Tall for ' + date_string2 + '.kvartal ' + date_string3
#Update DW
url = "https://api.datawrapper.de/v3/charts/XW1f9/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/XW1f9/"
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Antall jobber kommune mJgIS 
ssburl = 'https://data.ssb.no/api/v0/no/table/11657/'
query = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:KommunerFastIkkeFast",
        "values": [
          "0301",
          "3001",
          "3002",
          "3003",
          "3004",
          "3005",
          "3006",
          "3007",
          "3011",
          "3012",
          "3013",
          "3014",
          "3015",
          "3016",
          "3017",
          "3018",
          "3019",
          "3020",
          "3021",
          "3022",
          "3023",
          "3024",
          "3025",
          "3026",
          "3027",
          "3028",
          "3029",
          "3030",
          "3031",
          "3032",
          "3033",
          "3034",
          "3035",
          "3036",
          "3037",
          "3038",
          "3039",
          "3040",
          "3041",
          "3042",
          "3043",
          "3044",
          "3045",
          "3046",
          "3047",
          "3048",
          "3049",
          "3050",
          "3051",
          "3052",
          "3053",
          "3054",
          "3401",
          "3403",
          "3405",
          "3407",
          "3411",
          "3412",
          "3413",
          "3414",
          "3415",
          "3416",
          "3417",
          "3418",
          "3419",
          "3420",
          "3421",
          "3422",
          "3423",
          "3424",
          "3425",
          "3426",
          "3427",
          "3428",
          "3429",
          "3430",
          "3431",
          "3432",
          "3433",
          "3434",
          "3435",
          "3436",
          "3437",
          "3438",
          "3439",
          "3440",
          "3441",
          "3442",
          "3443",
          "3446",
          "3447",
          "3448",
          "3449",
          "3450",
          "3451",
          "3452",
          "3453",
          "3454",
          "3801",
          "3802",
          "3803",
          "3804",
          "3805",
          "3806",
          "3807",
          "3808",
          "3811",
          "3812",
          "3813",
          "3814",
          "3815",
          "3816",
          "3817",
          "3818",
          "3819",
          "3820",
          "3821",
          "3822",
          "3823",
          "3824",
          "3825",
          "4201",
          "4202",
          "4203",
          "4204",
          "4205",
          "4206",
          "4207",
          "4211",
          "4212",
          "4213",
          "4214",
          "4215",
          "4216",
          "4217",
          "4218",
          "4219",
          "4220",
          "4221",
          "4222",
          "4223",
          "4224",
          "4225",
          "4226",
          "4227",
          "4228",
          "1101",
          "1103",
          "1106",
          "1108",
          "1111",
          "1112",
          "1114",
          "1119",
          "1120",
          "1121",
          "1122",
          "1124",
          "1127",
          "1130",
          "1133",
          "1134",
          "1135",
          "1144",
          "1145",
          "1146",
          "1149",
          "1151",
          "1160",
          "4601",
          "4602",
          "4611",
          "4612",
          "4613",
          "4614",
          "4615",
          "4616",
          "4617",
          "4618",
          "4619",
          "4620",
          "4621",
          "4622",
          "4623",
          "4624",
          "4625",
          "4626",
          "4627",
          "4628",
          "4629",
          "4630",
          "4631",
          "4632",
          "4633",
          "4634",
          "4635",
          "4636",
          "4637",
          "4638",
          "4639",
          "4640",
          "4641",
          "4642",
          "4643",
          "4644",
          "4645",
          "4646",
          "4647",
          "4648",
          "4649",
          "4650",
          "4651",
          "1505",
          "1506",
          "1507",
          "1511",
          "1514",
          "1515",
          "1516",
          "1517",
          "1520",
          "1525",
          "1528",
          "1531",
          "1532",
          "1535",
          "1539",
          "1547",
          "1554",
          "1557",
          "1560",
          "1563",
          "1566",
          "1573",
          "1576",
          "1577",
          "1578",
          "1579",
          "5001",
          "5006",
          "5007",
          "5014",
          "5020",
          "5021",
          "5022",
          "5025",
          "5026",
          "5027",
          "5028",
          "5029",
          "5031",
          "5032",
          "5033",
          "5034",
          "5035",
          "5036",
          "5037",
          "5038",
          "5041",
          "5042",
          "5043",
          "5044",
          "5045",
          "5046",
          "5047",
          "5049",
          "5052",
          "5053",
          "5054",
          "5055",
          "5056",
          "5057",
          "5058",
          "5059",
          "5060",
          "5061",
          "1804",
          "1806",
          "1811",
          "1812",
          "1813",
          "1815",
          "1816",
          "1818",
          "1820",
          "1822",
          "1824",
          "1825",
          "1826",
          "1827",
          "1828",
          "1832",
          "1833",
          "1834",
          "1835",
          "1836",
          "1837",
          "1838",
          "1839",
          "1840",
          "1841",
          "1845",
          "1848",
          "1851",
          "1853",
          "1856",
          "1857",
          "1859",
          "1860",
          "1865",
          "1866",
          "1867",
          "1868",
          "1870",
          "1871",
          "1874",
          "1875",
          "5401",
          "5402",
          "5403",
          "5404",
          "5405",
          "5406",
          "5411",
          "5412",
          "5413",
          "5414",
          "5415",
          "5416",
          "5417",
          "5418",
          "5419",
          "5420",
          "5421",
          "5422",
          "5423",
          "5424",
          "5425",
          "5426",
          "5427",
          "5428",
          "5429",
          "5430",
          "5432",
          "5433",
          "5434",
          "5435",
          "5436",
          "5437",
          "5438",
          "5439",
          "5440",
          "5441",
          "5442",
          "5443",
          "5444",
          "9999g"
        ]
      }
    },
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
        "filter": "top",
        "values": [5]
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
df_new = df.pivot(index='region', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[0,4]]
Endring_antall = df_new2.iloc[:,1] - df_new2.iloc[:,0]
Endring_prosent = Endring_antall / df_new2.iloc[:,0]*100
import pandas as pd
df_new3 = pd.concat([df_new2.iloc[:,1],Endring_antall,Endring_prosent], axis=1, keys=['Antall','Endring i antall', 'Endring i prosent'])
antall = df_new2.iloc[:,1]
tittel_dato = (antall.name)
df_new3.to_csv('data/SSB_jobber_kommune_kvartal.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
date_string2 = tittel_dato[-1:]
date_string3 = tittel_dato[0:4]
date_string4 = 'Tall for ' + date_string2 + '.kvartal ' + date_string3 + '. ' + 'Endring fra samme kvartal året før i antall og prosent.'
#Update DW
url = "https://api.datawrapper.de/v3/charts/mJgIS/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/mJgIS/"
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Antall jobber etter alder wWRNG
ssburl = 'https://data.ssb.no/api/v0/no/table/11652/'
query = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Landet4",
        "values": []
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
        "filter": "item",
        "values": [
          "999A",
          "-24",
          "25-39",
          "40-54",
          "55-66",
          "67+"
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
        "filter": "top",
        "values": [5]
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
df_new = df.pivot(index='alder', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[0,4]]
Endring_antall = df_new2.iloc[:,1] - df_new2.iloc[:,0]
Endring_prosent = Endring_antall / df_new2.iloc[:,0]*100
import pandas as pd
df_new3 = pd.concat([df_new2.iloc[:,1],Endring_antall,Endring_prosent], axis=1, keys=['Antall','Endring i antall', 'Endring i prosent'])
antall = df_new2.iloc[:,1]
tittel_dato = (antall.name)
df_new3.to_csv('data/SSB_jobber_alder_beggekjonn.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Sist publiserte data: ' + oppdatert_dato.strftime ('%d/%m/%y')
date_string2 = tittel_dato[-1:]
date_string3 = tittel_dato[0:4]
date_string4 = 'Tall for ' + date_string2 + '.kvartal ' + date_string3 + '. ' + 'Endring fra samme kvartal året før i antall og prosent.'
#Update DW
url = "https://api.datawrapper.de/v3/charts/wWRNG/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/wWRNG/"
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)