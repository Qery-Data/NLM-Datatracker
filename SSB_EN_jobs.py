from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import pandas as pd
os.makedirs('data_EN', exist_ok=True)
access_token = os.getenv('DW_TOKEN')

#Jobs total Hs2JG
ssburl = 'https://data.ssb.no/api/v0/en/table/13126/'
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
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
            63
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
df_new = df.pivot(index='month', columns='contents', values='value')
df_new.to_csv('data_EN/SSB_jobs_total.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
chartid = 'Hs2JG'
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

#Jobs monthly change ibFMZ
ssburl = 'https://data.ssb.no/api/v0/en/table/13126/'
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
df['change'] = df.loc[:, 'value'].diff()
df['change in pct'] = df.loc[:, 'value'].pct_change()*100
df_new = df[37:62]
df_new.to_csv('data_EN/SSB_jobs_total_change.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object['updated']
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
chartid = 'ibFMZ'
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

#Jobber antall per næring og endring per næring NPfNj/vnqke/SXwZ8
ssburl = 'https://data.ssb.no/api/v0/en/table/13126/'
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
          62
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
df_new = df.pivot(index='industry (SIC2007)', columns='month', values='value')
df_new2 = df_new.iloc[:,[0,24,48,59,60]]
antall = df_new2.iloc[:,4]
tittel_dato = (antall.name)
antall.name = 'antall'
Endring_mnd = df_new2.iloc[:,4] - df_new2.iloc[:,3]
Endring_12 = df_new2.iloc[:,4] - df_new2.iloc[:,2]
Endring_covid = df_new2.iloc[:,4] - df_new['2020M02']
Endring_3 = df_new2.iloc[:,4] - df_new2.iloc[:,1]
Endring_5 = df_new2.iloc[:,4] - df_new2.iloc[:,0]
df_new3 = pd.concat([antall, Endring_mnd, Endring_12, Endring_covid,Endring_3, Endring_5], axis=1, keys=['Total','Change last month','Change last year','Change from feb.20','Change last 3 years','Change last 5 years'])
df_new3.to_csv('data_EN/SSB_jobs_industry.csv', index=True)
date_string = tittel_dato.replace("M","")
date_string2 = datetime.strptime(date_string, "%Y%m")
date_string3 = 'Seasonally adjusted numbers for ' + date_string2.strftime ('%B %Y') + '.'
date_string4 = 'Seasonally adjusted numbers for ' + date_string2.strftime ('%B %Y') + ' compared with:'
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')

#Update DW NPfNj (Total last month)
chartid = 'NPfNj'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"describe": {"intro": date_string3}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"annotate": {"notes": riktig_dato}}
    }
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

#Update DW vnqke (Change from feb.20)
chartid = 'vnqke'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"describe": {"intro": date_string3 + " compared with February 2020."}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"annotate": {"notes": riktig_dato}}
    }
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

#Update DW SXwZ8 (Table change in numbers)
chartid = 'SXwZ8'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"describe": {"intro": date_string4}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"annotate": {"notes": riktig_dato}}
    }
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

#Jobs change per industry HIT0e
Endring_mnd_pst = ((df_new2.iloc[:,4] - df_new2.iloc[:,3]) / df_new2.iloc[:,3]*100)
Endring_12_pst = ((df_new2.iloc[:,4] - df_new2.iloc[:,2]) / df_new2.iloc[:,2]*100)
Endring_covid = ((df_new2.iloc[:,4] - df_new['2020M02'])/ df_new['2020M02']*100)
Endring_3_pst = ((df_new2.iloc[:,4] - df_new2.iloc[:,1]) / df_new2.iloc[:,1]*100)
Endring_5_pst = ((df_new2.iloc[:,4] - df_new2.iloc[:,0]) / df_new2.iloc[:,0]*100)
df_new4 = pd.concat([antall, Endring_mnd_pst, Endring_12_pst, Endring_covid, Endring_3_pst, Endring_5_pst], axis=1, keys=['Total','Change last month','Change last year','Change from feb.20','Change last 3 years','Change last 5 years'])
df_new4.to_csv('data_EN/SSB_jobs_industry_change.csv', index=True)
date_string = tittel_dato.replace("M","")
date_string2 = datetime.strptime(date_string, "%Y%m")
date_string3 = 'Seasonally adjusted numbers for ' + date_string2.strftime ('%B %Y') + ' compared with:'
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
chartid = 'HIT0e'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"describe": {"intro": date_string3}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"annotate": {"notes": riktig_dato}}
    }
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