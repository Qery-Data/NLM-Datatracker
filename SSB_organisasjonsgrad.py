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

#Utvikling medlemmer og yrkesaktive lDM1n
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values": [
          "00"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2016",
          "2017",
          "2018",
          "2019",
          "2020"
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
df_new = df.pivot(index='statistikkvariabel', columns='år', values='value')
df_new.to_csv('data/SSB_organisasjonsgrad_medlemmer_vs_yrkesaktive.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
url = "https://api.datawrapper.de/v3/charts/lDM1n/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Utvikling i antall yrkesaktivemedlemmer i arbeidstakerorganisasjoner sM8OE
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "12",
          "20",
          "25",
          "35"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "YrkesaktivMedl"
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
df_new = df.pivot(index='Landsforening', columns='år', values='value')
df_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_medlemmer_yrkesaktive.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
url = "https://api.datawrapper.de/v3/charts/sM8OE/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Endring i antall yrkesaktivemedlemmer i hovedsammenslutningene VuaM6
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values": [
          "00",
          "01",
          "12",
          "20",
          "25",
          "35"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "YrkesaktivMedl"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          5
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
df_new = df.pivot(index='Landsforening', columns='år', values='value')
df_new['Sist_aar'] = df_new.iloc[:,4]
df_new['Endring sist år'] = (df_new.iloc[:,4]-df_new.iloc[:,3])/df_new.iloc[:,3]*100
df_new['Endring siste fem år'] = (df_new.iloc[:,4]-df_new.iloc[:,0])/df_new.iloc[:,0]*100
df_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_medlemmer_yrkesaktive_hoved.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato_sist=df_new.columns[4]
dato_nest_sist=df_new.columns[3]
dato_fem_aar=df_new.columns[0]
date_string = 'Antall per 31.desember ' + dato_sist +'. Endring fra ' + dato_nest_sist + '-' + dato_sist + ' og fra ' + dato_fem_aar + '-' + dato_sist + ' i prosent.'
#Update DW
url = "https://api.datawrapper.de/v3/charts/VuaM6/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/VuaM6/"
payload = {"metadata": {"describe": {"intro": date_string}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
#Utvikling LO 
#Medlemsutvikling i LO
#Medlemmer totalt
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "53",
          "07",
          "02",
          "04",
          "51",
          "56",
          "05",
          "50",
          "58",
          "09",
          "62",
          "64",
          "40",
          "06",
          "08",
          "69"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Ansatte"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2019",
          "2020"
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
df_new = df.pivot(index='Landsforening', columns='år', values='value')
#Medlemsutvikling i LO Zub1a
#Yrkesaktive totalt
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "53",
          "07",
          "02",
          "04",
          "51",
          "56",
          "05",
          "50",
          "58",
          "09",
          "62",
          "64",
          "40",
          "06",
          "08",
          "69"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "YrkesaktivMedl"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2019",
          "2020"
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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='Landsforening', columns='år', values='value')
df2_new=df2_new.rename(columns={"2019": "2019Y", "2020": "2020Y"})
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_LO.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato_sist=df_new.columns[1]
dato_nest_sist=df_new.columns[0]
date_string = 'Antall yrkesaktive medlemmer per 31.desember ' + dato_sist +'. Endring fra ' + dato_nest_sist + ' i antall og prosent.'
#Update DW
url = "https://api.datawrapper.de/v3/charts/Zub1a/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/Zub1a/"
payload = {"metadata": {"describe": {"intro": date_string}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Medlemsutvikling i YS oDPyE
#Medlemmer totalt
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values": [
          "12",
          "18",
          "13",
          "14",
          "41",
          "42",
          "46",
          "68",
          "19",
          "44"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Ansatte"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2019",
          "2020"
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
df_new = df.pivot(index='Landsforening', columns='år', values='value')
#Medlemsutvikling i YS oDPyE
#Yrkesaktive totalt
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values": [
          "12",
          "18",
          "13",
          "14",
          "41",
          "42",
          "46",
          "68",
          "19",
          "44"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "YrkesaktivMedl"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2019",
          "2020"
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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='Landsforening', columns='år', values='value')
df2_new=df2_new.rename(columns={"2019": "2019Y", "2020": "2020Y"})
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_YS.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato_sist=df_new.columns[1]
dato_nest_sist=df_new.columns[0]
date_string = 'Antall yrkesaktive medlemmer per 31.desember ' + dato_sist +'. Endring fra ' + dato_nest_sist + ' i antall og prosent.'
#Update DW
url = "https://api.datawrapper.de/v3/charts/oDPyE/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/oDPyE/"
payload = {"metadata": {"describe": {"intro": date_string}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Medlemsutvikling i Unio mhydR
#Medlemmer totalt
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values": [
          "20",
          "52",
          "55",
          "45",
          "59",
          "24",
          "77",
          "22",
          "23",
          "21"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Ansatte"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2019",
          "2020"
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
df_new = df.pivot(index='Landsforening', columns='år', values='value')
#Medlemsutvikling i Unio mhydR
#Yrkesaktive totalt
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values": [
          "20",
          "52",
          "55",
          "45",
          "59",
          "24",
          "77",
          "22",
          "23",
          "21"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "YrkesaktivMedl"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2019",
          "2020"
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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='Landsforening', columns='år', values='value')
df2_new=df2_new.rename(columns={"2019": "2019Y", "2020": "2020Y"})
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_Unio.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato_sist=df_new.columns[1]
dato_nest_sist=df_new.columns[0]
date_string = 'Antall yrkesaktive medlemmer per 31.desember ' + dato_sist +'. Endring fra ' + dato_nest_sist + ' i antall og prosent.'
#Update DW
url = "https://api.datawrapper.de/v3/charts/mhydR/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/mhydR/"
payload = {"metadata": {"describe": {"intro": date_string}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Medlemsutvikling i Akademikerne fyMbm
#Medlemmer totalt
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values":[ 
          "25",
          "27",
          "31",
          "54",
          "29",
          "34",
          "28",
          "63",
          "33",
          "32",
          "26"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Ansatte"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2019",
          "2020"
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
df_new = df.pivot(index='Landsforening', columns='år', values='value')
#Medlemsutvikling i Akademikerne fyMbm
#Yrkesaktive totalt
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values": [
          "25",
          "27",
          "31",
          "54",
          "29",
          "34",
          "28",
          "63",
          "33",
          "32",
          "26"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "YrkesaktivMedl"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2019",
          "2020"
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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='Landsforening', columns='år', values='value')
df2_new=df2_new.rename(columns={"2019": "2019Y", "2020": "2020Y"})
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_Akademikerne.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato_sist=df_new.columns[1]
dato_nest_sist=df_new.columns[0]
date_string = 'Antall yrkesaktive medlemmer per 31.desember ' + dato_sist +'. Endring fra ' + dato_nest_sist + ' i antall og prosent.'
#Update DW
url = "https://api.datawrapper.de/v3/charts/fyMbm/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/fyMbm/"
payload = {"metadata": {"describe": {"intro": date_string}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Medlemsutvikling i andre arbeidstakerorganisasjoner 86GF4
#Medlemmer totalt
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values":[
          "35",
          "71",
          "72",
          "74",
          "38",
          "70",
          "57",
          "37",
          "60",
          "76",
          "61",
          "75",
          "39",
          "73",
          "65",
          "67"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Ansatte"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2019",
          "2020"
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
df_new = df.pivot(index='Landsforening', columns='år', values='value')
#Medlemsutvikling i i andre arbeidstakerorganisasjoner 86GF4
#Yrkesaktive totalt
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values": [
          "35",
          "71",
          "72",
          "74",
          "38",
          "70",
          "57",
          "37",
          "60",
          "76",
          "61",
          "75",
          "39",
          "73",
          "65",
          "67"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "YrkesaktivMedl"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2019",
          "2020"
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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='Landsforening', columns='år', values='value')
df2_new=df2_new.rename(columns={"2019": "2019Y", "2020": "2020Y"})
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_Andre.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato_sist=df_new.columns[1]
dato_nest_sist=df_new.columns[0]
date_string = 'Antall yrkesaktive medlemmer per 31.desember ' + dato_sist +'. Endring fra ' + dato_nest_sist + ' i antall og prosent.'
#Update DW
url = "https://api.datawrapper.de/v3/charts/86GF4/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/86GF4/"
payload = {"metadata": {"describe": {"intro": date_string}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

# ***
#Utvikling i antall medlemmer i arbeidstakerorganisasjoner l44VI
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values": [
          "00",
          "01",
          "12",
          "20",
          "25",
          "35"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Ansatte"
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
df_new = df.pivot(index='Landsforening', columns='år', values='value')
df_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_medlemmer_utvikling_alle.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
url = "https://api.datawrapper.de/v3/charts/l44VI/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)