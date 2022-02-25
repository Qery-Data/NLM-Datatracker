from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import pandas as pd
os.makedirs('data', exist_ok=True)
access_token = os.getenv('DW_TOKEN')

#Trade union members ijQ77
ssburl = 'https://data.ssb.no/api/v0/en/table/03546/'
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
df_new = df.pivot(index='contents', columns='year', values='value')
df_new.to_csv('data_EN/SSB_unioemp_union_members.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
chartid = 'ijQ77'
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

#Trade unions working members by organization NuRRZ
ssburl = 'https://data.ssb.no/api/v0/en/table/03546/'
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
df_new = df.pivot(index='national confederation', columns='year', values='value')
df_new.to_csv('data_EN/SSB_unioemp_union_working_org.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
chartid = 'NuRRZ'
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

#Members in LO XU7zR and first page on main table FRuq3

#Members in LO total
ssburl = 'https://data.ssb.no/api/v0/en/table/03546/'
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
        "filter": "top",
        "values": [
          2
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
df_new = df.pivot(index='national confederation', columns='year', values='value')

#Working members in LO total
ssburl = 'https://data.ssb.no/api/v0/en/table/03546/'
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
        "filter": "top",
        "values": [
          2
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
df2_new = df2.pivot(index='national confederation', columns='year', values='value')
df2_new=df2_new.rename(columns={"2019": "2019Y", "2020": "2020Y"})
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.to_csv('data_EN/SSB_unioemp_unions_LO.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato_sist=df_new.columns[1]
dato_nest_sist=df_new.columns[0]
date_string = 'Members by national confederation and associated unions in ' + dato_sist
date_string2 = 'Members of associated unions in LO in ' + dato_sist

#Update DW
chartid = 'FRuq3'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"title": date_string}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

chartid = 'XU7zR'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"title": date_string2}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

#Members in YS TmM5a
#Members in YS total
ssburl = 'https://data.ssb.no/api/v0/en/table/03546/'
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
        "filter": "top",
        "values": [
          2
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
df_new = df.pivot(index='national confederation', columns='year', values='value')

#Working members in YS total
ssburl = 'https://data.ssb.no/api/v0/en/table/03546/'
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
        "filter": "top",
        "values": [
          2
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
df2_new = df2.pivot(index='national confederation', columns='year', values='value')
df2_new=df2_new.rename(columns={"2019": "2019Y", "2020": "2020Y"})
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.to_csv('data_EN/SSB_unioemp_unions_YS.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato_sist=df_new.columns[1]
dato_nest_sist=df_new.columns[0]
date_string = 'Members of associated unions in YS in ' + dato_sist

#Update DW
chartid = 'TmM5a'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"title": date_string}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

#Members of Unio Eo880
#Members in total
ssburl = 'https://data.ssb.no/api/v0/en/table/03546/'
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
        "filter": "top",
        "values": [
          2
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
df_new = df.pivot(index='national confederation', columns='year', values='value')

#Working members in total
ssburl = 'https://data.ssb.no/api/v0/en/table/03546/'
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
        "filter": "top",
        "values": [
          2
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
df2_new = df2.pivot(index='national confederation', columns='year', values='value')
df2_new=df2_new.rename(columns={"2019": "2019Y", "2020": "2020Y"})
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.to_csv('data_EN/SSB_unioemp_unions_unio.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato_sist=df_new.columns[1]
dato_nest_sist=df_new.columns[0]
date_string = 'Members of associated unions in Unio in ' + dato_sist
#Update DW
chartid = 'Eo880'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"title": date_string}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

#Members in Akademikerne iOf2c
#Members in total
ssburl = 'https://data.ssb.no/api/v0/en/table/03546/'
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
        "filter": "top",
        "values": [
          2
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
df_new = df.pivot(index='national confederation', columns='year', values='value')
#Working members
ssburl = 'https://data.ssb.no/api/v0/en/table/03546/'
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
        "filter": "top",
        "values": [
          2
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
df2_new = df2.pivot(index='national confederation', columns='year', values='value')
df2_new=df2_new.rename(columns={"2019": "2019Y", "2020": "2020Y"})
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.to_csv('data_EN/SSB_unioemp_unions_akademikerne.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato_sist=df_new.columns[1]
dato_nest_sist=df_new.columns[0]
date_string = 'Members of associated unions in Akademikerne in ' + dato_sist
#Update DW
chartid = 'iOf2c'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"title": date_string}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

#Members of other associations zWoFj
#Medmbers in total
ssburl = 'https://data.ssb.no/api/v0/en/table/03546/'
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
        "filter": "top",
        "values": [
          2
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
df_new = df.pivot(index='national confederation', columns='year', values='value')

#Working members in total
ssburl = 'https://data.ssb.no/api/v0/en/table/03546/'
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
        "filter": "top",
        "values": [
          2
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
df2_new = df2.pivot(index='national confederation', columns='year', values='value')
df2_new=df2_new.rename(columns={"2019": "2019Y", "2020": "2020Y"})
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.to_csv('data_EN/SSB_unioemp_unions_others.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato_sist=df_new.columns[1]
dato_nest_sist=df_new.columns[0]
date_string = 'Members of other associations in ' + dato_sist
#Update DW
chartid = 'zWoFj'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"title": date_string}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

#Employer organizations members IvQAO (employees) and YWqlt (companies)
#Employees
ssburl = 'https://data.ssb.no/api/v0/en/table/03532/'
query = {
  "query": [
    {
      "code": "ArbGivere",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02",
          "08",
          "42",
          "12",
          "03",
          "04",
          "44",
          "06",
          "07",
          "09",
          "73",
          "05",
          "10",
          "11",
          "59",
          "13",
          "14",
          "15",
          "16",
          "46",
          "18"
        ]
      }
    },
    {
      "code": "ArbForhold",
      "selection": {
        "filter": "item",
        "values": [
          "00"
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
df_new = df.pivot(index="employers' associations", columns='year', values='value')
df_new.loc['Totalt']= df_new.sum(skipna=True)
df_new.loc['Andre'] = df_new.loc[['Totalt']].sum(skipna=True)-df_new.loc[['Association of Local Authorities', 'Finance Norway','The Enterprise Federation of Norway','Confederation of Norwegian Business and Industry, total', "The Employers' Association Spekter"]].sum(skipna=True)
df_new_totalt=df_new.loc[['Totalt'],:]

#Bedrifter
ssburl = 'https://data.ssb.no/api/v0/en/table/03532/'
query = {
  "query": [
    {
      "code": "ArbGivere",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02",
          "08",
          "42",
          "12",
          "03",
          "04",
          "44",
          "06",
          "07",
          "09",
          "73",
          "05",
          "10",
          "11",
          "59",
          "13",
          "14",
          "15",
          "16",
          "46",
          "18"
        ]
      }
    },
    {
      "code": "ArbForhold",
      "selection": {
        "filter": "item",
        "values": [
          "00"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Bedrifter"
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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index="employers' associations", columns='year', values='value')
df2_new.loc['Totalt']= df2_new.sum(skipna=True)
df2_new.loc['Andre'] = df2_new.loc[['Totalt']].sum(skipna=True)-df2_new.loc[['Association of Local Authorities', 'Finance Norway','The Enterprise Federation of Norway','Confederation of Norwegian Business and Industry, total', "The Employers' Association Spekter"]].sum(skipna=True)
df2_new_totalt=df2_new.loc[['Totalt'],:]
df_new_final=pd.concat([df_new_totalt, df2_new_totalt], axis=0)
df_new_final.to_csv('data_EN/SSB_unioemp_employers_total.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')

#Update DW
chartid = 'IvQAO'
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

chartid = 'YWqlt'
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

#Members by employer organization employees 00ZIX
ssburl = 'https://data.ssb.no/api/v0/en/table/03532/'
query = {
  "query": [
    {
      "code": "ArbGivere",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02",
          "08",
          "42",
          "12",
          "03",
          "04",
          "44",
          "06",
          "07",
          "09",
          "73",
          "05",
          "10",
          "11",
          "59",
          "13",
          "14",
          "15",
          "16",
          "46",
          "18"
        ]
      }
    },
    {
      "code": "ArbForhold",
      "selection": {
        "filter": "item",
        "values": [
          "00"
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
df_new = df.pivot(index="employers' associations", columns='year', values='value')
df_new.loc['Totalt']= df_new.sum(skipna=True)
df_new.loc['Andre'] = df_new.loc[['Totalt']].sum(skipna=True)-df_new.loc[['Association of Local Authorities', 'Finance Norway','The Enterprise Federation of Norway','Confederation of Norwegian Business and Industry, total', "The Employers' Association Spekter"]].sum(skipna=True)
df_new2=df_new.loc[["The Employers' Association Spekter",'Finance Norway','Association of Local Authorities','The Enterprise Federation of Norway','Confederation of Norwegian Business and Industry, total','Andre'],:]
df_new2.to_csv('data_EN/SSB_unioemp_employers_organization_employees.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
chartid = '00ZIX'
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

#Members by employer organization companies AviZG
ssburl = 'https://data.ssb.no/api/v0/en/table/03532/'
query = {
  "query": [
    {
      "code": "ArbGivere",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02",
          "08",
          "42",
          "12",
          "03",
          "04",
          "44",
          "06",
          "07",
          "09",
          "73",
          "05",
          "10",
          "11",
          "59",
          "13",
          "14",
          "15",
          "16",
          "46",
          "18"
        ]
      }
    },
    {
      "code": "ArbForhold",
      "selection": {
        "filter": "item",
        "values": [
          "00"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Bedrifter"
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
df_new = df.pivot(index="employers' associations", columns='year', values='value')
df_new.loc['Totalt']= df_new.sum(skipna=True)
df_new.loc['Andre'] = df_new.loc[['Totalt']].sum(skipna=True)-df_new.loc[['Association of Local Authorities', 'Finance Norway','The Enterprise Federation of Norway','Confederation of Norwegian Business and Industry, total', "The Employers' Association Spekter"]].sum(skipna=True)
df_new2=df_new.loc[["The Employers' Association Spekter",'Finance Norway','Association of Local Authorities','The Enterprise Federation of Norway','Confederation of Norwegian Business and Industry, total','Andre'],:]
df_new2.to_csv('data_EN/SSB_unioemp_employers_organization_companies.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
chartid = 'AviZG'
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


#Employer organizations all Sw0Qm (companies) xdXUi (employees)
#Companies 
ssburl = 'https://data.ssb.no/api/v0/en/table/03532/'
query = {
  "query": [
    {
      "code": "ArbGivere",
      "selection": {
        "filter": "item",
        "values": [
          "02",
          "08",
          "42",
          "03",
          "04",
          "06",
          "09",
          "73",
          "05",
          "11",
          "59",
          "13",
          "14",
          "15",
          "18"
        ]
      }
    },
    {
      "code": "ArbForhold",
      "selection": {
        "filter": "item",
        "values": [
          "00"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Bedrifter"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          2
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
df_new = df.pivot(index="employers' associations", columns='year', values='value')
#Tilsatte
ssburl = 'https://data.ssb.no/api/v0/en/table/03532/'
query = {
  "query": [
    {
      "code": "ArbGivere",
      "selection": {
        "filter": "item",
        "values": [
          "02",
          "08",
          "42",
          "03",
          "04",
          "06",
          "09",
          "73",
          "05",
          "11",
          "59",
          "13",
          "14",
          "15",
          "18"
        ]
      }
    },
    {
      "code": "ArbForhold",
      "selection": {
        "filter": "item",
        "values": [
          "00"
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
        "filter": "top",
        "values": [
          2
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
df2_new = df2.pivot(index="employers' associations", columns='year', values='value')
df2_new=df2_new.rename(columns={"2019": "2019Y", "2020": "2020Y"})
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_b']=df3_new.iloc[:,1]
df3_new['Sist_aar_t']=df3_new.iloc[:,3]
df3_new['Endring_b']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_t']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_b_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_t_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.to_csv('data_EN/SSB_unionemp_unioemp_employers_table_all.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato_sist=df_new.columns[1]
dato_nest_sist=df_new.columns[0]
date_string = 'Employer organizations: Number of employees in member companies in ' + dato_sist
date_string2 = 'Employer organizations: Number of member companies in ' + dato_sist
#Update DW
chartid = 'Sw0Qm'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"title": date_string2}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

chartid = 'xdXUi'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"title": date_string}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

#NHO 78wDf (companies) daeM5 (employees)
#Companies 
ssburl = 'https://data.ssb.no/api/v0/en/table/03532/'
query = {
  "query": [
    {
      "code": "ArbGivere",
      "selection": {
        "filter": "item",
        "values": [
          "18",
          "19",
          "20",
          "21",
          "62",
          "69",
          "32",
          "63",
          "27",
          "58",
          "22"
        ]
      }
    },
    {
      "code": "ArbForhold",
      "selection": {
        "filter": "item",
        "values": [
          "00"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Bedrifter"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          2
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
df_new = df.pivot(index="employers' associations", columns='year', values='value')
#Employees
ssburl = 'https://data.ssb.no/api/v0/en/table/03532/'
query = {
  "query": [
    {
      "code": "ArbGivere",
      "selection": {
        "filter": "item",
        "values": [
          "18",
          "19",
          "20",
          "21",
          "62",
          "69",
          "32",
          "63",
          "27",
          "58",
          "22"
        ]
      }
    },
    {
      "code": "ArbForhold",
      "selection": {
        "filter": "item",
        "values": [
          "00"
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
        "filter": "top",
        "values": [
          2
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
df2_new = df2.pivot(index="employers' associations", columns='year', values='value')
df2_new=df2_new.rename(columns={"2019": "2019Y", "2020": "2020Y"})
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_b']=df3_new.iloc[:,1]
df3_new['Sist_aar_t']=df3_new.iloc[:,3]
df3_new['Endring_b']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_t']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_b_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_t_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.to_csv('data_EN/SSB_unionemp_unioemp_employers_table_NHO.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y') + ' Numbers not avaliable for all sectoral federations.'
dato_sist=df_new.columns[1]
dato_nest_sist=df_new.columns[0]
date_string = 'NHO: Number of employees in member companies by sectoral federation in ' + dato_sist
date_string2 = 'NHO: Number of member companies by sectoral federation in ' + dato_sist

#Update DW
chartid = '78wDf'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"title": date_string2}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

chartid = 'daeM5'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"title": date_string}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

#***
#Medlemsutvikling i hovedsammenslutningene VuaM6
#Medlemmer totalt
ssburl = 'https://data.ssb.no/api/v0/no/table/03546/'
query = {
  "query": [
    {
      "code": "NHO",
      "selection": {
        "filter": "item",
        "values":[
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

#Medlemsutvikling i hovedsammenslutningene VuaM6
#Yrkesaktive totalt
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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='Landsforening', columns='år', values='value')
df2_new=df2_new.rename(columns={"2016": "2016Y", "2017": "2017Y", "2018": "2018Y","2019": "2019Y", "2020": "2020Y"})
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,4]
df3_new['Sist_aar_y']=df3_new.iloc[:,9]
df3_new['Endring_m']=df3_new.iloc[:,4]-df3_new.iloc[:,3]
df3_new['Endring_y']=df3_new.iloc[:,9]-df3_new.iloc[:,8]
df3_new['Endring_m_pst']=(df3_new.iloc[:,4]-df3_new.iloc[:,3])/df3_new.iloc[:,3]*100
df3_new['Endring_y_pst']=(df3_new.iloc[:,9]-df3_new.iloc[:,8])/df3_new.iloc[:,8]*100
df3_new['Endring_m_pst_5']=(df3_new.iloc[:,4]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_y_pst_5']=(df3_new.iloc[:,9]-df3_new.iloc[:,5])/df3_new.iloc[:,5]*100
df3_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_medlemmer_yrkesaktive_hoved.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato_sist=df_new.columns[1]
dato_nest_sist=df_new.columns[0]
date_string = 'Antall yrkesaktive medlemmer per 31.desember ' + dato_sist +'. Endring fra ' + dato_nest_sist + ' i antall og prosent.'
#Update DW
chartid = 'VuaM6'
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
chartid = 'l44VI'
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


#Medlemsutvikling i alle hoved og forbund b2xCT
#Medlemmer totalt
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
          "69",
          "12",
          "18",
          "13",
          "14",
          "41",
          "42",
          "46",
          "68",
          "19",
          "44",
          "20",
          "52",
          "55",
          "45",
          "59",
          "24",
          "77",
          "22",
          "23",
          "21",
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
          "26",
          "35",
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
        "filter": "top",
        "values": [
          2
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
#Medlemsutvikling i alle hoved og forbund b2xCT
#Yrkesaktive totalt
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
          "69",
          "12",
          "18",
          "13",
          "14",
          "41",
          "42",
          "46",
          "68",
          "19",
          "44",
          "20",
          "52",
          "55",
          "45",
          "59",
          "24",
          "77",
          "22",
          "23",
          "21",
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
          "26",
          "35",
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
        "filter": "top",
        "values": [
          2
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
df3_new.to_csv('data/SSB_organisasjonsgrad_medlemmer_alle_hoved_og_forbund.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato_sist=df_new.columns[1]
dato_nest_sist=df_new.columns[0]
date_string = 'Antall yrkesaktive medlemmer per 31.desember ' + dato_sist +'. Endring fra ' + dato_nest_sist + ' i antall og prosent.'
#Update DW
chartid = 'b2xCT'
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