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
        "filter": "top",
        "values": [5
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
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='contents', columns='year', values='value')
df_new.to_csv('data_EN/SSB_unioemp_unions_members.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 'ijQ77'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='national confederation', columns='year', values='value')
df_new.to_csv('data_EN/SSB_unioemp_unions_working_org.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 'NuRRZ'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='national confederation', columns='year', values='value')
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.drop(columns=df3_new.columns[:4], axis=1,inplace=True)
df3_new.to_csv('data_EN/SSB_unioemp_unions_LO.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
chartid = 'FRuq3'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
chartid = 'XU7zR'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='national confederation', columns='year', values='value')
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.drop(columns=df3_new.columns[:4], axis=1,inplace=True)
df3_new.to_csv('data_EN/SSB_unioemp_unions_YS.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
chartid = 'TmM5a'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='national confederation', columns='year', values='value')
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.drop(columns=df3_new.columns[:4], axis=1,inplace=True)
df3_new.to_csv('data_EN/SSB_unioemp_unions_unio.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 'Eo880'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='national confederation', columns='year', values='value')
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.drop(columns=df3_new.columns[:4], axis=1,inplace=True)
df3_new.to_csv('data_EN/SSB_unioemp_unions_akademikerne.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
chartid = 'iOf2c'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='national confederation', columns='year', values='value')
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.drop(columns=df3_new.columns[:4], axis=1,inplace=True)
df3_new.to_csv('data_EN/SSB_unioemp_unions_others.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
chartid = 'zWoFj'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Employer organizations members IORXb (employees) and YWqlt (companies)
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index="employers' associations", columns='year', values='value')
df_new.loc['Totalt']= df_new.sum(skipna=True)
df_new.loc['Andre'] = df_new.loc[['Totalt']].sum(skipna=True)-df_new.loc[['Association of Local Authorities', 'Finance Norway','The Enterprise Federation of Norway','Confederation of Norwegian Business and Industry, total', "The Employers' Association Spekter"]].sum(skipna=True)
df_new_totalt=df_new.loc[['Totalt'],:]

#Companies
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index="employers' associations", columns='year', values='value')
df2_new.loc['Totalt']= df2_new.sum(skipna=True)
df2_new.loc['Andre'] = df2_new.loc[['Totalt']].sum(skipna=True)-df2_new.loc[['Association of Local Authorities', 'Finance Norway','The Enterprise Federation of Norway','Confederation of Norwegian Business and Industry, total', "The Employers' Association Spekter"]].sum(skipna=True)
df2_new_totalt=df2_new.loc[['Totalt'],:]
df_new_final=pd.concat([df_new_totalt, df2_new_totalt], axis=0)
df_new_final.to_csv('data_EN/SSB_unioemp_employers_total.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
chartid = 'IORXb'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

chartid = 'YWqlt'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index="employers' associations", columns='year', values='value')
df_new.loc['Total']= df_new.sum(skipna=True)
df_new.loc['Others'] = df_new.loc[['Total']].sum(skipna=True)-df_new.loc[['Association of Local Authorities', 'Finance Norway','The Enterprise Federation of Norway','Confederation of Norwegian Business and Industry, total', "The Employers' Association Spekter"]].sum(skipna=True)
df_new2=df_new.loc[["The Employers' Association Spekter",'Finance Norway','Association of Local Authorities','The Enterprise Federation of Norway','Confederation of Norwegian Business and Industry, total','Others'],:]
df_new2.to_csv('data_EN/SSB_unioemp_employers_organization_employees.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = '00ZIX'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index="employers' associations", columns='year', values='value')
df_new.loc['Total']= df_new.sum(skipna=True)
df_new.loc['Others'] = df_new.loc[['Total']].sum(skipna=True)-df_new.loc[['Association of Local Authorities', 'Finance Norway','The Enterprise Federation of Norway','Confederation of Norwegian Business and Industry, total', "The Employers' Association Spekter"]].sum(skipna=True)
df_new2=df_new.loc[["The Employers' Association Spekter",'Finance Norway','Association of Local Authorities','The Enterprise Federation of Norway','Confederation of Norwegian Business and Industry, total','Others'],:]
df_new2.to_csv('data_EN/SSB_unioemp_employers_organization_companies.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 'AviZG'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index="employers' associations", columns='year', values='value')
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_b']=df3_new.iloc[:,1]
df3_new['Sist_aar_t']=df3_new.iloc[:,3]
df3_new['Endring_b']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_t']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_b_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_t_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.drop(columns=df3_new.columns[:4], axis=1,inplace=True)
df3_new.to_csv('data_EN/SSB_unionemp_unioemp_employers_table_all.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 'Sw0Qm'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

chartid = 'xdXUi'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index="employers' associations", columns='year', values='value')
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_b']=df3_new.iloc[:,1]
df3_new['Sist_aar_t']=df3_new.iloc[:,3]
df3_new['Endring_b']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_t']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_b_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_t_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.drop(columns=df3_new.columns[:4], axis=1,inplace=True)
df3_new.to_csv('data_EN/SSB_unionemp_unioemp_employers_table_NHO.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y') + ' Numbers not avaliable for all sectoral federations.'

#Update DW
chartid = '78wDf'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

chartid = 'daeM5'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)