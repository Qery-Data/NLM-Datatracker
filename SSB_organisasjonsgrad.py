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
df = dataset.write('dataframe')
df_new = df.pivot(index='statistikkvariabel', columns='år', values='value')
df_new.to_csv('data/SSB_organisasjonsgrad_medlemmer_vs_yrkesaktive.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 'lDM1n'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df_new = df.pivot(index='Landsforening', columns='år', values='value')
df_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_medlemmer_yrkesaktive.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 'sM8OE'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Medlemsutvikling i LO Zub1a og Første side på hovedtabell FwTac (første side)
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
df = dataset.write('dataframe')
df_new = df.pivot(index='Landsforening', columns='år', values='value')

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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='Landsforening', columns='år', values='value')
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.drop(columns=df3_new.columns[:4], axis=1,inplace=True)
df3_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_LO.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
chartid = 'Zub1a'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

chartid = 'FwTac'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='Landsforening', columns='år', values='value')
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.drop(columns=df3_new.columns[:4], axis=1,inplace=True)
df3_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_YS.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 'oDPyE'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='Landsforening', columns='år', values='value')
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.drop(columns=df3_new.columns[:4], axis=1,inplace=True)
df3_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_Unio.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 'mhydR'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'

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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='Landsforening', columns='år', values='value')
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.drop(columns=df3_new.columns[:4], axis=1,inplace=True)
df3_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_Akademikerne.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 'fyMbm'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='Landsforening', columns='år', values='value')
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_m']=df3_new.iloc[:,1]
df3_new['Endring_m']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_m_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Sist_aar_y']=df3_new.iloc[:,3]
df3_new['Endring_y']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_y_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.drop(columns=df3_new.columns[:4], axis=1,inplace=True)
df3_new.to_csv('data/SSB_organisasjonsgrad_arbeidstaker_Andre.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
chartid = '86GF4'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Medlemsutvikling i alle arbeidsgiverorganisasjoner s7168
#Tilsatte
ssburl = 'https://data.ssb.no/api/v0/no/table/03532/'
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
df = dataset.write('dataframe')
df_new = df.pivot(index='arbeidsgivarorganisasjon', columns='år', values='value')
df_new.loc['Totalt']= df_new.sum(skipna=True)
df_new.loc['Andre'] = df_new.loc[['Totalt']].sum(skipna=True)-df_new.loc[['KS', 'Finans Norge','Hovedorganisasjonen Virke','Næringslivets Hovedorganisasjon i alt', 'Arbeidsgiverforeningen SPEKTER']].sum(skipna=True)
df_new_totalt=df_new.loc[['Totalt'],:]

#Medlemsutvikling i alle arbeidsgiverorganisasjoner te3SI
#Bedrifter
ssburl = 'https://data.ssb.no/api/v0/no/table/03532/'
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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='arbeidsgivarorganisasjon', columns='år', values='value')
df2_new.loc['Totalt']= df2_new.sum(skipna=True)
df2_new.loc['Andre'] = df2_new.loc[['Totalt']].sum(skipna=True)-df2_new.loc[['KS', 'Finans Norge','Hovedorganisasjonen Virke','Næringslivets Hovedorganisasjon i alt', 'Arbeidsgiverforeningen SPEKTER']].sum(skipna=True)
df2_new_total=df2_new.loc[['Totalt'],:]
df_new_final=pd.concat([df_new_totalt, df2_new_total], axis=0)
df_new_final.to_csv('data/SSB_organisasjonsgrad_arbeidsgiverorganisasjoner_utvikling_totalt.csv', index=True)

#Medlemsutvikling i alle arbeidsgiverorganisasjoner tilsatte cvRck
ssburl = 'https://data.ssb.no/api/v0/no/table/03532/'
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
df = dataset.write('dataframe')
df_new = df.pivot(index='arbeidsgivarorganisasjon', columns='år', values='value')
df_new.loc['Totalt']= df_new.sum(skipna=True)
df_new.loc['Andre'] = df_new.loc[['Totalt']].sum(skipna=True)-df_new.loc[['KS', 'Finans Norge','Hovedorganisasjonen Virke','Næringslivets Hovedorganisasjon i alt', 'Arbeidsgiverforeningen SPEKTER']].sum(skipna=True)
df_new2=df_new.loc[['Arbeidsgiverforeningen SPEKTER','Finans Norge','KS','Hovedorganisasjonen Virke','Næringslivets Hovedorganisasjon i alt','Andre'],:]
df_new2.to_csv('data/SSB_organisasjonsgrad_arbeidsgiverorganisasjoner_utvikling.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 'cvRck'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Medlemsutvikling i alle arbeidsgiverorganisasjoner bedrifter 4Wu2r
ssburl = 'https://data.ssb.no/api/v0/no/table/03532/'
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
df = dataset.write('dataframe')
df_new = df.pivot(index='arbeidsgivarorganisasjon', columns='år', values='value')
df_new.loc['Totalt']= df_new.sum(skipna=True)
df_new.loc['Andre'] = df_new.loc[['Totalt']].sum(skipna=True)-df_new.loc[['KS', 'Finans Norge','Hovedorganisasjonen Virke','Næringslivets Hovedorganisasjon i alt', 'Arbeidsgiverforeningen SPEKTER']].sum(skipna=True)
df_new2=df_new.loc[['Arbeidsgiverforeningen SPEKTER','Finans Norge','KS','Hovedorganisasjonen Virke','Næringslivets Hovedorganisasjon i alt','Andre'],:]
df_new2.to_csv('data/SSB_organisasjonsgrad_arbeidsgiverorganisasjoner_utvikling_bedrifter.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = '4Wu2r'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Medlemsutvikling tabell alle arbeidsgiverorganisasjoner 6lFAy (bedrifter) zbqaq (tilsatte)
#Bedrifter 
ssburl = 'https://data.ssb.no/api/v0/no/table/03532/'
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
df = dataset.write('dataframe')
df_new = df.pivot(index='arbeidsgivarorganisasjon', columns='år', values='value')
#Tilsatte
ssburl = 'https://data.ssb.no/api/v0/no/table/03532/'
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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='arbeidsgivarorganisasjon', columns='år', values='value')
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_b']=df3_new.iloc[:,1]
df3_new['Sist_aar_t']=df3_new.iloc[:,3]
df3_new['Endring_b']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_t']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_b_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_t_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.drop(columns=df3_new.columns[:4], axis=1,inplace=True)
df3_new.to_csv('data/SSB_organisasjonsgrad_arbeidsgiverorganisasjoner_tabell_alle.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
chartid = '6lFAy'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

chartid = 'zbqaq'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Medlemsutvikling tabell NHO S3VWa (bedrifter) 6m3dp (tilsatte)
#Bedrifter 
ssburl = 'https://data.ssb.no/api/v0/no/table/03532/'
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
df = dataset.write('dataframe')
df_new = df.pivot(index='arbeidsgivarorganisasjon', columns='år', values='value')
#Tilsatte
ssburl = 'https://data.ssb.no/api/v0/no/table/03532/'
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
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='arbeidsgivarorganisasjon', columns='år', values='value')
df3_new=pd.concat([df_new, df2_new], axis=1)
df3_new['Sist_aar_b']=df3_new.iloc[:,1]
df3_new['Sist_aar_t']=df3_new.iloc[:,3]
df3_new['Endring_b']=df3_new.iloc[:,1]-df3_new.iloc[:,0]
df3_new['Endring_t']=df3_new.iloc[:,3]-df3_new.iloc[:,2]
df3_new['Endring_b_pst']=(df3_new.iloc[:,1]-df3_new.iloc[:,0])/df3_new.iloc[:,0]*100
df3_new['Endring_t_pst']=(df3_new.iloc[:,3]-df3_new.iloc[:,2])/df3_new.iloc[:,2]*100
df3_new.drop(columns=df3_new.columns[:4], axis=1,inplace=True)
df3_new.to_csv('data/SSB_organisasjonsgrad_arbeidsgiverorganisasjoner_tabell_NHO.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y') + ' Tall ikke tilgjengelig fra SSB for alle landsforeningene.'
#Update DW
chartid = 'S3VWa'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

chartid = '6m3dp'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)