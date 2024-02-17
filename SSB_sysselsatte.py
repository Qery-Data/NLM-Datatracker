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

#Andel sysselsatte wo6kb
ssburl = 'https://data.ssb.no/api/v0/no/table/13760/'
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
          "15-74"
        ]
      }
    },
    {
      "code": "Justering",
      "selection": {
        "filter": "item",
        "values": [
          "T"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "SysselProsBefolkn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "all",
        "values": [
          "*"
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
df.to_csv('data/SSB_sysselsatte_pst.csv', index=False)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y') + ' Merk at y-aksen er kuttet for å bedre vise utviklingen.'
#Update DW
# chartid = 'wo6kb'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel sysselsatte etter kjønn AE6ZC
ssburl = 'https://data.ssb.no/api/v0/no/table/13760/'
query = {
  "query": [
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "1",
          "2"
        ]
      }
    },
    {
      "code": "Alder",
      "selection": {
        "filter": "item",
        "values": [
          "15-74"
        ]
      }
    },
    {
      "code": "Justering",
      "selection": {
        "filter": "item",
        "values": [
          "T"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "SysselProsBefolkn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "all",
        "values": ["*"]
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
df_new = df.pivot(index='kjønn', columns='måned', values='value')
df_new.to_csv('data/SSB_sysselsatte_kjønn_pst.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y') + ' Merk at y-aksen er kuttet for å bedre vise utviklingen.'
#Update DW
# chartid = 'AE6ZC'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel sysselsatte etter alder nFRTH
ssburl = 'https://data.ssb.no/api/v0/no/table/13760/'
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
          "15-24",
          "25-74"
        ]
      }
    },
    {
      "code": "Justering",
      "selection": {
        "filter": "item",
        "values": [
          "T"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "SysselProsBefolkn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "all",
        "values": ["*"]
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
df_new = df.pivot(index='alder', columns='måned', values='value')
df_new.to_csv('data/SSB_sysselsatte_alder_pst.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y') + ' Merk at y-aksen er kuttet for å bedre vise utviklingen.'
#Update DW
# chartid = 'nFRTH'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel midlertidig ansatte BOz7R
ssburl = 'https://data.ssb.no/api/v0/no/table/05611/'
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
          "15-74"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "AnsattProsent"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [41]
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
df_new = df.pivot(index='statistikkvariabel', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[0,4,8,12,16,20,24,28,32,36,40]]
total = df_new2.iloc[:,10]
title_date = (total.name)
df_new2.to_csv('data/SSB_midlertidig_ansatte_kvartal.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
date_string2 = title_date[-1:]
date_string3 = title_date[0:4]
date_int5 = int(date_string3)
date_int6 = date_int5 - 1
date_string7 = str(date_int6)
date_string4 = 'I prosent av de sysselsatte. Tall for ' + date_string2 + '.kvartal de siste ti årene.'
#Update DW
# chartid = 'BOz7R'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"describe": {"intro": date_string4}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel midlertidig ansatte etter kjønn smYJ7
ssburl = 'https://data.ssb.no/api/v0/no/table/05611/'
query = {
  "query": [
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "1",
          "2"
        ]
      }
    },
    {
      "code": "Alder",
      "selection": {
        "filter": "item",
        "values": [
          "15-74"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "AnsattProsent"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [41]
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
df_new = df.pivot(index='kjønn', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[0,4,8,12,16,20,24,28,32,36,40]]
total = df_new2.iloc[:,10]
title_date = (total.name)
df_new2.to_csv('data/SSB_midlertidig_ansatte_kjonn_kvartal.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
date_string2 = title_date[-1:]
date_string3 = title_date[0:4]
date_int5 = int(date_string3)
date_int6 = date_int5 - 1
date_string7 = str(date_int6)
date_string4 = 'I prosent av de sysselsatte. Tall for ' + date_string2 + '.kvartal de siste ti årene.'
#Update DW
# chartid = 'smYJ7'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"describe": {"intro": date_string4}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel midlertidig ansatte etter alder EYDM9
ssburl = 'https://data.ssb.no/api/v0/no/table/05611/'
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
          "15-24",
          "25-29",
          "30-39",
          "40-54",
          "55-74"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "AnsattProsent"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [41]
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
df_new = df.pivot(index='alder', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[0,4,8,12,16,20,24,28,32,36,40]]
total = df_new2.iloc[:,10]
title_date = (total.name)
df_new2.to_csv('data/SSB_midlertidig_ansatte_alder_kvartal.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
date_string2 = title_date[-1:]
date_string3 = title_date[0:4]
date_int5 = int(date_string3)
date_int6 = date_int5 - 1
date_string7 = str(date_int6)
date_string4 = 'I prosent av de sysselsatte. Tall for ' + date_string2 + '.kvartal de siste ti årene.'
#Update DW
# chartid = 'EYDM9'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"describe": {"intro": date_string4}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)