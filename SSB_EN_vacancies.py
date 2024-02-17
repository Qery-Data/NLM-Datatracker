from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import pandas as pd
os.makedirs('data', exist_ok=True)
access_token = os.getenv('DW_TOKEN')

#Number of vacancies Xytpo
ssburl = 'https://data.ssb.no/api/v0/en/table/11587/'
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df.to_csv('data_EN/SSB_vacancies.csv', index=False)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
# chartid = 'Xytpo'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Vacancy rate JAhWK
ssburl = 'https://data.ssb.no/api/v0/en/table/11587/'
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df.to_csv('data_EN/SSB_vacancies_rate.csv', index=False)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
# chartid = 'JAhWK'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Vacancies by industry pkz3S
ssburl = 'https://data.ssb.no/api/v0/en/table/11587/'
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
            1
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
df_new = df.pivot(index='industry (SIC2007)', columns='contents', values='value')
df_new.to_csv('data_EN/SSB_vacancies_industry.csv', index=True)
total = df.iloc[0,2]
title_date = (total)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
date_string2 = title_date[-1:]
date_string3 = title_date[0:4]
date_string4 = 'Data for Q' + date_string2 + ' ' + date_string3 + '.'
date_string5 = 'Data for Q' + date_string2 + ' ' + date_string3 + '. In % of total jobs.'
#Update DW pkz3S
# chartid = 'pkz3S'
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

#Change in the number of vacancies by industry tvsEy
ssburl = 'https://data.ssb.no/api/v0/en/table/11587/'
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df_new = df.pivot(index='industry (SIC2007)', columns='quarter', values='value')
df_new2 = df_new.iloc[:,[0,8,11,12]]
Change_quarter = df_new.iloc[:,12]
Change_last_quarter = df_new2.iloc[:,3] - df_new2.iloc[:,2]
Change_12_months = df_new2.iloc[:,3] - df_new2.iloc[:,1]
Change_3y = df_new2.iloc[:,3] - df_new2.iloc[:,0]
df_new3 = pd.concat([Change_quarter, Change_last_quarter, Change_12_months, Change_3y], axis=1, keys=['Last quarter','Change last quarter','Change last year','Change last 3 years'])
df_new3.to_csv('data_EN/SSB_vacancies_industry_change_numbers.csv', index=True)
total = df_new2.iloc[:,3]
title_date = (total.name)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last updated: ' + parsed_date.strftime ('%d/%m/%y')
date_string2 = title_date[-1:]
date_string3 = title_date[0:4]
date_string4 = 'Data for Q' + date_string2 + ' ' + date_string3 + ' compared with:'
#Update DW
# chartid = 'tvsEy'
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

#Change in the job vacancy rate by industry jj7hk
ssburl = 'https://data.ssb.no/api/v0/en/table/11587/'
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
          "LedigeStillingerPros"
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df_new = df.pivot(index='industry (SIC2007)', columns='quarter', values='value')
df_new2 = df_new.iloc[:,[0,8,11,12]]
Change_last_quarter = df_new2.iloc[:,3] - df_new2.iloc[:,2]
Change_12_months = df_new2.iloc[:,3] - df_new2.iloc[:,1]
Change_3y = df_new2.iloc[:,3] - df_new2.iloc[:,0]
df_new3 = pd.concat([Change_last_quarter, Change_12_months, Change_3y], axis=1, keys=['Change last quarter','Change last year','Change last 3 years'])
df_new3.to_csv('data_EN/SSB_vacancies_industry_change_rate.csv', index=True)
total = df_new2.iloc[:,3]
title_date = (total.name)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
date_string2 = title_date[-1:]
date_string3 = title_date[0:4]
date_string4 = 'Data for Q' + date_string2 + ' ' + date_string3 + " compared with: "
#Update DW
chartid = 'jj7hk'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)