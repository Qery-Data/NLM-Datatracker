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

#Forecast dates
forecast_dates = {
    'SSB': '10.06.2022',
    'Norges Bank': '23.06.2022',
    'FIN': '12.05.2022',
    'NAV': '15.06.2022',
    'OECD': '08.06.2022',
    'IMF': '19.04.2022',
    'NHO': '15.06.2022',
    'Danske Bank': '21.06.2022',
    'DNB': '28.04.2022',
    'Handelsbanken': '18.05.2022',
    'Nordea': '11.05.2022',
    'SEB': '10.05.2022',
    'Swedbank': '06.04.2022'
    }

#Prognoser Arbeidsledighet
ssburl = 'https://data.ssb.no/api/v0/no/table/12880/'
query = {
  "query": [
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "ArbLedighet"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2015",
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025"
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
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'Arbeidsledighetsrate (nivå)': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'FIN': [3.3,3.2,pd.NA,pd.NA],
    'NAV': [3.2,3.3,pd.NA,pd.NA],
    'OECD': [2.8,2.8,pd.NA,pd.NA],
    'IMF': [3.9,3.8,3.7,3.7],
    'NHO': [3.1,3.1,3.4,pd.NA],
    'DNB': [2.9,3,3.3,3.8],
    'SEB': [3.1,3.2,pd.NA,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2022','2023','2024','2025'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1)
years_na = ['2015','2016','2017','2018','2019','2020']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years, 'Konsensus'] = insert_na
df_new2.to_csv('data/Prognoser_arbeidsledighet_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2015','2016','2017','2018','2019','2020','2021'})
df_new3.drop(columns=['Faktisk utvikling'], inplace=True)
df_new3 = df_new3.transpose()
df_new3['Dato'] = df_new3.index.map(forecast_dates)
df_new3.to_csv('data/Prognoser_arbeidsledighet_tabell.csv', index=True)

#Prognoser Sysselatte personer smBL7 (no)
ssburl = 'https://data.ssb.no/api/v0/no/table/12880/'
query = {
  "query": [
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Sysselsatte"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2015",
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025"
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
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'Sysselsatte personer': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.to_csv('data/SSB_prognoser_sysselsattepersoner.csv', index=True)

#Prognoser Arbeidsstyrken aFpl4 (no)
ssburl = 'https://data.ssb.no/api/v0/no/table/12880/'
query = {
  "query": [
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "ArbStyrke"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2015",
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025"
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
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'Arbeidsstyrke': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.to_csv('data/SSB_prognoser_arbeidsstyrke.csv', index=True)

#Prognoser Yrkesandel 2OAZc (no)
ssburl = 'https://data.ssb.no/api/v0/no/table/12880/'
query = {
  "query": [
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Yrkesandel"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2015",
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025"
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
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'Yrkesandel (nivå)': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.to_csv('data/SSB_prognoser_yrkesandel.csv', index=True)

#Prognoser Utførte timeverk agoxS (no)
ssburl = 'https://data.ssb.no/api/v0/no/table/12880/'
query = {
  "query": [
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "TimeverkFastland"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2015",
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025"
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
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'Utførte timeverk i Fastlands-Norge': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.to_csv('data/SSB_prognoser_timeverk.csv', index=True)

#Prognoser Årslønn TZZSL (no)
ssburl = 'https://data.ssb.no/api/v0/no/table/12880/'
query = {
  "query": [
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Aarslonn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2015",
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025"
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
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'Årslønn': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.to_csv('data/SSB_prognoser_aarslonn.csv', index=True)

#Prognoser Konsumprisindeksen sNzun (no)
ssburl = 'https://data.ssb.no/api/v0/no/table/12880/'
query = {
  "query": [
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "KPI"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2015",
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025"
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
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'Konsumprisindeksen (KPI)': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.to_csv('data/SSB_prognoser_kpi.csv', index=True)

#BNP Fastlands-Norge
ssburl = 'https://data.ssb.no/api/v0/no/table/12880/'
query = {
  "query": [
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "BNPFastland"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2015",
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025"
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
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'BNP Fastlands-Norge': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'Norges Bank': [3.5,1.1,0.9,1.0],
    'FIN': [3.6,2.3,pd.NA,pd.NA],
    'NAV': [3.5,2.6,pd.NA,pd.NA],
    'OECD': [3.5,1.7,pd.NA,pd.NA],
    'IMF': [3.5,1.8,1.8,1.8],
    'NHO': [3.5,2.2,1.7,pd.NA],
    'Danske Bank': [3.6,1.6,pd.NA,pd.NA],
    'DNB': [3.8,1.5,1.2,1.1],
    'Handelsbanken': [3.6,1.9,1,pd.NA],
    'Nordea': [3.5,2,pd.NA,pd.NA],
    'SEB': [3.7,1.5,pd.NA,pd.NA],
    'Swedbank': [3.2,1.6,pd.NA,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2022','2023','2024','2025'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1)
years_na = ['2015','2016','2017','2018','2019','2020']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years, 'Konsensus'] = insert_na
df_new2.to_csv('data/Prognoser_bnp_fn_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2015','2016','2017','2018','2019','2020','2021'})
df_new3.drop(columns=['Faktisk utvikling'], inplace=True)
df_new3 = df_new3.transpose()
forecast_dates = {
    'SSB': '10.06.2022',
    'Norges Bank': '23.06.2022',
    'FIN': '12.05.2022',
    'NAV': '15.06.2022',
    'OECD': '08.06.2022',
    'IMF': '19.04.2022',
    'NHO': '15.06.2022',
    'Danske Bank': '21.06.2022',
    'DNB': '28.04.2022',
    'Handelsbanken': '18.05.2022',
    'Nordea': '11.05.2022',
    'SEB': '10.05.2022',
    'Swedbank': '06.04.2022'
    }
df_new3['Dato'] = df_new3.index.map(forecast_dates)
df_new3.to_csv('data/Prognoser_bnp_fn_tabell.csv', index=True)