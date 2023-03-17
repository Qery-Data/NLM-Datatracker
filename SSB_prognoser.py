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

#Prognose datoer
forecast_dates = {
    'SSB': '10.03.2023',
    'Norges Bank': '15.12.2022',
    'FIN': '05.03.2023',
    'NAV': '06.12.2022',
    'IMF': '12.10.2022',
    'OECD': '22.11.2022',
    'NHO': '07.03.2023',
    'LO': '22.09.2022',
    'Danske Bank': '05.01.2023',
    'DNB': '26.01.2023',
    'Handelsbanken': '25.01.2023',
    'Nordea': '25.01.2023',
    'SEB': '24.01.2023',
    'Swedbank': '25.10.2022'
    }

#Prognose Arbeidsledighet (AKU)
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
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025",
          "2026"
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
    'FIN': [3.5,3.6,pd.NA,pd.NA],
    'NAV': [3.7,3.9,pd.NA,pd.NA],
    'IMF': [3.8,3.7,pd.NA,pd.NA],
    'OECD': [3.6,3.7,pd.NA,pd.NA],
    'NHO': [3.7,3.8,3.7,pd.NA],
    'LO': [3.6,3.9,4.1,pd.NA],
    'DNB': [3.4,3.8,4.0,pd.NA],
    'SEB': [3.6,3.8,pd.NA,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2023','2024','2025','2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1).round(2)
years_na = ['2016','2017','2018','2019','2020','2021','2022']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.iloc[7:11, df_new2.columns.get_loc('Faktisk utvikling')] = df_new2.iloc[7:11, df_new2.columns.get_loc('Konsensus')].values
df_new2.to_csv('data/Prognoser_arbeidsledighet_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2016','2017','2018','2019','2020','2021','2022'})
df_new3.drop(columns=['Faktisk utvikling'], inplace=True)
df_new3 = df_new3.transpose()
df_new3['Dato'] = df_new3.index.map(forecast_dates)
df_new3.to_csv('data/Prognoser_arbeidsledighet_tabell.csv', index=True)

#Prognose Registrert ledighet (NAV)
df = {'Faktisk utvikling': [3.0,2.7,2.4,2.3,5.0,3.1,1.8,pd.NA,pd.NA,pd.NA,pd.NA]}
df_new = pd.DataFrame(df, index=['2016','2017','2018','2019','2020','2021','2022','2023','2024','2025','2026'])
forecasts = {
    'NAV': [1.9,2.1,pd.NA,pd.NA],
    'Norges Bank': [2.0,2.4,2.4,pd.NA],
    'FIN': [1.9,2.0,pd.NA,pd.NA],
    'NHO': [2.2,2.4,2.3,pd.NA],
    'DNB': [2.1,2.5,2.9,pd.NA],
    'Danske Bank': [2.2,2.4,pd.NA,pd.NA],
    'Handelsbanken': [2.1,2.3,2.3,pd.NA],
    'Nordea': [1.8,2.1,pd.NA,pd.NA],
    'Swedbank': [2.4,2.2,pd.NA,pd.NA]    
    }
df_forecast = pd.DataFrame(forecasts, index=['2023','2024','2025','2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1).round(2)
years_na = ['2016','2017','2018','2019','2020','2021','2022']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.iloc[7:11, df_new2.columns.get_loc('Faktisk utvikling')] = df_new2.iloc[7:11, df_new2.columns.get_loc('Konsensus')].values
df_new2.to_csv('data/Prognoser_registrert_ledighet_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2016','2017','2018','2019','2020','2021','2022'})
df_new3.drop(columns=['Faktisk utvikling'], inplace=True)
df_new3 = df_new3.transpose()
df_new3['Dato'] = df_new3.index.map(forecast_dates)
df_new3.to_csv('data/Prognoser_registrert_ledighet_tabell.csv', index=True)

#Prognose Sysselsatte personer
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
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025",
          "2026"
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
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'Norges Bank': [-0.4,-0.4,0.7,pd.NA],
    'FIN': [0.7,0.2,pd.NA,pd.NA],
    'NAV': [0.0,0.0,pd.NA,pd.NA],
    'NHO': [0.7,0.6,0.3,pd.NA],
    'LO':[0.0,0.0,-0.2,pd.NA],
    'DNB': [-0.2,0.2,0.2,pd.NA],
    'Danske Bank': [-0.2,0.1,pd.NA,pd.NA],
    'Handelsbanken': [-0.3,-0.2,pd.NA,pd.NA],
    'Swedbank': [-0.8,0.5,pd.NA,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2023','2024','2025','2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1).round(2)
years_na = ['2016','2017','2018','2019','2020','2021','2022']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.iloc[7:11, df_new2.columns.get_loc('Faktisk utvikling')] = df_new2.iloc[7:11, df_new2.columns.get_loc('Konsensus')].values
df_new2.to_csv('data/Prognoser_sysselsatte_personer_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2016','2017','2018','2019','2020','2021','2022'})
df_new3.drop(columns=['Faktisk utvikling'], inplace=True)
df_new3 = df_new3.transpose()
df_new3['Dato'] = df_new3.index.map(forecast_dates)
df_new3.to_csv('data/Prognoser_sysselsatte_personer_tabell.csv', index=True)

#Prognose Arbeidsstyrken
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
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025",
          "2026"
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
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'NAV': [0.5,0.5,pd.NA,pd.NA],
    }
df_forecast = pd.DataFrame(forecasts, index=['2023','2024','2025','2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1).round(2)
years_na = ['2016','2017','2018','2019','2020','2021','2022']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.iloc[7:11, df_new2.columns.get_loc('Faktisk utvikling')] = df_new2.iloc[7:11, df_new2.columns.get_loc('Konsensus')].values
df_new2.to_csv('data/Prognoser_arbeidsstyrken_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2016','2017','2018','2019','2020','2021','2022'})
df_new3.drop(columns=['Faktisk utvikling'], inplace=True)
df_new3 = df_new3.transpose()
df_new3['Dato'] = df_new3.index.map(forecast_dates)
df_new3.to_csv('data/Prognoser_arbeidsstyrken_tabell.csv', index=True)

#Prognose Yrkesandel
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
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025",
          "2026"
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
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'NAV': [72.5,72.3,pd.NA,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2023','2024','2025','2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1).round(2)
years_na = ['2016','2017','2018','2019','2020','2021','2022']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.iloc[7:11, df_new2.columns.get_loc('Faktisk utvikling')] = df_new2.iloc[7:11, df_new2.columns.get_loc('Konsensus')].values
df_new2.to_csv('data/Prognoser_yrkesandel_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2016','2017','2018','2019','2020','2021','2022'})
df_new3.drop(columns=['Faktisk utvikling'], inplace=True)
df_new3 = df_new3.transpose()
df_new3['Dato'] = df_new3.index.map(forecast_dates)
df_new3.to_csv('data/Prognoser_yrkesandel_tabell.csv', index=True)

#Prognose Utførte timeverk
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
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025",
          "2026"
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
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:6],'SSB'] = pd.NA
df_new.iloc[7:11, df_new.columns.get_loc('Faktisk utvikling')] = df_new.iloc[7:11, df_new.columns.get_loc('SSB')].values
df_new.to_csv('data/Prognoser_timeverk_konsensus_figur.csv', index=True)
df_new2 = df_new.drop(index={'2016','2017','2018','2019','2020','2021','2022'})
df_new2.drop(columns=['Faktisk utvikling'], inplace=True)
df_new2 = df_new2.transpose()
df_new2['Dato'] = df_new2.index.map(forecast_dates)
df_new2.to_csv('data/Prognoser_timeverk_tabell.csv', index=True)

#Prognose Årslønn
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
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025",
          "2026"
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
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'Norges Bank': [4.7,4.3,4.0,pd.NA],
    'Danske Bank': [4.3,3.8,pd.NA,pd.NA],
    'DNB': [4.8,4.5,4.0,pd.NA],
    'Handelsbanken': [4.6,4.0,pd.NA,pd.NA],
    'Nordea': [5.0,4.5,pd.NA,pd.NA],
    'SEB': [4.7,3.8,pd.NA,pd.NA],
    'Swedbank': [3.8,3.0,pd.NA,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2023','2024','2025','2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1).round(2)
years_na = ['2016','2017','2018','2019','2020','2021','2022']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.iloc[7:11, df_new2.columns.get_loc('Faktisk utvikling')] = df_new2.iloc[7:11, df_new2.columns.get_loc('Konsensus')].values
df_new2.to_csv('data/Prognoser_aarslonn_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2016','2017','2018','2019','2020','2021','2022'})
df_new3.drop(columns=['Faktisk utvikling'], inplace=True)
df_new3 = df_new3.transpose()
df_new3['Dato'] = df_new3.index.map(forecast_dates)
df_new3.to_csv('data/Prognoser_aarslonn_tabell.csv', index=True)

#Prognose Konsumprisindeksen
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
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025",
          "2026"
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
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'Norges Bank': [4.8,2.8,2.6,pd.NA],
    'IMF': [3.8,2.7,pd.NA,pd.NA],
    'OECD': [4.5,3.2,pd.NA,pd.NA],
    'NHO': [4.9,3.3,2.9,pd.NA],
    'LO':[3.5,1.9,2.1,pd.NA],
    'Danske Bank': [4.8,2.1,pd.NA,pd.NA],
    'DNB': [4.6,2.3,2.8,pd.NA],
    'Handelsbanken': [4.0,1.5,1.9,pd.NA],
    'Nordea': [4.3,3.0,pd.NA,pd.NA],
    'SEB': [5.4,3.0,pd.NA,pd.NA],
    'Swedbank': [4.6,1.7,pd.NA,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2023','2024','2025','2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1).round(2)
years_na = ['2016','2017','2018','2019','2020','2021','2022']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.iloc[7:11, df_new2.columns.get_loc('Faktisk utvikling')] = df_new2.iloc[7:11, df_new2.columns.get_loc('Konsensus')].values
df_new2.to_csv('data/Prognoser_kpi_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2016','2017','2018','2019','2020','2021','2022'})
df_new3.drop(columns=['Faktisk utvikling'], inplace=True)
df_new3 = df_new3.transpose()
df_new3['Dato'] = df_new3.index.map(forecast_dates)
df_new3.to_csv('data/Prognoser_kpi_tabell.csv', index=True)

#Prognose BNP Fastlands-Norge
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
          "2016",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",          
          "2022",
          "2023",
          "2024",
          "2025",
          "2026"
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
    'Norges Bank': [-0.2,0.2,1.4,pd.NA],
    'FIN': [0.9,1.4,pd.NA,pd.NA],
    'NAV': [1.2,1.5,pd.NA,pd.NA],
    'OECD': [0.7,1.3,pd.NA,pd.NA],
    'IMF': [2.6,2.2,pd.NA,pd.NA],
    'NHO': [1.3,1.4,1.4,pd.NA],
    'LO': [1.5,1.9,1.8,pd.NA],
    'Danske Bank': [0.6,1.5,pd.NA,pd.NA],
    'DNB': [0.5,1.2,1.2,pd.NA],
    'Handelsbanken': [-0.1,0.3,1.5,pd.NA],
    'Nordea': [1.0,1.0,pd.NA,pd.NA],
    'SEB': [-0.5,1.1,pd.NA,pd.NA],
    'Swedbank': [-0.5,1.4,pd.NA,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2023','2024','2025','2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1).round(2)
years_na = ['2016','2017','2018','2019','2020','2021','2022']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.iloc[7:11, df_new2.columns.get_loc('Faktisk utvikling')] = df_new2.iloc[7:11, df_new2.columns.get_loc('Konsensus')].values
df_new2.to_csv('data/Prognoser_bnp_fn_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2016','2017','2018','2019','2020','2021','2022'})
df_new3.drop(columns=['Faktisk utvikling'], inplace=True)
df_new3 = df_new3.transpose()
df_new3['Dato'] = df_new3.index.map(forecast_dates)
df_new3.to_csv('data/Prognoser_bnp_fn_tabell.csv', index=True)
