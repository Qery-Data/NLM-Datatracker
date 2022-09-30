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
    'SSB': '09.09.2022',
    'Norges Bank': '22.09.2022',
    'FIN': '12.05.2022',
    'NAV': '15.06.2022',
    'IMF': '19.04.2022',
    'OECD': '08.06.2022',
    'NHO': '15.06.2022',
    'LO': '22.09.2022',
    'Danske Bank': '21.06.2022',
    'DNB': '25.08.2022',
    'Handelsbanken': '21.09.2022',
    'Nordea': '07.09.2022',
    'SEB': '30.08.2022',
    'Swedbank': '24.08.2022'
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
    'IMF': [3.9,3.8,3.7,3.7],
    'OECD': [2.8,2.8,pd.NA,pd.NA],
    'NHO': [3.1,3.1,3.4,pd.NA],
    'LO': [3.3,3.6,3.9,4.1],
    'DNB': [3.2,3.5,3.9,4.2],
    'SEB': [3.1,3.4,3.8,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2022','2023','2024','2025'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1)
years_na = ['2015','2016','2017','2018','2019','2020']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.to_csv('data/Prognoser_arbeidsledighet_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2015','2016','2017','2018','2019','2020','2021'})
df_new3.drop(columns=['Faktisk utvikling'], inplace=True)
df_new3 = df_new3.transpose()
df_new3['Dato'] = df_new3.index.map(forecast_dates)
df_new3.to_csv('data/Prognoser_arbeidsledighet_tabell.csv', index=True)

#Prognose Registrert ledighet (NAV)
df = {'Faktisk utvikling': [3.0,3.0,2.7,2.4,2.3,5.0,3.1,pd.NA,pd.NA,pd.NA,pd.NA]}
df_new = pd.DataFrame(df, index=['2015','2016','2017','2018','2019','2020','2021','2022','2023','2024','2025'])
forecasts = {
    'NAV': [1.9,2.0,pd.NA,pd.NA],
    'Norges Bank': [1.8,2.0,2.3,2.3],
    'FIN': [1.8,1.7,pd.NA,pd.NA],
    'NHO': [1.9,1.7,2.0,pd.NA],
    'DNB': [1.8,2.1,2.6,3.0],
    'Danske Bank': [1.8,2.1,pd.NA,pd.NA],
    'Handelsbanken': [1.8,2.1,2.3,pd.NA],
    'Nordea': [1.8,1.8,2.1,pd.NA],
    'Swedbank': [1.8,2.2,2.1,pd.NA]    
    }
df_forecast = pd.DataFrame(forecasts, index=['2022','2023','2024','2025'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1)
years_na = ['2015','2016','2017','2018','2019','2020']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.to_csv('data/Prognoser_registrert_ledighet_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2015','2016','2017','2018','2019','2020','2021'})
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
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'Norges Bank': [3.5,-0.4,-0.1,0.5],
    'FIN': [3.0,0.8,pd.NA,pd.NA],
    'NAV': [3.1,0.9,pd.NA,pd.NA],
    'OECD': [3.0,0.6,pd.NA,pd.NA],
    'NHO': [2.5,1.1,0.9,pd.NA],
    'LO':[3.3,0.0,0.0,-0.2],
    'DNB': [2.7,0.1,0.1,0.0],
    'Danske Bank': [2.7,0.7,pd.NA,pd.NA],
    'Swedbank': [2.7,0.2,0.4,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2022','2023','2024','2025'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1)
years_na = ['2015','2016','2017','2018','2019','2020']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.to_csv('data/Prognoser_sysselsatte_personer_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2015','2016','2017','2018','2019','2020','2021'})
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
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'NAV': [1.8,1.0,pd.NA,pd.NA],
    }
df_forecast = pd.DataFrame(forecasts, index=['2022','2023','2024','2025'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1)
years_na = ['2015','2016','2017','2018','2019','2020']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.to_csv('data/Prognoser_arbeidsstyrken_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2015','2016','2017','2018','2019','2020','2021'})
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
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'NAV': [72.5,72.8,pd.NA,pd.NA],
    'Swedbank': [70.5,70.0,70.0,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2022','2023','2024','2025'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1)
years_na = ['2015','2016','2017','2018','2019','2020']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.to_csv('data/Prognoser_yrkesandel_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2015','2016','2017','2018','2019','2020','2021'})
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
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:6],'SSB'] = pd.NA
df_new.to_csv('data/Prognoser_timeverk_konsensus_figur.csv', index=True)
df_new2 = df_new.drop(index={'2015','2016','2017','2018','2019','2020','2021'})
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
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'Norges Bank': [4.0,4.6,4.5,4.2],
    'Danske Bank': [3.8,3.7,pd.NA,pd.NA],
    'DNB': [4.0,4.8,4.5,4.0],
    'Handelsbanken': [4.0,4.5,3.8,pd.NA],
    'Nordea': [4.5,5.0,4.0,pd.NA],
    'SEB': [4.1,4.5,3.6,pd.NA],
    'Swedbank': [4.1,3.5,3.0,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2022','2023','2024','2025'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1)
years_na = ['2015','2016','2017','2018','2019','2020']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.to_csv('data/Prognoser_aarslonn_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2015','2016','2017','2018','2019','2020','2021'})
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
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'Norges Bank': [5.4,4.5,3.1,2.5],
    'FIN': [3.5,3.4,pd.NA,pd.NA],
    'IMF': [3.5,1.8,2.1,2.0],
    'OECD': [4.6,3.3,pd.NA,pd.NA],
    'NHO': [3.3,2.8,2.2,pd.NA],
    'LO':[5.5,3.5,1.9,2.1],
    'Danske Bank': [4.6,2.0,pd.NA,pd.NA],
    'DNB': [5.3,4.0,2.7,2.7],
    'Handelsbanken': [5.5,3.7,2.0,pd.NA],
    'Nordea': [5.5,3.8,2.5,pd.NA],
    'SEB': [5.7,4.0,2.0,pd.NA],
    'Swedbank': [5.1,3.1,1.9,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2022','2023','2024','2025'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1)
years_na = ['2015','2016','2017','2018','2019','2020']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.to_csv('data/Prognoser_kpi_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2015','2016','2017','2018','2019','2020','2021'})
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
    'Norges Bank': [2.8,-0.3,0.4,1.1],
    'FIN': [3.6,2.3,pd.NA,pd.NA],
    'NAV': [3.5,2.6,pd.NA,pd.NA],
    'OECD': [3.5,1.7,pd.NA,pd.NA],
    'IMF': [4,2.6,2.2,1.6],
    'NHO': [3.5,2.2,1.7,pd.NA],
    'LO': [3.3,1.5,1.9,1.8],
    'Danske Bank': [3.6,1.6,pd.NA,pd.NA],
    'DNB': [3.3,0.8,1.2,1.1],
    'Handelsbanken': [2.9,0.7,1.3,pd.NA],
    'Nordea': [3.2,1.0,1.3,pd.NA],
    'SEB': [3.2,0.8,1.1,pd.NA],
    'Swedbank': [3.2,1.0,1.4,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2022','2023','2024','2025'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
df_new2['Konsensus'] = df_new2.mean(axis=1)
years_na = ['2015','2016','2017','2018','2019','2020']
insert_na = [pd.NA,pd.NA,pd.NA,pd.NA,pd.NA,pd.NA]
df_new2.loc[years_na, 'Konsensus'] = insert_na
df_new2.to_csv('data/Prognoser_bnp_fn_konsensus_figur.csv', index=True)
df_new3 = df_new2.drop(index={'2015','2016','2017','2018','2019','2020','2021'})
df_new3.drop(columns=['Faktisk utvikling'], inplace=True)
df_new3 = df_new3.transpose()
df_new3['Dato'] = df_new3.index.map(forecast_dates)
df_new3.to_csv('data/Prognoser_bnp_fn_tabell.csv', index=True)