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
    'SSB': '09.06.2023',
    'Norges Bank': '22.06.2023',
    'FIN': '11.05.2023',
    'NAV': '21.06.2023',
    'IMF': '11.04.2023',
    'OECD': '07.06.2023',
    'LO': '10.05.2023',    
    'NHO': '13.06.2023',
    'Danske Bank': '20.06.2023',
    'DNB': '24.08.2023',
    'Handelsbanken': '24.05.2023',
    'Nordea': '09.05.2023',
    'SEB': '29.08.2023',
    }

# Function to check if forecast date is within the last 100 days
def within_last_100_days(date_str):
    forecast_date = datetime.strptime(date_str, '%d.%m.%Y')
    days_difference = (datetime.now() - forecast_date).days
    return days_difference <= 100

#Forecast Unemployment/Arbeidsledighet (AKU/LFS)
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
    'FIN': [3.6,3.7,pd.NA,pd.NA],
    'NAV': [3.7,3.8,pd.NA,pd.NA],
    'IMF': [3.5,3.7,pd.NA,pd.NA],
    'OECD': [3.7,3.8,pd.NA,pd.NA],
    'LO': [3.7,3.9,4.2,pd.NA],    
    'NHO': [3.7,3.8,3.6,pd.NA],
    'DNB': [3.7,3.9,3.9,3.9],
    'SEB': [3.5,3.9,3.7,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2023', '2024', '2025', '2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
forecast_columns = list(df_forecast.columns) + ['SSB']
recent_forecast_mask = [within_last_100_days(forecast_dates[col]) for col in forecast_columns]
recent_forecasts_df = pd.concat([df_forecast, df_new['SSB']], axis=1).loc[:, recent_forecast_mask]
mean_recent_forecasts = recent_forecasts_df.mean(axis=1).round(2)
df_new2['Konsensus'] = mean_recent_forecasts
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

#Forecast Registrered unemployment/registrert ledighet (NAV)
df = {'Faktisk utvikling': [3.0,2.7,2.4,2.3,5.0,3.1,1.8,pd.NA,pd.NA,pd.NA,pd.NA]}
df_new = pd.DataFrame(df, index=['2016','2017','2018','2019','2020','2021','2022','2023','2024','2025','2026'])
forecasts = {
    'NAV': [1.8,2.1,pd.NA,pd.NA],
    'Norges Bank': [1.9,2.3,2.4,2.4],
    'FIN': [1.8,2.0,pd.NA,pd.NA],
    'NHO': [2.1,2.2,2.1,pd.NA],
    'DNB': [1.9,2.5,2.7,2.8],
    'Danske Bank': [1.9,2.3,pd.NA,pd.NA],
    'Handelsbanken': [2.0,2.3,2.4,pd.NA],
    'Nordea': [1.9,2.5,pd.NA,pd.NA],
    }
df_forecast = pd.DataFrame(forecasts, index=['2023', '2024', '2025', '2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
forecast_columns = list(df_forecast.columns)
recent_forecast_mask = [within_last_100_days(forecast_dates[col]) for col in forecast_columns]
recent_forecasts_df = pd.concat([df_forecast], axis=1).loc[:, recent_forecast_mask]
mean_recent_forecasts = recent_forecasts_df.mean(axis=1).round(2)
df_new2['Konsensus'] = mean_recent_forecasts
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

#Forecast Employed persons/Sysselsatte personer
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
    'Norges Bank': [1.2,-0.2,0.2,0.6],
    'FIN': [0.8,0.2,pd.NA,pd.NA],
    'NAV': [0.6,0.5,pd.NA,pd.NA],
    'LO': [0.5,0.0,0.0,pd.NA],    
    'NHO': [1.6,0.4,0.2,pd.NA],
    'DNB': [0.2,0.1,0.3,0.4],
    'Danske Bank': [0.8,-0.1,pd.NA,pd.NA],
    'Handelsbanken': [0.8,0.0,0.5,pd.NA],
    }
df_forecast = pd.DataFrame(forecasts, index=['2023', '2024', '2025', '2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
forecast_columns = list(df_forecast.columns) + ['SSB']
recent_forecast_mask = [within_last_100_days(forecast_dates[col]) for col in forecast_columns]
recent_forecasts_df = pd.concat([df_forecast, df_new['SSB']], axis=1).loc[:, recent_forecast_mask]
mean_recent_forecasts = recent_forecasts_df.mean(axis=1).round(2)
df_new2['Konsensus'] = mean_recent_forecasts
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

#Forecast Labour Force/Arbeidsstyrken
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
    'NAV': [1.1,0.5,pd.NA,pd.NA],
    }
df_forecast = pd.DataFrame(forecasts, index=['2023', '2024', '2025', '2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
forecast_columns = list(df_forecast.columns) + ['SSB']
recent_forecast_mask = [within_last_100_days(forecast_dates[col]) for col in forecast_columns]
recent_forecasts_df = pd.concat([df_forecast, df_new['SSB']], axis=1).loc[:, recent_forecast_mask]
mean_recent_forecasts = recent_forecasts_df.mean(axis=1).round(2)
df_new2['Konsensus'] = mean_recent_forecasts
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

#Forecast Labour Force Participation Rate/Yrkesandel
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
    'NAV': [72.6,72.4,pd.NA,pd.NA]
    }
df_forecast = pd.DataFrame(forecasts, index=['2023', '2024', '2025', '2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
forecast_columns = list(df_forecast.columns) + ['SSB']
recent_forecast_mask = [within_last_100_days(forecast_dates[col]) for col in forecast_columns]
recent_forecasts_df = pd.concat([df_forecast, df_new['SSB']], axis=1).loc[:, recent_forecast_mask]
mean_recent_forecasts = recent_forecasts_df.mean(axis=1).round(2)
df_new2['Konsensus'] = mean_recent_forecasts
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

#Forecast Wages/Årslønn
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
    'Norges Bank': [5.5,4.7,4.3,3.7],
    'FIN': [5.25,pd.NA,pd.NA,pd.NA],    
    'Danske Bank': [5.3,4.2,pd.NA,pd.NA],
    'DNB': [5.5,5.2,4.3,4.0],
    'Handelsbanken': [5.4,4.3,3.3,pd.NA],
    'Nordea': [5.6,4.5,pd.NA,pd.NA],
    'SEB': [5.5,4.5,3.3,pd.NA],
    }
df_forecast = pd.DataFrame(forecasts, index=['2023', '2024', '2025', '2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
forecast_columns = list(df_forecast.columns) + ['SSB']
recent_forecast_mask = [within_last_100_days(forecast_dates[col]) for col in forecast_columns]
recent_forecasts_df = pd.concat([df_forecast, df_new['SSB']], axis=1).loc[:, recent_forecast_mask]
mean_recent_forecasts = recent_forecasts_df.mean(axis=1).round(2)
df_new2['Konsensus'] = mean_recent_forecasts
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

#Forecast CPI/Konsumprisindeksen
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
    'Norges Bank': [6.0,3.9,2.9,2.4],
    'FIN': [5.4,3.7,pd.NA,pd.NA],    
    'IMF': [4.9,2.8,pd.NA,pd.NA],
    'OECD': [5.4,3.5,pd.NA,pd.NA],
    'LO': [4.9,2.1,2.5,pd.NA],    
    'NHO': [4.9,4.0,2.9,pd.NA],
    'Danske Bank': [5.3,2.5,pd.NA,pd.NA],
    'DNB': [5.8,4.4,3.3,2.9],
    'Handelsbanken': [5.2,3.2,2.0,pd.NA],
    'Nordea': [5.3,3.0,pd.NA,pd.NA],
    'SEB': [5.9,4.0,2.6,pd.NA],
    }
df_forecast = pd.DataFrame(forecasts, index=['2023', '2024', '2025', '2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
forecast_columns = list(df_forecast.columns) + ['SSB']
recent_forecast_mask = [within_last_100_days(forecast_dates[col]) for col in forecast_columns]
recent_forecasts_df = pd.concat([df_forecast, df_new['SSB']], axis=1).loc[:, recent_forecast_mask]
mean_recent_forecasts = recent_forecasts_df.mean(axis=1).round(2)
df_new2['Konsensus'] = mean_recent_forecasts
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

#Forecast GDP Mainland-Norway/BNP Fastlands-Norge
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
    'Norges Bank': [1.2,0.2,0.9,1.4],
    'FIN': [1.0,1.0,pd.NA,pd.NA],
    'NAV': [1.1,1.6,pd.NA,pd.NA],
    'OECD': [1.2,1.3,pd.NA,pd.NA],
    'LO': [1.4,0.9,1.0,pd.NA],    
    'NHO': [1.4,1.2,1.5,pd.NA],
    'Danske Bank': [1.1,1.4,pd.NA,pd.NA],
    'DNB': [1.2,1.0,1.4,1.4],
    'Handelsbanken': [1.2,0.5,1.2,pd.NA],
    'Nordea': [0.5,0.5,pd.NA,pd.NA],
    'SEB': [1.4,0.6,1.8,pd.NA],
    }
df_forecast = pd.DataFrame(forecasts, index=['2023', '2024', '2025', '2026'])
df_new2 = pd.concat([df_new, df_forecast], axis=1)
forecast_columns = list(df_forecast.columns) + ['SSB']
recent_forecast_mask = [within_last_100_days(forecast_dates[col]) for col in forecast_columns]
recent_forecasts_df = pd.concat([df_forecast, df_new['SSB']], axis=1).loc[:, recent_forecast_mask]
mean_recent_forecasts = recent_forecasts_df.mean(axis=1).round(2)
df_new2['Konsensus'] = mean_recent_forecasts
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
