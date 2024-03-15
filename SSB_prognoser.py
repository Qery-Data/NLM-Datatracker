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
    'SSB': '08.12.2023',
    'Norges Bank': '14.12.2023',
    'FIN': '06.10.2023',
    'NAV': '01.12.2023',
    'IMF': '10.10.2023',
    'OECD': '29.11.2023',
    'LO': '16.02.2024',    
    'NHO': '11.03.2024',
    'Danske Bank': '05.03.2024',
    'DNB': '25.01.2024',
    'Handelsbanken': '20.12.2023',
    'Nordea': '24.01.2024',
    'SEB': '23.01.2024',
    'SØA': '13.09.2023'
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
df = dataset.write('dataframe')
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'Arbeidsledighetsrate (nivå)': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'FIN': [3.5,3.7,3.7,pd.NA],
    'NAV': [3.6,3.7,3.8,pd.NA],
    'IMF': [3.6,3.8,pd.NA,pd.NA],
    'OECD': [3.6,3.8,3.8  ,pd.NA],
    'LO': [3.6,3.9,4.2,pd.NA],    
    'NHO': [3.6,3.9,4.0,3.8],
    'DNB': [3.6,4.0,4.1,4.1],
    'SEB': [3.6,3.9,3.7,pd.NA],
    'SØA': [3.6,4.1,4.2,pd.NA]
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
    'NAV': [1.8,2.1,2.3,pd.NA],
    'Norges Bank': [1.8,2.1,2.3,2.3],
    'FIN': [1.8,2.0,2.1,pd.NA],
    'NHO': [1.8,2.5,2.5,2.4],
    'DNB': [1.8,2.4,2.8,2.8],
    'Danske Bank': [1.8,2.3,2.5,pd.NA],
    'Handelsbanken': [1.8,2.2,2.3,pd.NA],
    'Nordea': [1.8,2.1,2.1,pd.NA],
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
df = dataset.write('dataframe')
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'Sysselsatte personer': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'Norges Bank': [1.3,-0.1,0.4,0.7],
    'FIN': [1.3,0.1,0.5,pd.NA],
    'NAV': [1.0,0.5,0.6,pd.NA],
    'LO': [1.3,0.0,0.0,pd.NA],    
    'NHO': [1.3,0.3,0.4,1.0],
    'DNB': [1.3,0.4,0.5,0.7],
    'Danske Bank': [1.3,-0.1,0.4,pd.NA],
    'Handelsbanken': [1.3,-0.1,0.4,pd.NA],
    'SØA': [1.2,-0.4,-0.7,0.2]
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
df = dataset.write('dataframe')
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'Arbeidsstyrke': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'NAV': [1.4,0.7,0.6,pd.NA]
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
df = dataset.write('dataframe')
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'Yrkesandel (nivå)': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'NAV': [72.9,73.0,73.1,pd.NA]
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
df = dataset.write('dataframe')
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'Årslønn': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'Norges Bank': [5.5,5.0,4.3,3.7],
    'FIN': [5.5,4.9,pd.NA,pd.NA],    
    'Danske Bank': [5.3,4.7,3.5,pd.NA],
    'DNB': [5.4,5.1,4.2,4.0],
    'Handelsbanken': [5.5,5.0,4.2,pd.NA],
    'Nordea': [5.6,4.8,4.0,pd.NA],
    'SEB': [5.5,4.9,4.0,pd.NA],
    'SØA': [5.6,4.8,4.0,4.0]
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
df = dataset.write('dataframe')
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'Konsumprisindeksen (KPI)': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'Norges Bank': [5.5,4.4,2.8,2.5],
    'FIN': [6.0,3.8,2.5,pd.NA],    
    'IMF': [5.8,3.7,pd.NA,pd.NA],
    'OECD': [5.5,3.9,3.2,pd.NA],
    'LO': [5.5,4.1,2.3,pd.NA],    
    'NHO': [5.5,4.1,3.3,2.4],
    'Danske Bank': [5.5,3.8,2.0,pd.NA],
    'DNB': [5.5,4.2,3.3,2.9],
    'Handelsbanken': [5.5,4.2,2.8,pd.NA],
    'Nordea': [5.5,3.8,2.8,pd.NA],
    'SEB': [5.5,3.9,2.8,pd.NA],
    'SØA': [5.8,4.4,3.2,2.9]
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
df = dataset.write('dataframe')
df_new = df.pivot(index='år', columns='statistikkvariabel', values='value')
df_new = df_new.rename(columns={'BNP Fastlands-Norge': 'Faktisk utvikling'})
df_new ['SSB'] = df_new['Faktisk utvikling']
df_new.loc[df_new.index[7:11],'Faktisk utvikling'] = pd.NA
df_new.loc[df_new.index[0:7],'SSB'] = pd.NA
forecasts = {
    'Norges Bank': [1.0,0.1,1.2,1.5],
    'FIN': [0.6,0.8,1.9,pd.NA],
    'NAV': [0.7,1.1,1.8,pd.NA],
    'OECD': [1.1,0.5,1.3,pd.NA],
    'LO': [0.7,1.0,1.2,pd.NA],    
    'NHO': [0.7,0.9,1.1,1.4],
    'Danske Bank': [1.1,1.1,2.1,pd.NA],
    'DNB': [1.0,0.6,1.4,1.8],
    'Handelsbanken': [1.2,0.4,1.2,pd.NA],
    'Nordea': [1.1,1.0,1.5,pd.NA],
    'SEB': [0.9,0.5,1.4,pd.NA],
    'SØA': [0.9,0.3,1.4,1.8]
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
