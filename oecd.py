from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import io
import pandas as pd
os.makedirs('data', exist_ok=True)
access_token = os.getenv('DW_TOKEN')

#Gjennomsnittlig årlig arbeidstimer d6F9Q (NO) and 2BNuF (EN)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/ANHRS/DNK+NOR+SWE+USA+OECD+EU27.TE.A/all?startTime=1970&pid=c0dcdd50-2d08-440b-94d7-8aa50471b7ff'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='TIME', columns='Country', values='Value')
df_new.to_csv('data/OECD_arbeidstid_aarligsnitt.csv', index=True)

# Produktivitet per time sammenligning sist år kCW5D (BNP) k3zon (BNI) (NO) + fszx7 (GDP) J4cFQ (GNI)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/PDB_LV/AUS+AUT+BEL+CAN+CHL+COL+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+OECD+NMEC+BRA+CHN+CRI+IND+IDN+RUS+ZAF+BRIICS.T_GDPHRS+T_GNIHRS.CPC/all?startTime=2022&endTime=2022'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Country', columns='Subject', values='Value')
parsed_date=(df.iloc[0,7])
parsed_date2=str(parsed_date)
df_new.to_csv('data/OECD_produktivitet_time.csv', index=True)
date_string = 'Bruttonasjonalprodukt (BNP) per utførte timeverk i USD*. Tall for ' + parsed_date2 + '.'
date_string2 = 'Bruttonasjonalinntekt (BNI) per utførte timeverk i USD*. Tall for ' + parsed_date2 + '.'
date_string_EN = 'GDP per hour worked in USD*. Data for ' + parsed_date2 + '.'
date_string2_EN = 'GNI per hour worked in USD*. Data for ' + parsed_date2 + '.'
#Update DW
chartid = 'kCW5D'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

chartid = 'k3zon'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string2}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Update DW
chartid = 'fszx7'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string_EN}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

chartid = 'J4cFQ'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string2_EN}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

# Produktivitet per time index 2000 N1JHC (NO) 8jVXu (EN)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/PDB_GR/DNK+FIN+DEU+NOR+SWE+EA19+OECD+USA.T_GDPHRS_V.2015Y/all?startTime=2000'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Time', columns='Country', values='Value')
df_index = df_new.div(df_new.iloc[0]).mul(100)
df_index.insert(8, 'Fastlands-Norge',[100,103,104.8,108.1,110.9,114.2,115.8,116.5,115,114.9,116.7,116.7,119.1,121.5,122.4,123.3,125,125.9,126.9,126.9,126.5,128.3,pd.NA], True)
df_index.to_csv('data/OECD_produktivitet_time_utvikling_2000.csv', index=True)

# Produktivitet per time index 2015 N1JHC (NO) and 1FF4k (EN)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/PDB_GR/DNK+FIN+DEU+NOR+SWE+EA19+OECD+USA.T_GDPHRS_V.2015Y/all?startTime=2015'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Time', columns='Country', values='Value')
df_new.insert(8, 'Fastlands-Norge',[123.3,125,125.9,126.9,126.9,126.5,128.3,pd.NA], True)
df_index = df_new.div(df_new.iloc[0]).mul(100)
df_index.to_csv('data/OECD_produktivitet_time_utvikling_2015.csv', index=True)

# Produktivitet per time vekst per tiår LIOyE
oecd_url='https://stats.oecd.org/SDMX-JSON/data/PDB_GR/DNK+FIN+FRA+DEU+ISL+IRL+ITA+JPN+KOR+NLD+NOR+PRT+ESP+SWE+GBR+USA+G7.T_GDPHRS_V.GRW/all?startTime=1970'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Country', columns='Time', values='Value')
df_new.to_csv('data/OECD_produktivitet_time_sammenligning_tiår.csv', index=True)

#Organisasjonsgrad i Norge og Norden bNB6q og hZL09 (NO) yfCjc og oRZAl (EN)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/TUD/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+OTO.A.PCT/all?startTime=2000'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Country', columns='Time', values='Value')
df_new.to_csv('data/OECD_organisasjonsgrad_utvikling.csv', index=True)

#Organisasjonsgrad rangering og sammenligning Wv6d0 og lxzk1 (NO) and eJb1j og 2RX2a (EN)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/TUD/AUT+BEL+CAN+DNK+EST+FIN+DEU+ISL+IRL+ITA+JPN+MEX+NLD+NOR+ESP+SWE+GBR+USA+OTO.A.PCT/all?startTime=2000'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Country', columns='Time', values='Value')
df_new.to_csv('data/OECD_organisasjonsgrad_rangering.csv', index=True)

#Arbeidsledighet MEI OECD wNXU5 (NO) 0R3hu (EN)
#oecd_url='https://stats.oecd.org/SDMX-JSON/data/MEI/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+OECD+ARG+BRA+BGR+CHN+CYP+IND+IDN+MLT+ROU+RUS+SAU+ZAF.LRHUTTTT.STSA.M/all?startTime=2012-01'
#result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
#df=pd.read_csv(io.StringIO(result.text))
#df_new = df.pivot(index='TIME', columns='Country', values='Value')
#df_new = df_new.round(decimals=1)
#df_new.to_csv('data/OECD_MEI_Unemployment.csv', index=True)

#Ungdomsledighet MEI OECD 5pqI6 (NO) + jjdYo (EN)
#oecd_url='https://stats.oecd.org/SDMX-JSON/data/MEI/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+OECD+ARG+BRA+BGR+CHN+CYP+IND+IDN+MLT+ROU+RUS+SAU+ZAF.LRHU24TT.STSA.M/all?startTime=2012-01'
#result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
#df=pd.read_csv(io.StringIO(result.text))
#df_new = df.pivot(index='TIME', columns='Country', values='Value')
#df_new = df_new.round(decimals=1)
#df_new.to_csv('data/OECD_MEI_Youth_Unemployment.csv', index=True)

#Sysselsettingsandel OECD CS8Rb (NO) VKfA9 (EN)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/MEI/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+OECD+ARG+BRA+BGR+CHN+CYP+IND+IDN+MLT+ROU+RUS+SAU+ZAF.LREM74TT.STSA.Q/all?startTime=2022-Q1'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Country', columns='TIME', values='Value')
df_new = df_new.round(decimals=1)
df_new.to_csv('data/OECD_Employment_Rate.csv', index=True)

#Sysselsettingsandel Kvinner OECD ZERuL (NO) SuY2u (EN)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/MEI/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+OECD+ARG+BRA+BGR+CHN+CYP+IND+IDN+MLT+ROU+RUS+SAU+ZAF.LREM74FE.STSA.Q/all?startTime=2022-Q1'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Country', columns='TIME', values='Value')
df_new = df_new.round(decimals=1)
df_new.to_csv('data/OECD_Employment_Rate_Women.csv', index=True)

#Sysselsettingsandel Menn OECD YpL1m (NO) Mqkeh (EN)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/MEI/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+OECD+ARG+BRA+BGR+CHN+CYP+IND+IDN+MLT+ROU+RUS+SAU+ZAF.LREM74MA.STSA.Q/all?startTime=2022-Q1'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Country', columns='TIME', values='Value')
df_new = df_new.round(decimals=1)
df_new.to_csv('data/OECD_Employment_Rate_Men.csv', index=True)

#Midlertidig ansatte OECD ohRTM (NO) vX91z (EN)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/TEMP_I/AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+EA19+EU27_2020+OECD+ARG+BRA+BGR+CHN+CYP+IND+IDN+MLT+ROU+SAU+ZAF.MW.900000.DE.PER_CENT_TEMP.A/all?startTime=2012'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Country', columns='TIME', values='Value')
df_new.to_csv('data/OECD_Temporary_Employment.csv', index=True)

#End

#Kollektiv forhandlingsrett
oecd_url='https://stats.oecd.org/SDMX-JSON/data/CBC/AUT+BEL+CAN+DNK+EST+FIN+DEU+ISL+IRL+ITA+JPN+MEX+NLD+NOR+ESP+SWE+GBR+USA+OTO.A.PCT/all?startTime=2000'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Country', columns='Year', values='Value')
df_new['last_value'] = df_new.ffill(axis=1).iloc[:, -1] 
df_new.to_csv('data/OECD_organisasjonsgrad_kollektiv_forhandlingsrett.csv', index=True)

#Kollektiv forhandlingsrett Skandinavia
oecd_url='https://stats.oecd.org/SDMX-JSON/data/CBC/DNK+NOR+SWE+OTO.A.PCT/all?startTime=2000'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='Country', columns='Year', values='Value')
df_new.to_csv('data/OECD_organisasjonsgrad_kollektiv_forhandlingsrett_norden.csv', index=True)