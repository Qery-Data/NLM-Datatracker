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
resultat = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(resultat.text))
df_new = df.pivot(index='TIME', columns='Country', values='Value')
df_new.to_csv('data/OECD_arbeidstid_aarligsnitt.csv', index=True)
#Update DW
chartid = 'd6F9Q'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/publish/'
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.request("POST", url, headers=headers)
#Update DW
chartid = '2BNuF'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/publish/'
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.request("POST", url, headers=headers)

# Produktivitet per time sammenligning sist år kCW5D (BNP) k3zon (BNI) (NO) + fszx7 (GDP) J4cFQ (GNI)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/PDB_LV/AUS+AUT+BEL+CAN+CHL+COL+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+OECD+NMEC+BRA+CHN+CRI+IND+IDN+RUS+ZAF+BRIICS.T_GDPHRS+T_GNIHRS.CPC/all?startTime=2020'
resultat = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(resultat.text))
df_new = df.pivot(index='Country', columns='Subject', values='Value')
dato_oppdatert=(df.iloc[0,7])
dato_oppdatert2=str(dato_oppdatert)
df_new.to_csv('data/OECD_produktivitet_time.csv', index=True)
date_string = 'Bruttonasjonalprodukt (BNP) per utførte timeverk i USD*. Tall for ' + dato_oppdatert2 + '.'
date_string2 = 'Bruttonasjonalinntekt (BNI) per utførte timeverk i USD*. Tall for ' + dato_oppdatert2 + '.'
date_string_EN = 'GDP per hour worked in USD*. Tall for ' + dato_oppdatert2 + '.'
date_string2_EN = 'GNI per hour worked in USD*. Tall for ' + dato_oppdatert2 + '.'
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
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/publish/'
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.request("POST", url, headers=headers)

chartid = 'k3zon'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string2}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/publish/'
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.request("POST", url, headers=headers)
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
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/publish/'
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.request("POST", url, headers=headers)

chartid = 'J4cFQ'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string2_EN}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/publish/'
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.request("POST", url, headers=headers)

# Produktivitet per time index 2000 N1JHC (NO) 8jVXu (EN)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/PDB_GR/DNK+FIN+DEU+NOR+SWE+EA19+OECD+USA.T_GDPHRS_V.2015Y/all?startTime=2000'
resultat = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(resultat.text))
df_new = df.pivot(index='Time', columns='Country', values='Value')
df_index = df_new.div(df_new.iloc[0]).mul(100)
df_index.insert(8, 'Fastlands-Norge',[100,103,105.8,109.8,113.3,117.3,119.4,120.6,119.3,119,120.9,120.9,123.3,125.8,126.8,127.5,127.6,129.6,130.5,131.3,130.8], True)
df_index.to_csv('data/OECD_produktivitet_time_utvikling_2000.csv', index=True)

# Produktivitet per time index 2015 N1JHC (NO) and 1FF4k (EN)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/PDB_GR/DNK+FIN+DEU+NOR+SWE+EA19+OECD+USA.T_GDPHRS_V.2015Y/all?startTime=2015'
resultat = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(resultat.text))
df_new = df.pivot(index='Time', columns='Country', values='Value')
df_new.insert(8, 'Fastlands-Norge',[127.5,127.6,129.6,130.5,131.3,130.8], True)
df_index = df_new.div(df_new.iloc[0]).mul(100)
df_index.to_csv('data/OECD_produktivitet_time_utvikling_2015.csv', index=True)

# Produktivitet per time vekst per tiår LIOyE
oecd_url='https://stats.oecd.org/SDMX-JSON/data/PDB_GR/DNK+FIN+FRA+DEU+ISL+IRL+ITA+JPN+KOR+NLD+NOR+PRT+ESP+SWE+GBR+USA+G7.T_GDPHRS_V.GRW/all?startTime=1970'
resultat = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(resultat.text))
df_new = df.pivot(index='Country', columns='Time', values='Value')
df_new.to_csv('data/OECD_produktivitet_time_sammenligning_tiår.csv', index=True)


#Organisasjonsgrad i Norge og Norden bNB6q og hZL09 (NO) yfCjc og oRZAl (NO)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/TUD/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+OTO.A.PCT/all?startTime=2000'
resultat = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(resultat.text))
df_new = df.pivot(index='Country', columns='Time', values='Value')
df_new.to_csv('data/OECD_organisasjonsgrad_utvikling.csv', index=True)

#Organisasjonsgrad rangering og sammenligning Wv6d0 og lxzk1 (NO) and eJb1j og 2RX2a (EN)
oecd_url='https://stats.oecd.org/SDMX-JSON/data/TUD/AUT+BEL+CAN+DNK+EST+FIN+DEU+ISL+IRL+ITA+JPN+MEX+NLD+NOR+ESP+SWE+GBR+USA+OTO.A.PCT/all?startTime=2000'
resultat = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(resultat.text))
df_new = df.pivot(index='Country', columns='Time', values='Value')
df_new.to_csv('data/OECD_organisasjonsgrad_rangering.csv', index=True)


#End

#Kollektiv forhandlingsrett
oecd_url='https://stats.oecd.org/SDMX-JSON/data/CBC/AUT+BEL+CAN+DNK+EST+FIN+DEU+ISL+IRL+ITA+JPN+MEX+NLD+NOR+ESP+SWE+GBR+USA+OTO.A.PCT/all?startTime=2000'
resultat = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(resultat.text))
df_new = df.pivot(index='Country', columns='Year', values='Value')
df_new['last_value'] = df_new.ffill(axis=1).iloc[:, -1] 
df_new.to_csv('data/OECD_organisasjonsgrad_kollektiv_forhandlingsrett.csv', index=True)

#Kollektiv forhandlingsrett Skandinavia
oecd_url='https://stats.oecd.org/SDMX-JSON/data/CBC/DNK+NOR+SWE+OTO.A.PCT/all?startTime=2000'
resultat = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(resultat.text))
df_new = df.pivot(index='Country', columns='Year', values='Value')
df_new.to_csv('data/OECD_organisasjonsgrad_kollektiv_forhandlingsrett_norden.csv', index=True)

