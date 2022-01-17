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

#Gjennomsnittlig årlig arbeidstimer d6F9Q
oecd_url='https://stats.oecd.org/SDMX-JSON/data/ANHRS/DNK+NOR+SWE+USA+OECD+EU27.TE.A/all?startTime=1970&pid=c0dcdd50-2d08-440b-94d7-8aa50471b7ff'
resultat = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(resultat.text))
df_new = df.pivot(index='TIME', columns='Country', values='Value')
df_new.to_csv('data/OECD_arbeidstid_aarligsnitt.csv', index=True)

# Produktivitet per time kCW5D
oecd_url='https://stats.oecd.org/SDMX-JSON/data/PDB_LV/AUS+AUT+BEL+CAN+CHL+COL+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+OECD+NMEC+BRA+CHN+CRI+IND+IDN+RUS+ZAF+BRIICS.T_GDPHRS+T_GNIHRS.CPC/all?startTime=2020'
resultat = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(resultat.text))
df_new = df.pivot(index='Country', columns='Subject', values='Value')
dato_oppdatert=(df.iloc[0,7])
dato_oppdatert2=str(dato_oppdatert)
df_new.to_csv('data/OECD_produktivitet_time.csv', index=True)
date_string = 'BNP per utførte timeverk i USD*. Tall for ' + dato_oppdatert2

#Update DW 
url = "https://api.datawrapper.de/v3/charts/kCW5D/"
payload = {"metadata": {"describe": {"intro": date_string}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
#END


