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

#Andel ledige stillinger QeY5e
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/jvs_q_nace2?s_adj=SA&lastTimePeriod=21&nace_r2=A-S&sizeclas=TOTAL&indic_em=JOBRATE&geo=FI&geo=NO&geo=SE&geo=NL')
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='time', columns='geo', values='value')
df_new.to_csv('data/Eurostat_ledige_stillinger_andel.csv', index=True)
antall = df_new.iloc[20,:]
tittel_dato = (antall.name)
oppdatert = dataset["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%d')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
date_string2 = tittel_dato[-1:]
date_string3 = tittel_dato[0:4]
date_string4 = 'Sist oppdatert med tall for ' + date_string2 + '.kvartal ' + date_string3
#Update DW QeY5e
url = "https://api.datawrapper.de/v3/charts/QeY5e/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/QeY5e/"
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel arbeidsledige wNXU5
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/une_rt_m?s_adj=SA&lastTimePeriod=62&age=TOTAL&unit=PC_ACT&sex=T')
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='time', columns='geo', values='value')
df_new.to_csv('data/Eurostat_arbeidsledighet.csv', index=True)
oppdatert = dataset["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%d')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
url = "https://api.datawrapper.de/v3/charts/wNXU5/"
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)