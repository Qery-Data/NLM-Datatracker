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

#Andel ledige stillinger QeY5e (NO) and ZwZQs (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/jvs_q_nace2?s_adj=SA&lastTimePeriod=21&nace_r2=B-S&sizeclas=TOTAL&indic_em=JOBRATE&geo=FI&geo=NO&geo=SE&geo=NL&geo=DE')
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='time', columns='geo', values='value')
df_new.to_csv('data/Eurostat_ledige_stillinger_andel.csv', index=True)
antall = df_new.iloc[20,:]
tittel_dato = (antall.name)
oppdatert = dataset["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%d')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
riktig_dato_EN = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y') + ' *Utvalg av NACE B-S for å kunne sammenligne tall på tvers av land.'
date_string2 = tittel_dato[-1:]
date_string3 = tittel_dato[0:4]
date_string4 = 'Sist oppdatert med tall for ' + date_string2 + '.kvartal ' + date_string3 + '.'
date_string5 = 'Last updated with data for Q' + date_string2 + ' ' + date_string3 + '.'
#Update DW QeY5e
chartid = 'QeY5e'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
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
#Update DW ZwZQs
chartid = 'ZwZQs'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato_EN}}}
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

#Andel arbeidsledige wNXU5 (NO) and 0R3hu (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/une_rt_m?s_adj=SA&lastTimePeriod=62&age=TOTAL&unit=PC_ACT&sex=T')
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='time', columns='geo', values='value')
df_new.to_csv('data/Eurostat_arbeidsledighet.csv', index=True)
oppdatert = dataset["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%d')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
riktig_dato_EN = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')

#Update DW
chartid = 'wNXU5'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
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
chartid = '0R3hu'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato_EN}}}
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

#Andel ungdomsarbeidsledige 5pqI6 (NO) and jjdYo (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/une_rt_m?s_adj=SA&lastTimePeriod=62&age=Y_LT25&unit=PC_ACT&sex=T')
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='time', columns='geo', values='value')
df_new.to_csv('data/Eurostat_arbeidsledighet_unge.csv', index=True)
oppdatert = dataset["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%d')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
riktig_dato_EN = 'Data last published: ' + oppdatert_dato.strftime ('%d/%m/%y')

#Update DW
chartid = '5pqI6'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
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
chartid = 'jjdYo'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
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

#Andel sysselsatte CS8Rb
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/lfsi_emp_q?indic_em=EMP_LFS&lastTimePeriod=61&s_adj=SA&sex=T&age=Y15-74&unit=PC_POP')
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='time', columns='geo', values='value')
df_new.to_csv('data/Eurostat_sysselsatte.csv', index=True)
oppdatert = dataset["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%d')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
date_string = 'I prosent av befolkningen mellom 15-74 år. Sesongjusterte tall.'
#Update DW
chartid = 'CS8Rb'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

#Andel sysselsatte sist kvartal UG10W
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/lfsi_emp_q?indic_em=EMP_LFS&lastTimePeriod=1&s_adj=SA&sex=T&age=Y15-74&unit=PC_POP&geo=AT&geo=BE&geo=CH&geo=DE&geo=DK&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=IE&geo=IS&geo=IT&geo=NL&geo=NO&geo=PL&geo=PT&geo=SE')
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='time', columns='geo', values='value')
df_new.to_csv('data/Eurostat_sysselsatte_andel_siste_kvartal.csv', index=True)
oppdatert = dataset["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%d')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato = df.iloc[0,6]
kvartal = dato[5]
aar = dato[0:4]
date_string = 'I prosent av befolkningen mellom 15-74 år. Sesongjusterte tall for ' + kvartal + '.kvartal ' + aar + '.'
#Update DW
chartid = 'UG10W'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

#Andel sysselsatte menn sist kvartal YpL1m
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/lfsi_emp_q?indic_em=EMP_LFS&lastTimePeriod=1&s_adj=SA&sex=M&age=Y15-74&unit=PC_POP&geo=AT&geo=BE&geo=CH&geo=DE&geo=DK&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=IE&geo=IS&geo=IT&geo=NL&geo=NO&&geo=PL&geo=PT&geo=SE')
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='time', columns='geo', values='value')
df_new.to_csv('data/Eurostat_sysselsatte__menn_andel_siste_kvartal.csv', index=True)
oppdatert = dataset["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%d')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato = df.iloc[0,6]
kvartal = dato[5]
aar = dato[0:4]
date_string = 'I prosent av befolkningen mellom 15-74 år. Sesongjusterte tall for ' + kvartal + '.kvartal ' + aar + '.'
#Update DW
chartid = 'YpL1m'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

#Andel sysselsatte kvinner sist kvartal ZERuL
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/lfsi_emp_q?indic_em=EMP_LFS&lastTimePeriod=1&s_adj=SA&sex=F&age=Y15-74&unit=PC_POP&geo=AT&geo=BE&geo=CH&geo=DE&geo=DK&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=IE&geo=IS&geo=IT&geo=NL&geo=NO&&geo=PL&geo=PT&geo=SE')
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='time', columns='geo', values='value')
df_new.to_csv('data/Eurostat_sysselsatte__kvinner_andel_siste_kvartal.csv', index=True)
oppdatert = dataset["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%d')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato = df.iloc[0,6]
kvartal = dato[5]
aar = dato[0:4]
date_string = 'I prosent av befolkningen mellom 15-74 år. Sesongjusterte tall for ' + kvartal + '.kvartal ' + aar + '.'
#Update DW
chartid = 'ZERuL'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

#Andel midlertidig ansatte siste kvartal ohRTM
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/lfsi_pt_q?wstatus=EMP_TEMP&lastTimePeriod=1&s_adj=NSA&sex=T&age=Y15-74&unit=PC_SAL&geo=AT&geo=BE&geo=DE&geo=DK&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=IE&geo=IS&geo=IT&geo=NL&geo=NO&&geo=PL&geo=PT&geo=SE')
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='time', columns='geo', values='value')
df_new.to_csv('data/Eurostat_sysselsatte__midlertidig_siste_kvartal.csv', index=True)
oppdatert = dataset["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%d')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato = df.iloc[0,6]
kvartal = dato[5]
aar = dato[0:4]
date_string = 'I prosent av sysselsatte mellom 15-74 år. Tall for ' + kvartal + '.kvartal ' + aar + '.'
#Update DW
chartid = 'ohRTM'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

#Andel deltid sist år Eurostat lmKlf
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/lfsa_eppga?lastTimePeriod=1&sex=F&sex=M&sex=T&age=Y15-74&geo=AT&geo=BE&geo=CH&geo=DE&geo=DK&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=IE&geo=IS&geo=IT&geo=NL&geo=NO&geo=PT&geo=SE')
type(dataset)
df = dataset.write('dataframe')
dato = df.iloc[0,4]
df_new = df.pivot(index='geo', columns='sex', values='value')
df_new.to_csv('data/Eurostat_arbeidstid_deltid_sist_kvartal.csv', index=True)
oppdatert = dataset["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%d')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
date_string = 'I prosent av sysselsatte mellom 15-74 år. Tall for ' + dato + '.'
#Update DW
chartid = 'lmKlf'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

#Arbeidstid per uke avtalt/vanlig NUF70
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/lfsq_ewhais?lastTimePeriod=1&sex=T&age=Y_GE15&worktime=TOTAL&wstatus=EMP&isco08=TOTAL')
type(dataset)
df = dataset.write('dataframe')
df=df.replace({'Czechia':'Czech Rep.','Germany (until 1990 former territory of the FRG)':'Germany'})
df.to_csv('data/Eurostat_arbeidstid_faktiskuke_siste_kvartal.csv', index=True)
df_new = df.pivot(index='geo', columns='time', values = 'value')
EU_snitt = str(df_new.at['European Union - 27 countries (from 2020)', '2021Q3'])
oppdatert = dataset["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%d')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y') + '.' + ' Gjennomsnitt for EU: ' + EU_snitt + '.'
dato = df.iloc[0,7]
kvartal = dato[5]
aar = dato[0:4]
date_string = 'Faktisk arbeidstid per uke i timer. Tall for ' + kvartal + '.kvartal ' + aar + '.'
#Update DW
chartid = 'NUF70'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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

#Arbeidstid per uke avtalt/vanlig heltid Av2Nk
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/lfsq_ewhais?lastTimePeriod=1&sex=T&age=Y_GE15&worktime=FT&wstatus=EMP&isco08=TOTAL')
type(dataset)
df = dataset.write('dataframe')
df=df.replace({'Czechia':'Czech Rep.','Germany (until 1990 former territory of the FRG)':'Germany'})
df.to_csv('data/Eurostat_arbeidstid_faktiskuke_heltid_siste_kvartal.csv', index=True)
df_new = df.pivot(index='geo', columns='time', values = 'value')
EU_snitt = str(df_new.at['European Union - 27 countries (from 2020)', '2021Q3'])
oppdatert = dataset["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%d')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y') + '.' + ' Gjennomsnitt for EU: ' + EU_snitt + '.'
dato = df.iloc[0,7]
kvartal = dato[5]
aar = dato[0:4]
date_string = 'Faktisk arbeidstid per uke i timer for heltidsansatte. Tall for ' + kvartal + '.kvartal ' + aar + '.'
#Update DW
chartid = 'Av2Nk'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": riktig_dato}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
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