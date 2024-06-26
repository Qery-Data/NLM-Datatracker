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

#Andel ledige stillinger QeY5e (NO) + Vacancy rate ZwZQs (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/jvs_q_nace2?s_adj=SA&lastTimePeriod=21&nace_r2=B-S&sizeclas=TOTAL&indic_em=JOBRATE&geo=FI&geo=NO&geo=SE&geo=NL&geo=DE&geo=EU27_2020')
df = dataset.write('dataframe')
df_new = df.pivot(index='Time', columns='Geopolitical entity (reporting)', values='value')
df_new.to_csv('data/Eurostat_ledige_stillinger_andel.csv', index=True)
total = df_new.iloc[20,:]
tittel_dato = (total.name)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
chart_date_EN = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
date_string2 = tittel_dato[-1:]
date_string3 = tittel_dato[0:4]
date_string4 = 'Sist raw_date med tall for ' + date_string2 + '.quarter ' + date_string3 + '.'
date_string5 = 'Last updated with data for Q' + date_string2 + ' ' + date_string3 + '.'
#Update DW
# chartid = 'QeY5e'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# chartid = 'ZwZQs'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)


#Andel arbeidsledige wNXU5 (NO) + Unemployment rate 0R3hu (EN)
dataset_tn = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/une_rt_m?s_adj=TC&lastTimePeriod=62&age=TOTAL&unit=PC_ACT&sex=T&geo=DE&geo=DK&geo=EU27_2020&geo=NO&geo=SE')
df_tn = dataset_tn.write('dataframe')
df_new_tn = df_tn.pivot(index='Time', columns='Geopolitical entity (reporting)', values='value')
df_new_tn.rename(columns={'Germany (until 1990 former territory of the FRG)': 'Germany', 'European Union - 27 countries (from 2020)': 'EU'},inplace=True)
df_new_tn.to_csv('data/Eurostat_arbeidsledighet.csv', index=True)
raw_date = dataset_tn["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
chart_date_EN = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
# chartid = '0R3hu'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW
# chartid = 'wNXU5'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel ungdomsarbeidsledige 5pqI6 (NO) + Youth unemployment rate jjdYo (EN)
dataset_tn = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/une_rt_m?s_adj=TC&lastTimePeriod=62&age=Y_LT25&unit=PC_ACT&sex=T&geo=DE&geo=DK&geo=EU27_2020&geo=SE&geo=NO')
df_tn = dataset_tn.write('dataframe')
df_new_tn = df_tn.pivot(index='Time', columns='Geopolitical entity (reporting)', values='value')
df_new_tn.rename(columns={'Germany (until 1990 former territory of the FRG)': 'Germany', 'European Union - 27 countries (from 2020)': 'EU'},inplace=True)
df_new_tn.to_csv('data/Eurostat_arbeidsledighet_unge.csv', index=True)
raw_date = dataset_tn["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
chart_date_EN = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
# chartid = 'jjdYo'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW
# chartid = '5pqI6'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel sysselsatte CS8Rb (NO) + Employment rate VKfA9 (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/lfsi_emp_q?indic_em=EMP_LFS&lastTimePeriod=61&s_adj=SA&sex=T&age=Y15-74&unit=PC_POP')
df = dataset.write('dataframe')
df_new = df.pivot(index='Time', columns='Geopolitical entity (reporting)', values='value')
df_new.to_csv('data/Eurostat_sysselsatte.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
chart_date_EN = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
date_string = 'I prosent av befolkningen mellom 15-74 år. Sesongjusterte tall.'
date_string_EN = 'As % of the population 15-74 years. Seasonally adjusted.'

#Update DW
# chartid = 'VKfA9'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW
# chartid = 'CS8Rb'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel sysselsatte sist quarter UG10W (NO) + Employment rate last quarter weNQJ (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/lfsi_emp_q?indic_em=EMP_LFS&lastTimePeriod=2&s_adj=SA&sex=T&age=Y15-74&unit=PC_POP&geo=AT&geo=BE&geo=CH&geo=DE&geo=DK&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=IE&geo=IS&geo=IT&geo=NL&geo=NO&geo=PL&geo=PT&geo=SE&geo=EE&geo=LU')
df = dataset.write('dataframe')
df_new = df.pivot(index='Time', columns='Geopolitical entity (reporting)', values='value')
df_new.to_csv('data/Eurostat_sysselsatte_andel_siste_kvartal.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
chart_date_EN = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
# chartid = 'weNQJ'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW
# chartid = 'UG10W'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel sysselsatte menn sist quarter YpL1m (NO) + Employment share men last quarter Mqkeh (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/lfsi_emp_q?indic_em=EMP_LFS&lastTimePeriod=2&s_adj=SA&sex=M&age=Y15-74&unit=PC_POP&geo=AT&geo=BE&geo=CH&geo=DE&geo=DK&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=IE&geo=IS&geo=IT&geo=NL&geo=NO&geo=PL&geo=PT&geo=SE&geo=EE&geo=LU')
df = dataset.write('dataframe')
df_new = df.pivot(index='Time', columns='Geopolitical entity (reporting)', values='value')
df_new.to_csv('data/Eurostat_sysselsatte__menn_andel_siste_kvartal.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
# chartid = 'Mqkeh'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW
# chartid = 'YpL1m'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel sysselsatte kvinner sist quarter ZERuL (NO) + Employment share women last quarter SuY2u (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/lfsi_emp_q?indic_em=EMP_LFS&lastTimePeriod=2&s_adj=SA&sex=F&age=Y15-74&unit=PC_POP&geo=AT&geo=BE&geo=CH&geo=DE&geo=DK&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=IE&geo=IS&geo=IT&geo=NL&geo=NO&geo=PL&geo=PT&geo=SE&geo=EE&geo=LU')
df = dataset.write('dataframe')
df_new = df.pivot(index='Time', columns='Geopolitical entity (reporting)', values='value')
df_new.to_csv('data/Eurostat_sysselsatte__kvinner_andel_siste_kvartal.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
chart_date_EN = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
# chartid = 'SuY2u'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW
# chartid = 'ZERuL'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel midlertidig ansatte siste quarter ohRTM (NO) + Temporary employment rate vX91z (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/lfsi_pt_q?wstatus=EMP_TEMP&lastTimePeriod=2&s_adj=NSA&sex=T&age=Y15-74&unit=PC_SAL&geo=AT&geo=BE&geo=CH&geo=DE&geo=DK&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=IE&geo=IS&geo=IT&geo=NL&geo=NO&geo=PL&geo=PT&geo=SE&geo=EE&geo=LU')
df = dataset.write('dataframe')
df_new = df.pivot(index='Time', columns='Geopolitical entity (reporting)', values='value')
df_new.to_csv('data/Eurostat_sysselsatte__midlertidig_siste_kvartal.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
chart_date_EN = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW
# chartid = 'vX91z'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW
# chartid = 'ohRTM'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel deltid sist år Eurostat lmKlf (NO) + Part time share last year cR3Tp (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/lfsa_eppga?lastTimePeriod=1&sex=F&sex=M&sex=T&age=Y15-74&geo=AT&geo=BE&geo=CH&geo=DE&geo=DK&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=IE&geo=IS&geo=IT&geo=NL&geo=NO&geo=PT&geo=SE')
df = dataset.write('dataframe')
date = df.iloc[0,5]
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Sex', values='value')
df_new.index.name = 'geo'
df_new.to_csv('data/Eurostat_arbeidstid_deltid_sist_kvartal.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
chart_date_EN = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
date_string = 'I prosent av sysselsatte mellom 15-74 år. Tall for ' + date + '.'
date_string_EN = 'As share of employed persons 15-74 years. Data for ' + date + '.'
# #Update DW
# chartid = 'lmKlf'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"describe": {"intro": date_string}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Update DW
# chartid = 'cR3Tp'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"describe": {"intro": date_string_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Arbeidstid per uke avtalt/vanlig NUF70 (NO) + Wokrking time per week actual xn9f1 (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/lfsq_ewhais?sex=T&age=Y_GE15&worktime=TOTAL&wstatus=EMP&isco08=TOTAL&time=2023-Q4')
df = dataset.write('dataframe')
df=df.replace({'Czechia':'Czech Rep.','Germany (until 1990 former territory of the FRG)':'Germany', 'Türkiye':'Turkey'})
df.to_csv('data/Eurostat_arbeidstid_faktiskuke_siste_kvartal.csv', index=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values = 'value')
df_new.index.name = 'geo'
EU_snitt = str(df_new.loc["European Union - 27 countries (from 2020)", "2023-Q4"])
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y') + '.' + ' Gjennomsnitt for EU: ' + EU_snitt + '.'
chart_date_EN = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y') + '.' + ' EU average: ' + EU_snitt + '.'
date = df.iloc[0,8]
quarter = date[6]
year = date[0:4]
date_string = 'Faktisk arbeidstid per uke i timer. Tall for ' + quarter + '. kvartal ' + year + '.'
date_string_EN = 'Average number of actual weekly hours of work. Data for Q' + quarter + ' ' + year + '.'
# #Update DW
# chartid = 'NUF70'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"describe": {"intro": date_string}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW
# chartid = 'xn9f1'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"describe": {"intro": date_string_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Arbeidstid per uke avtalt/vanlig heltid Av2Nk (NO) + Working time per week actual WD0Uz (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/lfsq_ewhais?sex=T&age=Y_GE15&worktime=FT&wstatus=EMP&isco08=TOTAL&time=2023-Q4')
df = dataset.write('dataframe')
df=df.replace({'Czechia':'Czech Rep.','Germany (until 1990 former territory of the FRG)':'Germany', 'Türkiye':'Turkey'})
df.to_csv('data/Eurostat_arbeidstid_faktiskuke_heltid_siste_kvartal.csv', index=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values = 'value')
df_new.index.name = 'geo'
EU_snitt = str(df_new.loc["European Union - 27 countries (from 2020)", "2023-Q4"])
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y') + '.' + ' Gjennomsnitt for EU: ' + EU_snitt + '.'
chart_date_EN = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y') + '.' + ' EU average: ' + EU_snitt + '.'
date = df.iloc[0,8]
quarter = date[6]
year = date[0:4]
date_string = 'Faktisk arbeidstid per uke i timer for heltidsansatte. Tall for ' + quarter + '. kvartal ' + year + '.'
date_string_EN = 'Average number of actual weekly hours of work. Data for Q' + quarter + ' ' + year + '.'
# #Update DW
# chartid = 'Av2Nk'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"describe": {"intro": date_string}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW
# chartid = 'WD0Uz'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"describe": {"intro": date_string_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Andel deltatt i læring siste 12 AES 1MDIh 1UpQT (NO) + Share of adults learning activities 7G7wd 84Kco (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/trng_aes_100?lang=en&lastTimePeriod=4&age=Y25-64&sex=T&training=FE_NFE&geo=EU27_2020&geo=EA20&geo=BE&geo=BG&geo=CZ&geo=DK&geo=DE&geo=EE&geo=IE&geo=EL&geo=ES&geo=FR&geo=HR&geo=IT&geo=CY&geo=LV&geo=LT&geo=LU&geo=HU&geo=MT&geo=NL&geo=AT&geo=PL&geo=PT&geo=RO&geo=SI&geo=SK&geo=FI&geo=SE&geo=NO&geo=CH&geo=UK&geo=BA&geo=MK&geo=AL&geo=RS&geo=TR')
df = dataset.write('dataframe')
df=df.replace({'Czechia':'Czech Rep.','Germany (until 1990 former territory of the FRG)':'Germany', 'European Union - 27 countries (from 2020)':'EU'})
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.index.name = 'geo'
df_new.to_csv('data/Eurostat_livslang_laring_AES_siste_aar.csv', index=True)
raw_date = dataset["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
chart_date_EN = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW (NO_EU/NO)
# chartid = '1MDIh'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW (NO_ALL)
# chartid = '1UpQT'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW (EN_EU/NO)
# chartid = '7G7wd'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW (EN_ALL)
# chartid = '84Kco'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Andel deltatt i læring siste 12 AES Kjønn iwbnr (NO) + Share of adults learning activities last 12 months eIK1N (EN)
# dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/trng_aes_100?lang=en&age=Y25-64&sex=M&sex=F&training=FE_NFE&geo=EU27_2020&geo=EA20&geo=BE&geo=BG&geo=CZ&geo=DK&geo=DE&geo=EE&geo=IE&geo=EL&geo=ES&geo=FR&geo=HR&geo=IT&geo=CY&geo=LV&geo=LT&geo=LU&geo=HU&geo=MT&geo=NL&geo=AT&geo=PL&geo=PT&geo=RO&geo=SI&geo=SK&geo=FI&geo=SE&geo=NO&geo=CH&geo=UK&geo=BA&geo=MK&geo=AL&geo=RS&geo=TR&time=2016')
# df = dataset.write('dataframe')
# df=df.replace({'Czechia':'Czech Rep.','Germany (until 1990 former territory of the FRG)':'Germany', 'European Union - 27 countries (from 2020)':'EU'})
# df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Sex', values='value')
# df_new.index.name = 'geo'
# df_new.to_csv('data/Eurostat_livslang_laring_AES_siste_aar_kjonn.csv', index=True)
# raw_date = dataset["updated"]
# parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
# chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
# chart_date_EN = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
# #Update DW (NO)
# chartid = 'iwbnr'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW (EN)
# chartid = 'eIK1N'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Andel deltatt i læring siste 12 AES Alder a5821 (NO) + Share of adults learning activities age IuO7H (EN)
# dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/trng_aes_101?age=Y25-34&age=Y35-44&age=Y45-54&age=Y55-64&lastTimePeriod=1&training=FE_NFE&geo=AL&geo=AT&geo=BA&geo=BE&geo=BG&geo=CH&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=HR&geo=HU&geo=IE&geo=IT&geo=LT&geo=LU&geo=LV&geo=MK&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK&geo=TR&geo=UK')
# df = dataset.write('dataframe')
# df=df.replace({'Czechia':'Czech Rep.','Germany (until 1990 former territory of the FRG)':'Germany', 'European Union - 27 countries (from 2020)':'EU'})
# df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Age class', values='value')
# df_new.index.name = 'geo'
# df_new.to_csv('data/Eurostat_livslang_laring_AES_siste_aar_alder.csv', index=True)
# raw_date = dataset["updated"]
# parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%S%z')
# chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
# chart_date_EN = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
# #Update DW (NO)
# chartid = 'a5821'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW (EN)
# chartid = 'IuO7H'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": chart_date_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Lenght of working life 1sRO7 (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/lfsi_dwl_a?sex=T&lastTimePeriod=20&geo=AT&geo=BE&geo=BG&geo=CH&geo=CY&geo=CZ&geo=DE&geo=DK&geo=EE&geo=EL&geo=ES&geo=EU27_2020&geo=FI&geo=FR&geo=HR&geo=HU&geo=IE&geo=IS&geo=IT&geo=LT&geo=LU&geo=LV&geo=MT&geo=NL&geo=NO&geo=PL&geo=PT&geo=RO&geo=RS&geo=SE&geo=SI&geo=SK')
df = dataset.write('dataframe')
df=df.replace({'Czechia':'Czech Rep.','Germany (until 1990 former territory of the FRG)':'Germany', 'European Union - 27 countries (from 2020)':'EU'})
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.index.name = 'geo'
df_new.to_csv('data/Eurostat_working_life_lenght.csv', index=True)
EU_avg = str(df_new.iloc[7,19])
year = str(df_new.columns[19])
updated = dataset["updated"]
updated_date = datetime.strptime(updated, '%Y-%m-%dT%H:%M:%S%z')
note_EN = 'Data last published: ' + updated_date.strftime ('%d/%m/%y') + '.' + ' EU average: ' + EU_avg + '.'
description_EN = 'Estimated duration of working life in years for a person who is 15 years old in ' + year + '.'
# #Update DW
# chartid = '1sRO7'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": note_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"describe": {"intro": description_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

# #Update DW
# chartid = 'uDh6D'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": note_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"describe": {"intro": description_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)

#Lenght of working life EU average gender NvW5H (EN)
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/lfsi_dwl_a?sex=F&sex=M&sex=T&lastTimePeriod=22&geo=EU27_2020')
df = dataset.write('dataframe')
df=df.replace({'Czechia':'Czech Rep.','Germany (until 1990 former territory of the FRG)':'Germany', 'European Union - 27 countries (from 2020)':'EU'})
df_new = df.pivot(index='Sex', columns='Time', values='value')
df_new.to_csv('data/Eurostat_working_life_lenght_EU.csv', index=True)
updated = dataset["updated"]
updated_date = datetime.strptime(updated, '%Y-%m-%dT%H:%M:%S%z')
note_EN = 'Data last published: ' + updated_date.strftime ('%d/%m/%y') + '.'
# #Update DW
# chartid = 'NvW5H'
# url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
# payload = {"metadata": {"annotate": {"notes": note_EN}}}
# headers = {
#     "Authorization": ("Bearer " + access_token),
#     "Accept": "*/*",
#     "Content-Type": "application/json"
#     }
# response = requests.request("PATCH", url, json=payload, headers=headers)