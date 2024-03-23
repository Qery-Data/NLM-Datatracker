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

rename_columns = {"AUS": "Australia", "AUT": "Austria", "BEL": "Belgium", "CAN": "Canada", "CHE": "Switerland", "CHL": "Chile", "COL": "Colombia", "CRI": "Costa Rica", "CZE": "Czechia", "DNK": "Denmark", "EST": "Estonia", "EA20": "Euro area", "EU27_2020": "EU27", "FIN": "Finland", "FRA": "France", "DEU": "Germany", "GRC": "Greece", "HUN": "Hungary", "ISL": "Iceland", "IRL": "Ireland", "ISR": "Israel", "ITA": "Italy", "JPN": "Japan", "KOR": "Korea", "LVA": "Latvia", "LTU": "Lithuania", "LUX": "Luxembourg", "MEX": "Mexico", "NLD": "Netherlands", "NZL": "New Zealand", "NOR": "Norway", "POL": "Poland", "PRT": "Portugal", "SVK": "Slovak Republic", "SVN": "Slovenia", "ESP": "Spain", "SWE": "Sweden", "TUR": "Türkiye", "GBR": "United Kingdom", "USA": "United States"}

rename_columns_no = {"AUS": "Australia", "AUT": "Østerrike", "BEL": "Belgia", "CAN": "Canada", "CHE": "Sveits", "CHL": "Chile", "COL": "Colombia", "CRI": "Costa Rica", "CZE": "Tsjekkia", "DNK": "Danmark", "EST": "Estland", "EA20": "Euroområdet", "EU27_2020": "EU27", "FIN": "Finland", "FRA": "Frankrike", "DEU": "Tyskland", "GRC": "Hellas", "HUN": "Ungarn", "ISL": "Island", "IRL": "Irland", "ISR": "Israel", "ITA": "Italia", "JPN": "Japan", "KOR": "Sør-Korea", "LVA": "Latvia", "LTU": "Litauen", "LUX": "Luxembourg", "MEX": "Mexico", "NLD": "Nederland", "NZL": "New Zealand", "NOR": "Norge", "POL": "Polen", "PRT": "Portugal", "SVK": "Slovakia", "SVN": "Slovenia", "ESP": "Spania", "SWE": "Sverige", "TUR": "Tyrkia", "GBR": "Storbritannia", "USA": "USA"}

#Gjennomsnittlig årlig arbeidstimer d6F9Q (NO) and 2BNuF (EN)
oecd_url='https://sdmx.oecd.org/public/rest/data/OECD.ELS.SAE,DSD_HW@DF_AVG_ANN_HRS_WKD,1.0/DNK+NOR+SWE+USA+OECD+EU27........_T....?startPeriod=1970'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='TIME_PERIOD', columns='REF_AREA', values='OBS_VALUE')
df_new.index.rename("TIME", inplace=True)
df_new = df_new.rename(columns=rename_columns)
df_new.to_csv('data/OECD_arbeidstid_aarligsnitt.csv', index=True)

#Produktivitet BNP per timeverk siste år
oecd_url='https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_PDB@DF_PDB_LV,1.0/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+OECD.A.GDPHRS..USD_EXC_H.V...?lastNObservations=2'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new = df_new.rename(index=rename_columns)
df_new.to_csv('data/OECD_Produktivitet_BNP_EN.csv', index=True)
df_new_no = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new_no = df_new_no.rename(index=rename_columns_no)
df_new_no.to_csv('data/OECD_Produktivitet_BNP_NO.csv', index=True)

#Produktivitet BNI per timeverk siste år
oecd_url='https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_PDB@DF_PDB_LV,1.0/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+OECD.A.GNIHRS..USD_EXC_H.V...?lastNObservations=2'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new = df_new.rename(index=rename_columns)
df_new.to_csv('data/OECD_Produktivitet_BNI_EN.csv', index=True)
df_new_no = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new_no = df_new_no.rename(index=rename_columns_no)
df_new_no.to_csv('data/OECD_Produktivitet_BNI_NO.csv', index=True)

#Produktivitet sammenligning av vekstrater 2000-2007 v 2010-2022
oecd_url='https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_PDB@DF_PDB_GR,1.0/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+HUN+GRC+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+OECD.A.GDPHRS..PA....?startPeriod=2000&dimensionAtObservation=AllDimensions'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new = df_new.rename(index=rename_columns)
df_new['2000-2007'] = df_new.loc[:, 2000:2007].mean(axis=1)
df_new['2010-2022'] = df_new.loc[:, 2010:2022].mean(axis=1)
df_new.to_csv('data/OECD_Produktivitet_Vekstrater_EN.csv', index=True)
df_new_no = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new_no['2000-2007'] = df_new_no.loc[:, 2000:2007].mean(axis=1)
df_new_no['2010-2022'] = df_new_no.loc[:, 2010:2022].mean(axis=1)
df_new_no = df_new_no.rename(index=rename_columns_no)
df_new_no.to_csv('data/OECD_Produktivitet_Vekstrater_NO.csv', index=True)

#Sysselsettingsandel OECD CS8Rb (NO) VKfA9 (EN)
oecd_url='https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_EMP_WAP_Q,1.0/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+OECD+EU27_2020.EMP_WAP.._Z.Y._T.Y15T74..Q?lastNObservations=4&dimensionAtObservation=AllDimensions'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new = df_new.rename(index=rename_columns)
df_new.to_csv('data/OECD_Employment_Rate_EN.csv', index=True)
df_new_no = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new_no = df_new_no.rename(index=rename_columns_no)
df_new_no.to_csv('data/OECD_Employment_Rate_NO.csv', index=True)

#Sysselsettingsandel Menn OECD YpL1m (NO) Mqkeh (EN)
oecd_url='https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_EMP_WAP_Q,1.0/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+OECD+EU27_2020.EMP_WAP.._Z.Y.M.Y15T74..Q?lastNObservations=4&dimensionAtObservation=AllDimensions'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new = df_new.rename(index=rename_columns)
df_new.to_csv('data/OECD_Employment_Rate_Men_EN.csv', index=True)
df_new_no = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new_no = df_new_no.rename(index=rename_columns_no)
df_new_no.to_csv('data/OECD_Employment_Rate_Men_NO.csv', index=True)

#Sysselsettingsandel Kvinner OECD ZERuL (NO) SuY2u (EN)
oecd_url='https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_IALFS_EMP_WAP_Q,1.0/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+OECD+EU27_2020.EMP_WAP.._Z.Y.F.Y15T74..Q?lastNObservations=4&dimensionAtObservation=AllDimensions'
result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
df=pd.read_csv(io.StringIO(result.text))
df_new = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new = df_new.rename(index=rename_columns)
df_new.to_csv('data/OECD_Employment_Rate_Women_EN.csv', index=True)
df_new_no = df.pivot(index='REF_AREA', columns='TIME_PERIOD', values='OBS_VALUE')
df_new_no = df_new_no.rename(index=rename_columns_no)
df_new_no.to_csv('data/OECD_Employment_Rate_Women_NO.csv', index=True)

#Midlertidig ansatte OECD ohRTM (NO) vX91z (EN)
#oecd_url='https://stats.oecd.org/SDMX-JSON/data/TEMP_I/AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+EA19+EU27_2020+OECD+ARG+BRA+BGR+CHN+CYP+IND+IDN+MLT+ROU+SAU+ZAF.MW.900000.DE.PER_CENT_TEMP.A/all?startTime=2012'
#result = requests.get(oecd_url, headers={'Accept': 'text/csv'})
#df=pd.read_csv(io.StringIO(result.text))
#df_new = df.pivot(index='Country', columns='TIME', values='Value')
#df_new.to_csv('data/OECD_Temporary_Employment.csv', index=True)
