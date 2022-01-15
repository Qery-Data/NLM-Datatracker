from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import pandas as pd
os.makedirs('data', exist_ok=True)
access_token = os.getenv('DW_TOKEN')

#Ledige stillinger
def get_from_oecd(sdmx_query):
    return pd.read_csv(
        f"https://stats.oecd.org/SDMX-JSON/data/{sdmx_query}?contentType=csv"
    )
data = (get_from_oecd("LAB_REG_VAC/SWE+DEN+FIN+NOR.LMUNRLTT_STSA.M/"))
df = pd.DataFrame(data)
df_new = df.pivot(index='TIME', columns='Country', values='Value')
df_new2 = df_new.tail(12)
df_new2.to_csv('data/OECDledigestillinger.csv', index=True)

#Gjennomsnittlig årlig arbeidstimer
def get_from_oecd(sdmx_query):
    return pd.read_csv(
        f"https://stats.oecd.org/SDMX-JSON/data/{sdmx_query}?contentType=csv"
    )
data = (get_from_oecd("ANHRS/SWE+DEN+FIN+NOR/"))
df = pd.DataFrame(data)
df_new = df.pivot(index='TIME', columns='Country', values='Value')
df_new2 = df_new.tail(12)
df_new2.to_csv('data/OECD_arbeidstid_aarligsnitt.csv', index=True)
