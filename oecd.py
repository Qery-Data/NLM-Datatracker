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


#END


