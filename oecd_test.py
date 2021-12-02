from pyjstat import pyjstat
import requests
import os
os.makedirs('data', exist_ok=True)
ssburl = 'https://stats.oecd.org/SDMX-JSON/data/STLABOUR/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+OECD+RUS+ZAF.LRACTTFE+LRACTTMA+LRACTTTT.STSA.Q/all?startTime=2019-Q2&endTime=2021-Q4&dimensionAtObservation=allDimensions&pid=c0dcdd50-2d08-440b-94d7-8aa50471b7ff'
resultat = requests.post(oecdurl)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df = dataset.write('dataframe')
df.to_csv('data/oecd.csv', index=False)
