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

#1_Empl
url = "https://api.datawrapper.de/v3/charts?folderId=93018&order=DESC&orderBy=createdAt&limit=100&offset=0&expand=false"
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.get(url, headers=headers)
json_object = json.loads(response.text)
chart_list_1_empl = []
for i in json_object['list']:
        chart_list_1_empl.append(i['publicId'])

#2_Forc
url = "https://api.datawrapper.de/v3/charts?folderId=100256&order=DESC&orderBy=createdAt&limit=100&offset=0&expand=false"
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.get(url, headers=headers)
json_object = json.loads(response.text)
chart_list_2_forc = []
for i in json_object['list']:
        chart_list_2_forc.append(i['publicId'])

#3_Jobs
url = "https://api.datawrapper.de/v3/charts?folderId=91886&order=DESC&orderBy=createdAt&limit=100&offset=0&expand=false"
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.get(url, headers=headers)
json_object = json.loads(response.text)
chart_list_3_jobs = []
for i in json_object['list']:
        chart_list_3_jobs.append(i['publicId'])

#4_LifL
url = "https://api.datawrapper.de/v3/charts?folderId=99109&order=DESC&orderBy=createdAt&limit=100&offset=0&expand=false"
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.get(url, headers=headers)
json_object = json.loads(response.text)
chart_list_4_lifl = []
for i in json_object['list']:
        chart_list_4_lifl.append(i['publicId'])

#5_Prod
url = "https://api.datawrapper.de/v3/charts?folderId=93393&order=DESC&orderBy=createdAt&limit=100&offset=0&expand=false"
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.get(url, headers=headers)
json_object = json.loads(response.text)
chart_list_5_prod = []
for i in json_object['list']:
        chart_list_5_prod.append(i['publicId'])

#6_Unem
url = "https://api.datawrapper.de/v3/charts?folderId=92857&order=DESC&orderBy=createdAt&limit=100&offset=0&expand=false"
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.get(url, headers=headers)
json_object = json.loads(response.text)
chart_list_6_unem = []
for i in json_object['list']:
        chart_list_6_unem.append(i['publicId'])

#7_Unie
url = "https://api.datawrapper.de/v3/charts?folderId=93514&order=DESC&orderBy=createdAt&limit=100&offset=0&expand=false"
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.get(url, headers=headers)
json_object = json.loads(response.text)
chart_list_7_unie = []
for i in json_object['list']:
        chart_list_7_unie.append(i['publicId'])

#8_Vaca
url = "https://api.datawrapper.de/v3/charts?folderId=92826&order=DESC&orderBy=createdAt&limit=100&offset=0&expand=false"
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.get(url, headers=headers)
json_object = json.loads(response.text)
chart_list_8_vaca = []
for i in json_object['list']:
        chart_list_8_vaca.append(i['publicId'])

#9_Wage
url = "https://api.datawrapper.de/v3/charts?folderId=93663&order=DESC&orderBy=createdAt&limit=100&offset=0&expand=false"
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.get(url, headers=headers)
json_object = json.loads(response.text)
chart_list_9_wage = []
for i in json_object['list']:
        chart_list_9_wage.append(i['publicId'])

#10_Work
url = "https://api.datawrapper.de/v3/charts?folderId=91886&order=DESC&orderBy=createdAt&limit=100&offset=0&expand=false"
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*"
    }
response = requests.get(url, headers=headers)
json_object = json.loads(response.text)
chart_list_10_work = []
for i in json_object['list']:
        chart_list_10_work.append(i['publicId'])

#Publish
chart_list_EN_all = (chart_list_1_empl + chart_list_2_forc + chart_list_3_jobs + chart_list_4_lifl + chart_list_5_prod + chart_list_6_unem + chart_list_7_unie + chart_list_8_vaca + chart_list_9_wage + chart_list_10_work)
for vars in chart_list_EN_all:
    url = "https://api.datawrapper.de/v3/charts/" + vars + '/publish/'
    headers = {
        "Authorization": ("Bearer " + access_token),
        "Accept": "*/*"
        }
    response = requests.request("POST", url, headers=headers)