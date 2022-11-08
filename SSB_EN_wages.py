from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import pandas as pd
os.makedirs('data', exist_ok=True)
access_token = os.getenv('DW_TOKEN')

#Wages, nominal xJSYI, annual and real growth
ssburl = 'https://data.ssb.no/api/v0/en/table/09785/'
query = {
  "query": [
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Arslonn",
          "ArslonnEndring"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='year', columns='contents',values='value')
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
ssburl = 'https://data.ssb.no/api/v0/en/table/03014/'
query = {
  "query": [
    {
      "code": "Konsumgrp",
      "selection": {
        "filter": "vs:CoiCop2016niva1",
        "values": [
          "TOTAL"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "KpiAar"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "all",
        "values": [
          "*"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='year', columns='contents', values='value')
df3_new = pd.concat([df_new,df2_new.reindex(df_new.index)], axis=1)
df3_new['Earnings*CPI>2016']= df3_new['Annual earnings (NOK 1 000)'] / (df3_new['Consumer Price Index (2015=100)']/100)
df3_new['Real>16'] = df3_new['Earnings*CPI>2016'].pct_change(periods=1)*100
df3_new.rename(columns = {'Real>16':'Real wage growth'}, inplace=True)
df4_new = df3_new.iloc[:,[0,1,4]]
df5_new = df4_new.iloc[3:]
df5_new.to_csv('data_EN/SSB_earningswages_annual_real_nominal.csv', index=True)
#Update DW
chartid = 'xJSYI'
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y') + ' *Annual earnings comprises the regular wage, including bonus and irregular supplements, but excluding over-time pay.'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

chartid = 'JCzze'
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y') 
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

chartid = 'HhExj'
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Average monthly earnings by occupation level 4 qiHCS
ssburl = 'https://data.ssb.no/api/v0/en/table/11418/'
query = {
  "query": [
    {
      "code": "MaaleMetode",
      "selection": {
        "filter": "item",
        "values": [
          "02"
        ]
      }
    },
    {
      "code": "Yrke",
      "selection": {
        "filter": "vs:NYK08Lonnansatt",
        "values": [
          "0000",
          "0110",
          "0210",
          "0310",
          "1111",
          "1112",
          "1114",
          "1120",
          "1211",
          "1212",
          "1213",
          "1219",
          "1221",
          "1222",
          "1223",
          "1311",
          "1312",
          "1321",
          "1322",
          "1323",
          "1324",
          "1330",
          "1341",
          "1342",
          "1343",
          "1344",
          "1345",
          "1346",
          "1349",
          "1411",
          "1412",
          "1420",
          "1431",
          "1439",
          "2111",
          "2112",
          "2113",
          "2114",
          "2120",
          "2131",
          "2132",
          "2133",
          "2141",
          "2142",
          "2143",
          "2144",
          "2145",
          "2146",
          "2149",
          "2151",
          "2152",
          "2153",
          "2161",
          "2162",
          "2163",
          "2164",
          "2165",
          "2166",
          "2211",
          "2212",
          "2221",
          "2222",
          "2223",
          "2224",
          "2250",
          "2261",
          "2262",
          "2263",
          "2264",
          "2265",
          "2266",
          "2267",
          "2269",
          "2310",
          "2320",
          "2330",
          "2341",
          "2342",
          "2351",
          "2352",
          "2353",
          "2354",
          "2355",
          "2356",
          "2359",
          "2411",
          "2412",
          "2413",
          "2421",
          "2422",
          "2423",
          "2424",
          "2431",
          "2432",
          "2433",
          "2434",
          "2511",
          "2512",
          "2513",
          "2514",
          "2519",
          "2521",
          "2522",
          "2523",
          "2529",
          "2611",
          "2612",
          "2619",
          "2621",
          "2622",
          "2631",
          "2632",
          "2633",
          "2634",
          "2635",
          "2636",
          "2641",
          "2642",
          "2643",
          "2651",
          "2652",
          "2653",
          "2654",
          "2655",
          "2656",
          "2659",
          "3112",
          "3113",
          "3114",
          "3115",
          "3116",
          "3117",
          "3118",
          "3119",
          "3121",
          "3122",
          "3123",
          "3131",
          "3132",
          "3133",
          "3134",
          "3135",
          "3139",
          "3141",
          "3142",
          "3143",
          "3151",
          "3152",
          "3153",
          "3154",
          "3155",
          "3211",
          "3212",
          "3213",
          "3214",
          "3230",
          "3240",
          "3251",
          "3254",
          "3256",
          "3257",
          "3258",
          "3259",
          "3311",
          "3312",
          "3313",
          "3315",
          "3321",
          "3322",
          "3323",
          "3324",
          "3331",
          "3332",
          "3333",
          "3334",
          "3339",
          "3341",
          "3342",
          "3343",
          "3351",
          "3352",
          "3353",
          "3354",
          "3355",
          "3359",
          "3411",
          "3412",
          "3413",
          "3421",
          "3422",
          "3423",
          "3431",
          "3432",
          "3433",
          "3434",
          "3439",
          "3511",
          "3512",
          "3513",
          "3514",
          "3521",
          "3522",
          "4110",
          "4131",
          "4132",
          "4211",
          "4212",
          "4213",
          "4214",
          "4221",
          "4222",
          "4223",
          "4224",
          "4225",
          "4226",
          "4227",
          "4229",
          "4311",
          "4312",
          "4313",
          "4321",
          "4322",
          "4323",
          "4411",
          "4412",
          "4413",
          "4415",
          "4416",
          "5111",
          "5112",
          "5113",
          "5120",
          "5131",
          "5132",
          "5141",
          "5142",
          "5151",
          "5152",
          "5153",
          "5161",
          "5163",
          "5164",
          "5165",
          "5169",
          "5211",
          "5212",
          "5221",
          "5222",
          "5223",
          "5230",
          "5241",
          "5242",
          "5243",
          "5244",
          "5245",
          "5246",
          "5249",
          "5311",
          "5312",
          "5321",
          "5322",
          "5329",
          "5411",
          "5413",
          "5414",
          "5419",
          "6111",
          "6112",
          "6113",
          "6114",
          "6121",
          "6122",
          "6123",
          "6129",
          "6130",
          "6210",
          "6221",
          "6222",
          "6224",
          "7112",
          "7113",
          "7114",
          "7115",
          "7119",
          "7121",
          "7122",
          "7123",
          "7124",
          "7125",
          "7126",
          "7127",
          "7131",
          "7132",
          "7133",
          "7211",
          "7212",
          "7213",
          "7214",
          "7215",
          "7221",
          "7222",
          "7223",
          "7224",
          "7231",
          "7232",
          "7233",
          "7234",
          "7311",
          "7312",
          "7313",
          "7314",
          "7315",
          "7316",
          "7317",
          "7318",
          "7319",
          "7321",
          "7322",
          "7323",
          "7411",
          "7412",
          "7413",
          "7421",
          "7422",
          "7511",
          "7512",
          "7513",
          "7514",
          "7515",
          "7522",
          "7531",
          "7532",
          "7534",
          "7535",
          "7536",
          "7541",
          "7542",
          "7543",
          "7544",
          "7549",
          "8111",
          "8112",
          "8113",
          "8114",
          "8121",
          "8122",
          "8131",
          "8132",
          "8141",
          "8142",
          "8143",
          "8151",
          "8152",
          "8153",
          "8154",
          "8155",
          "8156",
          "8157",
          "8159",
          "8160",
          "8171",
          "8172",
          "8181",
          "8182",
          "8183",
          "8189",
          "8211",
          "8212",
          "8219",
          "8311",
          "8312",
          "8322",
          "8331",
          "8332",
          "8341",
          "8342",
          "8343",
          "8344",
          "8350",
          "9111",
          "9112",
          "9122",
          "9123",
          "9129",
          "9211",
          "9212",
          "9213",
          "9214",
          "9215",
          "9216",
          "9311",
          "9312",
          "9313",
          "9321",
          "9329",
          "9331",
          "9333",
          "9334",
          "9412",
          "9510",
          "9611",
          "9612",
          "9613",
          "9621",
          "9622",
          "9623",
          "9629"
        ]
      }
    },
    {
      "code": "AvtaltVanlig",
      "selection": {
        "filter": "item",
        "values": [
          "5"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Manedslonn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          5
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='occupation',columns='year',values='value')
df_new['Endring sist år'] = (df_new.iloc[:,4]-df_new.iloc[:,3])/df_new.iloc[:,3]*100
df_new['Endring siste 5 år'] = (df_new.iloc[:,4]-df_new.iloc[:,0])/df_new.iloc[:,0]*100
df_new.dropna(inplace=True)
df_new['Sist år'] = (df_new.iloc[:,4])
df_new2 = df_new[['Sist år', 'Endring sist år', 'Endring siste 5 år']]
df_new2.to_csv('data_EN/SSB_earningswages_occupation_monthly.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
title_date = str(df_new.columns[4])
date_string = 'Average monthly earnings in NOK. Full-time employees. Data for ' + title_date +'.'
#Update DW
chartid = 'qiHCS'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
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

#Average and median monthly earnings by occupation level 4
ssburl = 'https://data.ssb.no/api/v0/en/table/11418/'
query = {
  "query": [
    {
      "code": "MaaleMetode",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02"
        ]
      }
    },
    {
      "code": "Yrke",
      "selection": {
        "filter": "vs:NYK08Lonnansatt",
        "values": [
          "0000",
          "0110",
          "0210",
          "0310",
          "1111",
          "1112",
          "1114",
          "1120",
          "1211",
          "1212",
          "1213",
          "1219",
          "1221",
          "1222",
          "1223",
          "1311",
          "1312",
          "1321",
          "1322",
          "1323",
          "1324",
          "1330",
          "1341",
          "1342",
          "1343",
          "1344",
          "1345",
          "1346",
          "1349",
          "1411",
          "1412",
          "1420",
          "1431",
          "1439",
          "2111",
          "2112",
          "2113",
          "2114",
          "2120",
          "2131",
          "2132",
          "2133",
          "2141",
          "2142",
          "2143",
          "2144",
          "2145",
          "2146",
          "2149",
          "2151",
          "2152",
          "2153",
          "2161",
          "2162",
          "2163",
          "2164",
          "2165",
          "2166",
          "2211",
          "2212",
          "2221",
          "2222",
          "2223",
          "2224",
          "2250",
          "2261",
          "2262",
          "2263",
          "2264",
          "2265",
          "2266",
          "2267",
          "2269",
          "2310",
          "2320",
          "2330",
          "2341",
          "2342",
          "2351",
          "2352",
          "2353",
          "2354",
          "2355",
          "2356",
          "2359",
          "2411",
          "2412",
          "2413",
          "2421",
          "2422",
          "2423",
          "2424",
          "2431",
          "2432",
          "2433",
          "2434",
          "2511",
          "2512",
          "2513",
          "2514",
          "2519",
          "2521",
          "2522",
          "2523",
          "2529",
          "2611",
          "2612",
          "2619",
          "2621",
          "2622",
          "2631",
          "2632",
          "2633",
          "2634",
          "2635",
          "2636",
          "2641",
          "2642",
          "2643",
          "2651",
          "2652",
          "2653",
          "2654",
          "2655",
          "2656",
          "2659",
          "3112",
          "3113",
          "3114",
          "3115",
          "3116",
          "3117",
          "3118",
          "3119",
          "3121",
          "3122",
          "3123",
          "3131",
          "3132",
          "3133",
          "3134",
          "3135",
          "3139",
          "3141",
          "3142",
          "3143",
          "3151",
          "3152",
          "3153",
          "3154",
          "3155",
          "3211",
          "3212",
          "3213",
          "3214",
          "3230",
          "3240",
          "3251",
          "3254",
          "3256",
          "3257",
          "3258",
          "3259",
          "3311",
          "3312",
          "3313",
          "3315",
          "3321",
          "3322",
          "3323",
          "3324",
          "3331",
          "3332",
          "3333",
          "3334",
          "3339",
          "3341",
          "3342",
          "3343",
          "3351",
          "3352",
          "3353",
          "3354",
          "3355",
          "3359",
          "3411",
          "3412",
          "3413",
          "3421",
          "3422",
          "3423",
          "3431",
          "3432",
          "3433",
          "3434",
          "3439",
          "3511",
          "3512",
          "3513",
          "3514",
          "3521",
          "3522",
          "4110",
          "4131",
          "4132",
          "4211",
          "4212",
          "4213",
          "4214",
          "4221",
          "4222",
          "4223",
          "4224",
          "4225",
          "4226",
          "4227",
          "4229",
          "4311",
          "4312",
          "4313",
          "4321",
          "4322",
          "4323",
          "4411",
          "4412",
          "4413",
          "4415",
          "4416",
          "5111",
          "5112",
          "5113",
          "5120",
          "5131",
          "5132",
          "5141",
          "5142",
          "5151",
          "5152",
          "5153",
          "5161",
          "5163",
          "5164",
          "5165",
          "5169",
          "5211",
          "5212",
          "5221",
          "5222",
          "5223",
          "5230",
          "5241",
          "5242",
          "5243",
          "5244",
          "5245",
          "5246",
          "5249",
          "5311",
          "5312",
          "5321",
          "5322",
          "5329",
          "5411",
          "5413",
          "5414",
          "5419",
          "6111",
          "6112",
          "6113",
          "6114",
          "6121",
          "6122",
          "6123",
          "6129",
          "6130",
          "6210",
          "6221",
          "6222",
          "6224",
          "7112",
          "7113",
          "7114",
          "7115",
          "7119",
          "7121",
          "7122",
          "7123",
          "7124",
          "7125",
          "7126",
          "7127",
          "7131",
          "7132",
          "7133",
          "7211",
          "7212",
          "7213",
          "7214",
          "7215",
          "7221",
          "7222",
          "7223",
          "7224",
          "7231",
          "7232",
          "7233",
          "7234",
          "7311",
          "7312",
          "7313",
          "7314",
          "7315",
          "7316",
          "7317",
          "7318",
          "7319",
          "7321",
          "7322",
          "7323",
          "7411",
          "7412",
          "7413",
          "7421",
          "7422",
          "7511",
          "7512",
          "7513",
          "7514",
          "7515",
          "7522",
          "7531",
          "7532",
          "7534",
          "7535",
          "7536",
          "7541",
          "7542",
          "7543",
          "7544",
          "7549",
          "8111",
          "8112",
          "8113",
          "8114",
          "8121",
          "8122",
          "8131",
          "8132",
          "8141",
          "8142",
          "8143",
          "8151",
          "8152",
          "8153",
          "8154",
          "8155",
          "8156",
          "8157",
          "8159",
          "8160",
          "8171",
          "8172",
          "8181",
          "8182",
          "8183",
          "8189",
          "8211",
          "8212",
          "8219",
          "8311",
          "8312",
          "8322",
          "8331",
          "8332",
          "8341",
          "8342",
          "8343",
          "8344",
          "8350",
          "9111",
          "9112",
          "9122",
          "9123",
          "9129",
          "9211",
          "9212",
          "9213",
          "9214",
          "9215",
          "9216",
          "9311",
          "9312",
          "9313",
          "9321",
          "9329",
          "9331",
          "9333",
          "9334",
          "9412",
          "9510",
          "9611",
          "9612",
          "9613",
          "9621",
          "9622",
          "9623",
          "9629"
        ]
      }
    },
    {
      "code": "AvtaltVanlig",
      "selection": {
        "filter": "item",
        "values": [
          "5"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Manedslonn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          1
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='occupation',columns='measuring method',values='value')
df_new.dropna(inplace=True)
df_new['Difference in NOK']=df_new['Average']-df_new['Median']
df_new['Difference in %']=(df_new['Average']-df_new['Median'])/(df_new['Average'])*100
df_new.to_csv('data_EN/SSB_earningswages_occupation_monthly_average_median.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 'HhAWT'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Average monthly earnings by sector level 4
ssburl = 'https://data.ssb.no/api/v0/en/table/11418/'
query = {
  "query": [
    {
      "code": "MaaleMetode",
      "selection": {
        "filter": "item",
        "values": [
          "02"
        ]
      }
    },
    {
      "code": "Yrke",
      "selection": {
        "filter": "vs:NYK08Lonnansatt",
        "values": [
          "0000",
          "0110",
          "0210",
          "0310",
          "1111",
          "1112",
          "1114",
          "1120",
          "1211",
          "1212",
          "1213",
          "1219",
          "1221",
          "1222",
          "1223",
          "1311",
          "1312",
          "1321",
          "1322",
          "1323",
          "1324",
          "1330",
          "1341",
          "1342",
          "1343",
          "1344",
          "1345",
          "1346",
          "1349",
          "1411",
          "1412",
          "1420",
          "1431",
          "1439",
          "2111",
          "2112",
          "2113",
          "2114",
          "2120",
          "2131",
          "2132",
          "2133",
          "2141",
          "2142",
          "2143",
          "2144",
          "2145",
          "2146",
          "2149",
          "2151",
          "2152",
          "2153",
          "2161",
          "2162",
          "2163",
          "2164",
          "2165",
          "2166",
          "2211",
          "2212",
          "2221",
          "2222",
          "2223",
          "2224",
          "2250",
          "2261",
          "2262",
          "2263",
          "2264",
          "2265",
          "2266",
          "2267",
          "2269",
          "2310",
          "2320",
          "2330",
          "2341",
          "2342",
          "2351",
          "2352",
          "2353",
          "2354",
          "2355",
          "2356",
          "2359",
          "2411",
          "2412",
          "2413",
          "2421",
          "2422",
          "2423",
          "2424",
          "2431",
          "2432",
          "2433",
          "2434",
          "2511",
          "2512",
          "2513",
          "2514",
          "2519",
          "2521",
          "2522",
          "2523",
          "2529",
          "2611",
          "2612",
          "2619",
          "2621",
          "2622",
          "2631",
          "2632",
          "2633",
          "2634",
          "2635",
          "2636",
          "2641",
          "2642",
          "2643",
          "2651",
          "2652",
          "2653",
          "2654",
          "2655",
          "2656",
          "2659",
          "3112",
          "3113",
          "3114",
          "3115",
          "3116",
          "3117",
          "3118",
          "3119",
          "3121",
          "3122",
          "3123",
          "3131",
          "3132",
          "3133",
          "3134",
          "3135",
          "3139",
          "3141",
          "3142",
          "3143",
          "3151",
          "3152",
          "3153",
          "3154",
          "3155",
          "3211",
          "3212",
          "3213",
          "3214",
          "3230",
          "3240",
          "3251",
          "3254",
          "3256",
          "3257",
          "3258",
          "3259",
          "3311",
          "3312",
          "3313",
          "3315",
          "3321",
          "3322",
          "3323",
          "3324",
          "3331",
          "3332",
          "3333",
          "3334",
          "3339",
          "3341",
          "3342",
          "3343",
          "3351",
          "3352",
          "3353",
          "3354",
          "3355",
          "3359",
          "3411",
          "3412",
          "3413",
          "3421",
          "3422",
          "3423",
          "3431",
          "3432",
          "3433",
          "3434",
          "3439",
          "3511",
          "3512",
          "3513",
          "3514",
          "3521",
          "3522",
          "4110",
          "4131",
          "4132",
          "4211",
          "4212",
          "4213",
          "4214",
          "4221",
          "4222",
          "4223",
          "4224",
          "4225",
          "4226",
          "4227",
          "4229",
          "4311",
          "4312",
          "4313",
          "4321",
          "4322",
          "4323",
          "4411",
          "4412",
          "4413",
          "4415",
          "4416",
          "5111",
          "5112",
          "5113",
          "5120",
          "5131",
          "5132",
          "5141",
          "5142",
          "5151",
          "5152",
          "5153",
          "5161",
          "5163",
          "5164",
          "5165",
          "5169",
          "5211",
          "5212",
          "5221",
          "5222",
          "5223",
          "5230",
          "5241",
          "5242",
          "5243",
          "5244",
          "5245",
          "5246",
          "5249",
          "5311",
          "5312",
          "5321",
          "5322",
          "5329",
          "5411",
          "5413",
          "5414",
          "5419",
          "6111",
          "6112",
          "6113",
          "6114",
          "6121",
          "6122",
          "6123",
          "6129",
          "6130",
          "6210",
          "6221",
          "6222",
          "6224",
          "7112",
          "7113",
          "7114",
          "7115",
          "7119",
          "7121",
          "7122",
          "7123",
          "7124",
          "7125",
          "7126",
          "7127",
          "7131",
          "7132",
          "7133",
          "7211",
          "7212",
          "7213",
          "7214",
          "7215",
          "7221",
          "7222",
          "7223",
          "7224",
          "7231",
          "7232",
          "7233",
          "7234",
          "7311",
          "7312",
          "7313",
          "7314",
          "7315",
          "7316",
          "7317",
          "7318",
          "7319",
          "7321",
          "7322",
          "7323",
          "7411",
          "7412",
          "7413",
          "7421",
          "7422",
          "7511",
          "7512",
          "7513",
          "7514",
          "7515",
          "7522",
          "7531",
          "7532",
          "7534",
          "7535",
          "7536",
          "7541",
          "7542",
          "7543",
          "7544",
          "7549",
          "8111",
          "8112",
          "8113",
          "8114",
          "8121",
          "8122",
          "8131",
          "8132",
          "8141",
          "8142",
          "8143",
          "8151",
          "8152",
          "8153",
          "8154",
          "8155",
          "8156",
          "8157",
          "8159",
          "8160",
          "8171",
          "8172",
          "8181",
          "8182",
          "8183",
          "8189",
          "8211",
          "8212",
          "8219",
          "8311",
          "8312",
          "8322",
          "8331",
          "8332",
          "8341",
          "8342",
          "8343",
          "8344",
          "8350",
          "9111",
          "9112",
          "9122",
          "9123",
          "9129",
          "9211",
          "9212",
          "9213",
          "9214",
          "9215",
          "9216",
          "9311",
          "9312",
          "9313",
          "9321",
          "9329",
          "9331",
          "9333",
          "9334",
          "9412",
          "9510",
          "9611",
          "9612",
          "9613",
          "9621",
          "9622",
          "9623",
          "9629"
        ]
      }
    },
    {
      "code": "Sektor",
      "selection": {
        "filter": "item",
        "values": [
          "ALLE",
          "A+B+D+E",
          "6500",
          "6100"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0"
        ]
      }
    },
    {
      "code": "AvtaltVanlig",
      "selection": {
        "filter": "item",
        "values": [
          "5"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Manedslonn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          1
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='occupation',columns='sector',values='value')
df_new.dropna(thresh=3, inplace=True)
df_new = df_new[['Sum all sectors','Private sector and public enterprises','Central government','Local government']]
df_new.to_csv('data_EN/SSB_earningswages_occupation_sector.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y') + ' Data not avaliable for all sectors and occupations.'
title_date = str(df.iloc[0,6])
date_string = 'Average monthly earnings by sector. Full-time employees. Data for ' + title_date + '.'
#Update DW
chartid = 'fEPHK'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
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

#Earnings differences women and men average and median jGRUZ  
ssburl = 'https://data.ssb.no/api/v0/en/table/11418/'
query = {
  "query": [
    {
      "code": "MaaleMetode",
      "selection": {
        "filter": "item",
        "values": [
          "02",
          "01"
        ]
      }
    },
    {
      "code": "Yrke",
      "selection": {
        "filter": "vs:NYK08Lonnansatt1siff",
        "values": [
          "0-9"
        ]
      }
    },
    {
      "code": "Sektor",
      "selection": {
        "filter": "item",
        "values": [
          "ALLE"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "1",
          "2"
        ]
      }
    },
    {
      "code": "AvtaltVanlig",
      "selection": {
        "filter": "item",
        "values": [
          "0",
          "5"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Manedslonn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
         1
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df['målarbeidstid']=df['measuring method']+df['contractual/usual working hours per week']
df_new=df.pivot(index='målarbeidstid', columns='sex', values='value')
df_new = df_new.reindex(['AverageAll employees','AverageFull-time employees','MedianAll employees','MedianFull-time employees'])
df_new.to_csv('data_EN/SSB_earningswages_women_men.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
title_date = str(df.iloc[0,6])
date_string = ' By different measurements (in NOK).'+' Data for ' + title_date +'.'
#Update DW
chartid = 'jGRUZ'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
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

#Earnings change women and men average and median VlBm7
ssburl = 'https://data.ssb.no/api/v0/en/table/11418/'
query = {
  "query": [
    {
      "code": "MaaleMetode",
      "selection": {
        "filter": "item",
        "values": [
          "02",
          "01"
        ]
      }
    },
    {
      "code": "Yrke",
      "selection": {
        "filter": "vs:NYK08Lonnansatt1siff",
        "values": [
          "0-9"
        ]
      }
    },
    {
      "code": "Sektor",
      "selection": {
        "filter": "item",
        "values": [
          "ALLE"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "1",
          "2"
        ]
      }
    },
    {
      "code": "AvtaltVanlig",
      "selection": {
        "filter": "item",
        "values": [
          "0"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Manedslonn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
         5
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df['measuresex'] = df['measuring method']+df['sex']
df_new = df.pivot(index='year', columns='measuresex',values='value')

#full-time 
ssburl = 'https://data.ssb.no/api/v0/en/table/11418/'
query = {
  "query": [
    {
      "code": "MaaleMetode",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02"
        ]
      }
    },
    {
      "code": "Yrke",
      "selection": {
        "filter": "vs:NYK08Lonnansatt1siff",
        "values": [
          "0-9"
        ]
      }
    },
    {
      "code": "Sektor",
      "selection": {
        "filter": "item",
        "values": [
          "ALLE"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "1",
          "2"
        ]
      }
    },
    {
      "code": "AvtaltVanlig",
      "selection": {
        "filter": "item",
        "values": [
          "5"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Manedslonn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          5
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df2 = dataset.write('dataframe')
df2['measuresex'] = df['measuring method']+df['sex']
df2_new = df2.pivot(index='year', columns='measuresex',values='value')
df2_new.rename(columns={"AverageFemales": "AverageFemalesFullTime", "AverageMales": "AverageMalesFullTime", 'MedianFemales': 'MedianFemalesFullTime', 'MedianMales': 'MedianMalesFullTime'}, inplace=True)
df3_new = pd.concat([df_new, df2_new],axis=1)
df3_new['Difference average'] = (df3_new['AverageFemales']/df3_new['AverageMales'])*100
df3_new['Difference median'] = (df3_new['MedianFemales']/df3_new['MedianMales'])*100
df3_new['Difference average fulltime'] = (df3_new['AverageFemalesFullTime']/df3_new['AverageMalesFullTime']*100)
df3_new['Difference median fulltime'] = (df3_new['MedianFemalesFullTime']/df3_new['MedianMalesFullTime']*100)
df4_new = df3_new.filter(['Difference average', 'Difference average fulltime', 'Difference median', 'Difference median fulltime'], axis=1)
df4_new.to_csv('data_EN/SSB_earningswages_women_men_gap_change.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y') + '. FT: Full-time workers.'
title_date=str(df.iloc[4,6])
date_string = ' Womens monthly earnings as share of mens earnings by different measurements.' 
#Update DW
chartid = 'VlBm7'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Median montly earnings between women and men occupation 4 level xaCxb 
ssburl = 'https://data.ssb.no/api/v0/en/table/11418/'
query = {
  "query": [
    {
      "code": "MaaleMetode",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "10"
        ]
      }
    },
    {
      "code": "Yrke",
      "selection": {
        "filter": "vs:NYK08Lonnansatt",
        "values": [
          "0000",
          "0110",
          "0210",
          "0310",
          "1111",
          "1112",
          "1114",
          "1120",
          "1211",
          "1212",
          "1213",
          "1219",
          "1221",
          "1222",
          "1223",
          "1311",
          "1312",
          "1321",
          "1322",
          "1323",
          "1324",
          "1330",
          "1341",
          "1342",
          "1343",
          "1344",
          "1345",
          "1346",
          "1349",
          "1411",
          "1412",
          "1420",
          "1431",
          "1439",
          "2111",
          "2112",
          "2113",
          "2114",
          "2120",
          "2131",
          "2132",
          "2133",
          "2141",
          "2142",
          "2143",
          "2144",
          "2145",
          "2146",
          "2149",
          "2151",
          "2152",
          "2153",
          "2161",
          "2162",
          "2163",
          "2164",
          "2165",
          "2166",
          "2211",
          "2212",
          "2221",
          "2222",
          "2223",
          "2224",
          "2250",
          "2261",
          "2262",
          "2263",
          "2264",
          "2265",
          "2266",
          "2267",
          "2269",
          "2310",
          "2320",
          "2330",
          "2341",
          "2342",
          "2351",
          "2352",
          "2353",
          "2354",
          "2355",
          "2356",
          "2359",
          "2411",
          "2412",
          "2413",
          "2421",
          "2422",
          "2423",
          "2424",
          "2431",
          "2432",
          "2433",
          "2434",
          "2511",
          "2512",
          "2513",
          "2514",
          "2519",
          "2521",
          "2522",
          "2523",
          "2529",
          "2611",
          "2612",
          "2619",
          "2621",
          "2622",
          "2631",
          "2632",
          "2633",
          "2634",
          "2635",
          "2636",
          "2641",
          "2642",
          "2643",
          "2651",
          "2652",
          "2653",
          "2654",
          "2655",
          "2656",
          "2659",
          "3112",
          "3113",
          "3114",
          "3115",
          "3116",
          "3117",
          "3118",
          "3119",
          "3121",
          "3122",
          "3123",
          "3131",
          "3132",
          "3133",
          "3134",
          "3135",
          "3139",
          "3141",
          "3142",
          "3143",
          "3151",
          "3152",
          "3153",
          "3154",
          "3155",
          "3211",
          "3212",
          "3213",
          "3214",
          "3230",
          "3240",
          "3251",
          "3254",
          "3256",
          "3257",
          "3258",
          "3259",
          "3311",
          "3312",
          "3313",
          "3315",
          "3321",
          "3322",
          "3323",
          "3324",
          "3331",
          "3332",
          "3333",
          "3334",
          "3339",
          "3341",
          "3342",
          "3343",
          "3351",
          "3352",
          "3353",
          "3354",
          "3355",
          "3359",
          "3411",
          "3412",
          "3413",
          "3421",
          "3422",
          "3423",
          "3431",
          "3432",
          "3433",
          "3434",
          "3439",
          "3511",
          "3512",
          "3513",
          "3514",
          "3521",
          "3522",
          "4110",
          "4131",
          "4132",
          "4211",
          "4212",
          "4213",
          "4214",
          "4221",
          "4222",
          "4223",
          "4224",
          "4225",
          "4226",
          "4227",
          "4229",
          "4311",
          "4312",
          "4313",
          "4321",
          "4322",
          "4323",
          "4411",
          "4412",
          "4413",
          "4415",
          "4416",
          "5111",
          "5112",
          "5113",
          "5120",
          "5131",
          "5132",
          "5141",
          "5142",
          "5151",
          "5152",
          "5153",
          "5161",
          "5163",
          "5164",
          "5165",
          "5169",
          "5211",
          "5212",
          "5221",
          "5222",
          "5223",
          "5230",
          "5241",
          "5242",
          "5243",
          "5244",
          "5245",
          "5246",
          "5249",
          "5311",
          "5312",
          "5321",
          "5322",
          "5329",
          "5411",
          "5413",
          "5414",
          "5419",
          "6111",
          "6112",
          "6113",
          "6114",
          "6121",
          "6122",
          "6123",
          "6129",
          "6130",
          "6210",
          "6221",
          "6222",
          "6224",
          "7112",
          "7113",
          "7114",
          "7115",
          "7119",
          "7121",
          "7122",
          "7123",
          "7124",
          "7125",
          "7126",
          "7127",
          "7131",
          "7132",
          "7133",
          "7211",
          "7212",
          "7213",
          "7214",
          "7215",
          "7221",
          "7222",
          "7223",
          "7224",
          "7231",
          "7232",
          "7233",
          "7234",
          "7311",
          "7312",
          "7313",
          "7314",
          "7315",
          "7316",
          "7317",
          "7318",
          "7319",
          "7321",
          "7322",
          "7323",
          "7411",
          "7412",
          "7413",
          "7421",
          "7422",
          "7511",
          "7512",
          "7513",
          "7514",
          "7515",
          "7522",
          "7531",
          "7532",
          "7534",
          "7535",
          "7536",
          "7541",
          "7542",
          "7543",
          "7544",
          "7549",
          "8111",
          "8112",
          "8113",
          "8114",
          "8121",
          "8122",
          "8131",
          "8132",
          "8141",
          "8142",
          "8143",
          "8151",
          "8152",
          "8153",
          "8154",
          "8155",
          "8156",
          "8157",
          "8159",
          "8160",
          "8171",
          "8172",
          "8181",
          "8182",
          "8183",
          "8189",
          "8211",
          "8212",
          "8219",
          "8311",
          "8312",
          "8322",
          "8331",
          "8332",
          "8341",
          "8342",
          "8343",
          "8344",
          "8350",
          "9111",
          "9112",
          "9122",
          "9123",
          "9129",
          "9211",
          "9212",
          "9213",
          "9214",
          "9215",
          "9216",
          "9311",
          "9312",
          "9313",
          "9321",
          "9329",
          "9331",
          "9333",
          "9334",
          "9412",
          "9510",
          "9611",
          "9612",
          "9613",
          "9621",
          "9622",
          "9623",
          "9629"
        ]
      }
    },
    {
      "code": "Sektor",
      "selection": {
        "filter": "item",
        "values": [
          "ALLE"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "1",
          "2"
        ]
      }
    },
    {
      "code": "AvtaltVanlig",
      "selection": {
        "filter": "item",
        "values": [
          "5"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Manedslonn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          1
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df['measuresex'] = df['measuring method']+df['sex']
df_new = df.pivot(index='occupation', columns='measuresex', values='value')
df_new['Women as share of men']=df_new['MedianFemales']/df_new['MedianMales']*100
df_new['Women difference to men']=df_new['MedianFemales']-df_new['MedianMales']
df_new['Share women'] = df_new['Number of eployments with earningsFemales']/(df_new['Number of eployments with earningsFemales']+df_new['Number of eployments with earningsMales'])*100
df_new.dropna(inplace=True)
df_new.rename(columns={'MedianFemales': 'Women','MedianMales': 'Men',}, inplace=True)
df_new.to_csv('data_EN/SSB_earningswages_women_men_occupation_median.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
title_date = str(df.iloc[0,6])
date_string = 'Measured by median monthly earnings. Full-time employees.' + ' Data for ' + title_date +'.'
#Update DW
chartid = 'xaCxb'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
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

#Earnings differences by level of education and industry 
ssburl = 'https://data.ssb.no/api/v0/en/table/11420/'
query = {
  "query": [
    {
      "code": "MaaleMetode",
      "selection": {
        "filter": "item",
        "values": [
          "02"
        ]
      }
    },
    {
      "code": "Sektor",
      "selection": {
        "filter": "item",
        "values": [
          "ALLE"
        ]
      }
    },
    {
      "code": "UtdanNivaa",
      "selection": {
        "filter": "item",
        "values": [
          "Ialt",
          "1-2",
          "3-5",
          "6",
          "7-8"
        ]
      }
    },
    {
      "code": "NACE2007",
      "selection": {
        "filter": "vs:NACELonnalle03",
        "values": [
          "01-03",
          "05-09",
          "10-33",
          "35-39",
          "41-43",
          "45-47",
          "49-53",
          "55-56",
          "58-63",
          "64-66",
          "68-75",
          "77-82",
          "84",
          "85",
          "86-88",
          "90-99",
          "00.0"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0"
        ]
      }
    },
    {
      "code": "ArbeidsTid",
      "selection": {
        "filter": "item",
        "values": [
          "5"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Manedslonn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          1
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='industry (SIC2007)', columns='level of education', values='value')
df_new = df_new.reindex(columns=['Total', 'Primary and lower secondary education', 'Upper secondary education', 'First stage of tertiary education, undergraduate level', 'First and second stage of tertiary education, graduate and postgraduate level'])
df_new.rename(columns={'Primary and lower secondary education':'Primary and lower secondary','Upper secondary education': 'Upper secondary education','First stage of tertiary education, undergraduate level':'Undergraduate', 'First and second stage of tertiary education, graduate and postgraduate level':'Postgraduate'}, inplace=True)
df_new.drop(('Unspecified'), inplace=True)
df_new.to_csv('data_EN/SSB_earningswages_education_industry.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y')
title_date = str(df.iloc[0,7])
date_string = 'Average monthly earnings (in NOK) for full-time employees in different industries by educational level.' + ' Data for ' + title_date +'.'
#Update DW
chartid = '9Kuoo'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
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

#Update DW
chartid = 'TDVPk'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
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

#Earnings differences municipalities maps LVMUL (living) and A7rRr (work) + table fU8iJ
ssburl = 'https://data.ssb.no/api/v0/en/table/12852/'
query = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:KommunerFastIkkeFast",
        "values": [
          "3001",
          "3002",
          "3003",
          "3004",
          "3005",
          "3006",
          "3007",
          "3011",
          "3012",
          "3013",
          "3014",
          "3015",
          "3016",
          "3017",
          "3018",
          "3019",
          "3020",
          "3021",
          "3022",
          "3023",
          "3024",
          "3025",
          "3026",
          "3027",
          "3028",
          "3029",
          "3030",
          "3031",
          "3032",
          "3033",
          "3034",
          "3035",
          "3036",
          "3037",
          "3038",
          "3039",
          "3040",
          "3041",
          "3042",
          "3043",
          "3044",
          "3045",
          "3046",
          "3047",
          "3048",
          "3049",
          "3050",
          "3051",
          "3052",
          "3053",
          "3054",
          "0101",
          "0104",
          "0105",
          "0106",
          "0111",
          "0118",
          "0119",
          "0121",
          "0122",
          "0123",
          "0124",
          "0125",
          "0127",
          "0128",
          "0135",
          "0136",
          "0137",
          "0138",
          "0199",
          "0211",
          "0213",
          "0214",
          "0215",
          "0216",
          "0217",
          "0219",
          "0220",
          "0221",
          "0226",
          "0227",
          "0228",
          "0229",
          "0230",
          "0231",
          "0233",
          "0234",
          "0235",
          "0236",
          "0237",
          "0238",
          "0239",
          "0299",
          "0602",
          "0604",
          "0605",
          "0612",
          "0615",
          "0616",
          "0617",
          "0618",
          "0619",
          "0620",
          "0621",
          "0622",
          "0623",
          "0624",
          "0625",
          "0626",
          "0627",
          "0628",
          "0631",
          "0632",
          "0633",
          "0301",
          "0399",
          "3401",
          "3403",
          "3405",
          "3407",
          "3411",
          "3412",
          "3413",
          "3414",
          "3415",
          "3416",
          "3417",
          "3418",
          "3419",
          "3420",
          "3421",
          "3422",
          "3423",
          "3424",
          "3425",
          "3426",
          "3427",
          "3428",
          "3429",
          "3430",
          "3431",
          "3432",
          "3433",
          "3434",
          "3435",
          "3436",
          "3437",
          "3438",
          "3439",
          "3440",
          "3441",
          "3442",
          "3443",
          "3446",
          "3447",
          "3448",
          "3449",
          "3450",
          "3451",
          "3452",
          "3453",
          "3454",
          "0402",
          "0403",
          "0412",
          "0415",
          "0417",
          "0418",
          "0419",
          "0420",
          "0423",
          "0425",
          "0426",
          "0427",
          "0428",
          "0429",
          "0430",
          "0432",
          "0434",
          "0436",
          "0437",
          "0438",
          "0439",
          "0441",
          "0499",
          "0501",
          "0502",
          "0511",
          "0512",
          "0513",
          "0514",
          "0515",
          "0516",
          "0517",
          "0519",
          "0520",
          "0521",
          "0522",
          "0528",
          "0529",
          "0532",
          "0533",
          "0534",
          "0536",
          "0538",
          "0540",
          "0541",
          "0542",
          "0543",
          "0544",
          "0545",
          "0599",
          "3801",
          "3802",
          "3803",
          "3804",
          "3805",
          "3806",
          "3807",
          "3808",
          "3811",
          "3812",
          "3813",
          "3814",
          "3815",
          "3816",
          "3817",
          "3818",
          "3819",
          "3820",
          "3821",
          "3822",
          "3823",
          "3824",
          "3825",
          "0699",
          "0701",
          "0702",
          "0704",
          "0706",
          "0709",
          "0710",
          "0711",
          "0712",
          "0713",
          "0714",
          "0715",
          "0716",
          "0719",
          "0720",
          "0722",
          "0723",
          "0728",
          "0729",
          "0799",
          "0805",
          "0806",
          "0807",
          "0811",
          "0814",
          "0815",
          "0817",
          "0819",
          "0821",
          "0822",
          "0826",
          "0827",
          "0828",
          "0829",
          "0830",
          "0831",
          "0833",
          "0834",
          "0899",
          "4201",
          "4202",
          "4203",
          "4204",
          "4205",
          "4206",
          "4207",
          "4211",
          "4212",
          "4213",
          "4214",
          "4215",
          "4216",
          "4217",
          "4218",
          "4219",
          "4220",
          "4221",
          "4222",
          "4223",
          "4224",
          "4225",
          "4226",
          "4227",
          "4228",
          "0901",
          "0904",
          "0906",
          "0911",
          "0912",
          "0914",
          "0919",
          "0926",
          "0928",
          "0929",
          "0935",
          "0937",
          "0938",
          "0940",
          "0941",
          "0999",
          "1001",
          "1002",
          "1003",
          "1004",
          "1014",
          "1017",
          "1018",
          "1021",
          "1026",
          "1027",
          "1029",
          "1032",
          "1034",
          "1037",
          "1046",
          "1099",
          "1101",
          "1102",
          "1103",
          "1106",
          "1108",
          "1111",
          "1112",
          "1114",
          "1119",
          "1120",
          "1121",
          "1122",
          "1124",
          "1127",
          "1129",
          "1130",
          "1133",
          "1134",
          "1135",
          "1141",
          "1142",
          "1144",
          "1145",
          "1146",
          "1149",
          "1151",
          "1154",
          "1159",
          "1160",
          "1199",
          "4601",
          "4602",
          "4611",
          "4612",
          "4613",
          "4614",
          "4615",
          "4616",
          "4617",
          "4618",
          "4619",
          "4620",
          "4621",
          "4622",
          "4623",
          "4624",
          "4625",
          "4626",
          "4627",
          "4628",
          "4629",
          "4630",
          "4631",
          "4632",
          "4633",
          "4634",
          "4635",
          "4636",
          "4637",
          "4638",
          "4639",
          "4640",
          "4641",
          "4642",
          "4643",
          "4644",
          "4645",
          "4646",
          "4647",
          "4648",
          "4649",
          "4650",
          "4651",
          "1201",
          "1211",
          "1216",
          "1219",
          "1221",
          "1222",
          "1223",
          "1224",
          "1227",
          "1228",
          "1231",
          "1232",
          "1233",
          "1234",
          "1235",
          "1238",
          "1241",
          "1242",
          "1243",
          "1244",
          "1245",
          "1246",
          "1247",
          "1251",
          "1252",
          "1253",
          "1256",
          "1259",
          "1260",
          "1263",
          "1264",
          "1265",
          "1266",
          "1299",
          "1401",
          "1411",
          "1412",
          "1413",
          "1416",
          "1417",
          "1418",
          "1419",
          "1420",
          "1421",
          "1422",
          "1424",
          "1426",
          "1428",
          "1429",
          "1430",
          "1431",
          "1432",
          "1433",
          "1438",
          "1439",
          "1441",
          "1443",
          "1444",
          "1445",
          "1449",
          "1499",
          "1502",
          "1503",
          "1504",
          "1505",
          "1506",
          "1507",
          "1511",
          "1514",
          "1515",
          "1516",
          "1517",
          "1519",
          "1520",
          "1523",
          "1524",
          "1525",
          "1526",
          "1528",
          "1529",
          "1531",
          "1532",
          "1534",
          "1535",
          "1539",
          "1543",
          "1545",
          "1546",
          "1547",
          "1548",
          "1551",
          "1554",
          "1556",
          "1557",
          "1560",
          "1563",
          "1566",
          "1567",
          "1569",
          "1571",
          "1572",
          "1573",
          "1576",
          "1577",
          "1578",
          "1579",
          "1599",
          "5001",
          "5004",
          "5005",
          "5006",
          "5007",
          "5011",
          "5012",
          "5013",
          "5014",
          "5015",
          "5016",
          "5017",
          "5018",
          "5019",
          "5020",
          "5021",
          "5022",
          "5023",
          "5024",
          "5025",
          "5026",
          "5027",
          "5028",
          "5029",
          "5030",
          "5031",
          "5032",
          "5033",
          "5034",
          "5035",
          "5036",
          "5037",
          "5038",
          "5039",
          "5040",
          "5041",
          "5042",
          "5043",
          "5044",
          "5045",
          "5046",
          "5047",
          "5048",
          "5049",
          "5050",
          "5051",
          "5052",
          "5053",
          "5054",
          "5055",
          "5056",
          "5057",
          "5058",
          "5059",
          "5060",
          "5061",
          "5099",
          "1601",
          "1612",
          "1613",
          "1617",
          "1620",
          "1621",
          "1622",
          "1624",
          "1627",
          "1630",
          "1632",
          "1633",
          "1634",
          "1635",
          "1636",
          "1638",
          "1640",
          "1644",
          "1648",
          "1653",
          "1657",
          "1662",
          "1663",
          "1664",
          "1665",
          "1699",
          "1702",
          "1703",
          "1711",
          "1714",
          "1717",
          "1718",
          "1719",
          "1721",
          "1723",
          "1724",
          "1725",
          "1729",
          "1736",
          "1738",
          "1739",
          "1740",
          "1742",
          "1743",
          "1744",
          "1748",
          "1749",
          "1750",
          "1751",
          "1755",
          "1756",
          "1799",
          "1804",
          "1805",
          "1806",
          "1811",
          "1812",
          "1813",
          "1815",
          "1816",
          "1818",
          "1820",
          "1822",
          "1824",
          "1825",
          "1826",
          "1827",
          "1828",
          "1832",
          "1833",
          "1834",
          "1835",
          "1836",
          "1837",
          "1838",
          "1839",
          "1840",
          "1841",
          "1842",
          "1845",
          "1848",
          "1849",
          "1850",
          "1851",
          "1852",
          "1853",
          "1854",
          "1856",
          "1857",
          "1859",
          "1860",
          "1865",
          "1866",
          "1867",
          "1868",
          "1870",
          "1871",
          "1874",
          "1875",
          "1899",
          "5401",
          "5402",
          "5403",
          "5404",
          "5405",
          "5406",
          "5411",
          "5412",
          "5413",
          "5414",
          "5415",
          "5416",
          "5417",
          "5418",
          "5419",
          "5420",
          "5421",
          "5422",
          "5423",
          "5424",
          "5425",
          "5426",
          "5427",
          "5428",
          "5429",
          "5430",
          "5432",
          "5433",
          "5434",
          "5435",
          "5436",
          "5437",
          "5438",
          "5439",
          "5440",
          "5441",
          "5442",
          "5443",
          "5444",
          "1901",
          "1902",
          "1903",
          "1911",
          "1913",
          "1915",
          "1917",
          "1919",
          "1920",
          "1922",
          "1923",
          "1924",
          "1925",
          "1926",
          "1927",
          "1928",
          "1929",
          "1931",
          "1933",
          "1936",
          "1938",
          "1939",
          "1940",
          "1941",
          "1942",
          "1943",
          "1999",
          "2002",
          "2003",
          "2004",
          "2011",
          "2012",
          "2014",
          "2015",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",
          "2022",
          "2023",
          "2024",
          "2025",
          "2027",
          "2028",
          "2030",
          "2099",
          "9999",
          "9999g"
        ]
      }
    },
    {
      "code": "MaaleMetode",
      "selection": {
        "filter": "item",
        "values": [
          "01"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "0"
        ]
      }
    },
    {
      "code": "Alder",
      "selection": {
        "filter": "item",
        "values": [
          "Ialt"
        ]
      }
    },
    {
      "code": "AvtaltVanlig",
      "selection": {
        "filter": "item",
        "values": [
          "5"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Manedslonn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          1
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='region', columns='place of work and place of residence', values='value')
df_new.dropna(inplace=True)
df_new.rename(index={'Bø (Nordland)':'Bø'},inplace=True)
df_new.rename(index={'Deatnu - Tana':'Tana'},inplace=True)
df_new.rename(index={'Evenes - Evenássi':'Evenes'},inplace=True)
df_new.rename(index={'Fauske - Fuosko':'Fauske'},inplace=True)
df_new.rename(index={'Guovdageaidnu - Kautokeino':'Kautokeino'},inplace=True)
df_new.rename(index={'Gáivuotna - Kåfjord - Kaivuono':'Kåfjord'},inplace=True)
df_new.rename(index={'Herøy (Møre og Romsdal)':'Herøy – Møre og Romsdal'},inplace=True)
df_new.rename(index={'Herøy (Nordland)':'Herøy – Nordland'},inplace=True)
df_new.rename(index={'Kárásjohka - Karasjok':'Karasjok'},inplace=True)
df_new.rename(index={'Loabák - Lavangen':'Lavangen'},inplace=True)
df_new.rename(index={'Oslo municipality':'Oslo'},inplace=True)
df_new.rename(index={'Porsanger - Porsángu - Porsanki ':'Porsanger'},inplace=True)
df_new.rename(index={'Raarvihke - Røyrvik':'Røyrvik'},inplace=True)
df_new.rename(index={'Sande (Møre og Romsdal)':'Sande'},inplace=True)
df_new.rename(index={'Sortland - Suortá':'Sortland'},inplace=True)
df_new.rename(index={'Snåase - Snåsa':'Snåsa'},inplace=True)
df_new.rename(index={'Storfjord - Omasvuotna - Omasvuono':'Storfjord'},inplace=True)
df_new.rename(index={'Unjárga - Nesseby':'Nesseby'},inplace=True)
df_new.rename(index={'Våler (Innlandet)':'Våler – Innlandet'},inplace=True)
df_new.rename(index={'Våler (Viken)':'Våler – Viken'},inplace=True)
df_new.drop(index={'Not mainland Norway'}, inplace=True)
df_new.to_csv('data_EN/SSB_earningswages_municipalities_median.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last updated: ' + parsed_date.strftime ('%d/%m/%y')
title_date = str(df.iloc[0,7])
date_string= 'Median earnings (in NOK). Data for ' + title_date + '.' + ' Full-time employees.'
date_string2= 'Median earnings (in NOK) of those who work in the region. Data for ' + title_date + '.' + ' Full-time employees.'
date_string3= 'Median earnings (in NOK) of those who live in the region. Data for ' + title_date +'.'+ ' Full-time employees.'

#Update DW
chartid = 'fU8iJ'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
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

#Update DW
chartid = 'A7rRr'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string2}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Update DW
chartid = 'LVMUL'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string3}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Earnings differences women and men by municipality 1Bl4h
ssburl = 'https://data.ssb.no/api/v0/en/table/12852/'
query = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:KommunerFastIkkeFast",
        "values": [
          "3001",
          "3002",
          "3003",
          "3004",
          "3005",
          "3006",
          "3007",
          "3011",
          "3012",
          "3013",
          "3014",
          "3015",
          "3016",
          "3017",
          "3018",
          "3019",
          "3020",
          "3021",
          "3022",
          "3023",
          "3024",
          "3025",
          "3026",
          "3027",
          "3028",
          "3029",
          "3030",
          "3031",
          "3032",
          "3033",
          "3034",
          "3035",
          "3036",
          "3037",
          "3038",
          "3039",
          "3040",
          "3041",
          "3042",
          "3043",
          "3044",
          "3045",
          "3046",
          "3047",
          "3048",
          "3049",
          "3050",
          "3051",
          "3052",
          "3053",
          "3054",
          "0101",
          "0104",
          "0105",
          "0106",
          "0111",
          "0118",
          "0119",
          "0121",
          "0122",
          "0123",
          "0124",
          "0125",
          "0127",
          "0128",
          "0135",
          "0136",
          "0137",
          "0138",
          "0199",
          "0211",
          "0213",
          "0214",
          "0215",
          "0216",
          "0217",
          "0219",
          "0220",
          "0221",
          "0226",
          "0227",
          "0228",
          "0229",
          "0230",
          "0231",
          "0233",
          "0234",
          "0235",
          "0236",
          "0237",
          "0238",
          "0239",
          "0299",
          "0602",
          "0604",
          "0605",
          "0612",
          "0615",
          "0616",
          "0617",
          "0618",
          "0619",
          "0620",
          "0621",
          "0622",
          "0623",
          "0624",
          "0625",
          "0626",
          "0627",
          "0628",
          "0631",
          "0632",
          "0633",
          "0301",
          "0399",
          "3401",
          "3403",
          "3405",
          "3407",
          "3411",
          "3412",
          "3413",
          "3414",
          "3415",
          "3416",
          "3417",
          "3418",
          "3419",
          "3420",
          "3421",
          "3422",
          "3423",
          "3424",
          "3425",
          "3426",
          "3427",
          "3428",
          "3429",
          "3430",
          "3431",
          "3432",
          "3433",
          "3434",
          "3435",
          "3436",
          "3437",
          "3438",
          "3439",
          "3440",
          "3441",
          "3442",
          "3443",
          "3446",
          "3447",
          "3448",
          "3449",
          "3450",
          "3451",
          "3452",
          "3453",
          "3454",
          "0402",
          "0403",
          "0412",
          "0415",
          "0417",
          "0418",
          "0419",
          "0420",
          "0423",
          "0425",
          "0426",
          "0427",
          "0428",
          "0429",
          "0430",
          "0432",
          "0434",
          "0436",
          "0437",
          "0438",
          "0439",
          "0441",
          "0499",
          "0501",
          "0502",
          "0511",
          "0512",
          "0513",
          "0514",
          "0515",
          "0516",
          "0517",
          "0519",
          "0520",
          "0521",
          "0522",
          "0528",
          "0529",
          "0532",
          "0533",
          "0534",
          "0536",
          "0538",
          "0540",
          "0541",
          "0542",
          "0543",
          "0544",
          "0545",
          "0599",
          "3801",
          "3802",
          "3803",
          "3804",
          "3805",
          "3806",
          "3807",
          "3808",
          "3811",
          "3812",
          "3813",
          "3814",
          "3815",
          "3816",
          "3817",
          "3818",
          "3819",
          "3820",
          "3821",
          "3822",
          "3823",
          "3824",
          "3825",
          "0699",
          "0701",
          "0702",
          "0704",
          "0706",
          "0709",
          "0710",
          "0711",
          "0712",
          "0713",
          "0714",
          "0715",
          "0716",
          "0719",
          "0720",
          "0722",
          "0723",
          "0728",
          "0729",
          "0799",
          "0805",
          "0806",
          "0807",
          "0811",
          "0814",
          "0815",
          "0817",
          "0819",
          "0821",
          "0822",
          "0826",
          "0827",
          "0828",
          "0829",
          "0830",
          "0831",
          "0833",
          "0834",
          "0899",
          "4201",
          "4202",
          "4203",
          "4204",
          "4205",
          "4206",
          "4207",
          "4211",
          "4212",
          "4213",
          "4214",
          "4215",
          "4216",
          "4217",
          "4218",
          "4219",
          "4220",
          "4221",
          "4222",
          "4223",
          "4224",
          "4225",
          "4226",
          "4227",
          "4228",
          "0901",
          "0904",
          "0906",
          "0911",
          "0912",
          "0914",
          "0919",
          "0926",
          "0928",
          "0929",
          "0935",
          "0937",
          "0938",
          "0940",
          "0941",
          "0999",
          "1001",
          "1002",
          "1003",
          "1004",
          "1014",
          "1017",
          "1018",
          "1021",
          "1026",
          "1027",
          "1029",
          "1032",
          "1034",
          "1037",
          "1046",
          "1099",
          "1101",
          "1102",
          "1103",
          "1106",
          "1108",
          "1111",
          "1112",
          "1114",
          "1119",
          "1120",
          "1121",
          "1122",
          "1124",
          "1127",
          "1129",
          "1130",
          "1133",
          "1134",
          "1135",
          "1141",
          "1142",
          "1144",
          "1145",
          "1146",
          "1149",
          "1151",
          "1154",
          "1159",
          "1160",
          "1199",
          "4601",
          "4602",
          "4611",
          "4612",
          "4613",
          "4614",
          "4615",
          "4616",
          "4617",
          "4618",
          "4619",
          "4620",
          "4621",
          "4622",
          "4623",
          "4624",
          "4625",
          "4626",
          "4627",
          "4628",
          "4629",
          "4630",
          "4631",
          "4632",
          "4633",
          "4634",
          "4635",
          "4636",
          "4637",
          "4638",
          "4639",
          "4640",
          "4641",
          "4642",
          "4643",
          "4644",
          "4645",
          "4646",
          "4647",
          "4648",
          "4649",
          "4650",
          "4651",
          "1201",
          "1211",
          "1216",
          "1219",
          "1221",
          "1222",
          "1223",
          "1224",
          "1227",
          "1228",
          "1231",
          "1232",
          "1233",
          "1234",
          "1235",
          "1238",
          "1241",
          "1242",
          "1243",
          "1244",
          "1245",
          "1246",
          "1247",
          "1251",
          "1252",
          "1253",
          "1256",
          "1259",
          "1260",
          "1263",
          "1264",
          "1265",
          "1266",
          "1299",
          "1401",
          "1411",
          "1412",
          "1413",
          "1416",
          "1417",
          "1418",
          "1419",
          "1420",
          "1421",
          "1422",
          "1424",
          "1426",
          "1428",
          "1429",
          "1430",
          "1431",
          "1432",
          "1433",
          "1438",
          "1439",
          "1441",
          "1443",
          "1444",
          "1445",
          "1449",
          "1499",
          "1502",
          "1503",
          "1504",
          "1505",
          "1506",
          "1507",
          "1511",
          "1514",
          "1515",
          "1516",
          "1517",
          "1519",
          "1520",
          "1523",
          "1524",
          "1525",
          "1526",
          "1528",
          "1529",
          "1531",
          "1532",
          "1534",
          "1535",
          "1539",
          "1543",
          "1545",
          "1546",
          "1547",
          "1548",
          "1551",
          "1554",
          "1556",
          "1557",
          "1560",
          "1563",
          "1566",
          "1567",
          "1569",
          "1571",
          "1572",
          "1573",
          "1576",
          "1577",
          "1578",
          "1579",
          "1599",
          "5001",
          "5004",
          "5005",
          "5006",
          "5007",
          "5011",
          "5012",
          "5013",
          "5014",
          "5015",
          "5016",
          "5017",
          "5018",
          "5019",
          "5020",
          "5021",
          "5022",
          "5023",
          "5024",
          "5025",
          "5026",
          "5027",
          "5028",
          "5029",
          "5030",
          "5031",
          "5032",
          "5033",
          "5034",
          "5035",
          "5036",
          "5037",
          "5038",
          "5039",
          "5040",
          "5041",
          "5042",
          "5043",
          "5044",
          "5045",
          "5046",
          "5047",
          "5048",
          "5049",
          "5050",
          "5051",
          "5052",
          "5053",
          "5054",
          "5055",
          "5056",
          "5057",
          "5058",
          "5059",
          "5060",
          "5061",
          "5099",
          "1601",
          "1612",
          "1613",
          "1617",
          "1620",
          "1621",
          "1622",
          "1624",
          "1627",
          "1630",
          "1632",
          "1633",
          "1634",
          "1635",
          "1636",
          "1638",
          "1640",
          "1644",
          "1648",
          "1653",
          "1657",
          "1662",
          "1663",
          "1664",
          "1665",
          "1699",
          "1702",
          "1703",
          "1711",
          "1714",
          "1717",
          "1718",
          "1719",
          "1721",
          "1723",
          "1724",
          "1725",
          "1729",
          "1736",
          "1738",
          "1739",
          "1740",
          "1742",
          "1743",
          "1744",
          "1748",
          "1749",
          "1750",
          "1751",
          "1755",
          "1756",
          "1799",
          "1804",
          "1805",
          "1806",
          "1811",
          "1812",
          "1813",
          "1815",
          "1816",
          "1818",
          "1820",
          "1822",
          "1824",
          "1825",
          "1826",
          "1827",
          "1828",
          "1832",
          "1833",
          "1834",
          "1835",
          "1836",
          "1837",
          "1838",
          "1839",
          "1840",
          "1841",
          "1842",
          "1845",
          "1848",
          "1849",
          "1850",
          "1851",
          "1852",
          "1853",
          "1854",
          "1856",
          "1857",
          "1859",
          "1860",
          "1865",
          "1866",
          "1867",
          "1868",
          "1870",
          "1871",
          "1874",
          "1875",
          "1899",
          "5401",
          "5402",
          "5403",
          "5404",
          "5405",
          "5406",
          "5411",
          "5412",
          "5413",
          "5414",
          "5415",
          "5416",
          "5417",
          "5418",
          "5419",
          "5420",
          "5421",
          "5422",
          "5423",
          "5424",
          "5425",
          "5426",
          "5427",
          "5428",
          "5429",
          "5430",
          "5432",
          "5433",
          "5434",
          "5435",
          "5436",
          "5437",
          "5438",
          "5439",
          "5440",
          "5441",
          "5442",
          "5443",
          "5444",
          "1901",
          "1902",
          "1903",
          "1911",
          "1913",
          "1915",
          "1917",
          "1919",
          "1920",
          "1922",
          "1923",
          "1924",
          "1925",
          "1926",
          "1927",
          "1928",
          "1929",
          "1931",
          "1933",
          "1936",
          "1938",
          "1939",
          "1940",
          "1941",
          "1942",
          "1943",
          "1999",
          "2002",
          "2003",
          "2004",
          "2011",
          "2012",
          "2014",
          "2015",
          "2017",
          "2018",
          "2019",
          "2020",
          "2021",
          "2022",
          "2023",
          "2024",
          "2025",
          "2027",
          "2028",
          "2030",
          "2099",
          "9999",
          "9999g"
        ]
      }
    },
    {
      "code": "MaaleMetode",
      "selection": {
        "filter": "item",
        "values": [
          "01"
        ]
      }
    },
    {
      "code": "ArbBostedRegion",
      "selection": {
        "filter": "item",
        "values": [
          "2"
        ]
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "1",
          "2"
        ]
      }
    },
    {
      "code": "Alder",
      "selection": {
        "filter": "item",
        "values": [
          "Ialt"
        ]
      }
    },
    {
      "code": "AvtaltVanlig",
      "selection": {
        "filter": "item",
        "values": [
          "5"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Manedslonn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          1
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='region', columns='sex', values='value')
df_new.dropna(inplace=True)
df_new.rename(index={'Bø (Nordland)':'Bø'},inplace=True)
df_new.rename(index={'Deatnu - Tana':'Tana'},inplace=True)
df_new.rename(index={'Evenes - Evenássi':'Evenes'},inplace=True)
df_new.rename(index={'Fauske - Fuosko':'Fauske'},inplace=True)
df_new.rename(index={'Guovdageaidnu - Kautokeino':'Kautokeino'},inplace=True)
df_new.rename(index={'Gáivuotna - Kåfjord - Kaivuono':'Kåfjord'},inplace=True)
df_new.rename(index={'Herøy (Møre og Romsdal)':'Herøy – Møre og Romsdal'},inplace=True)
df_new.rename(index={'Herøy (Nordland)':'Herøy – Nordland'},inplace=True)
df_new.rename(index={'Kárásjohka - Karasjok':'Karasjok'},inplace=True)
df_new.rename(index={'Loabák - Lavangen':'Lavangen'},inplace=True)
df_new.rename(index={'Oslo municipality':'Oslo'},inplace=True)
df_new.rename(index={'Porsanger - Porsángu - Porsanki ':'Porsanger'},inplace=True)
df_new.rename(index={'Raarvihke - Røyrvik':'Røyrvik'},inplace=True)
df_new.rename(index={'Sande (Møre og Romsdal)':'Sande'},inplace=True)
df_new.rename(index={'Sortland - Suortá':'Sortland'},inplace=True)
df_new.rename(index={'Snåase - Snåsa':'Snåsa'},inplace=True)
df_new.rename(index={'Storfjord - Omasvuotna - Omasvuono':'Storfjord'},inplace=True)
df_new.rename(index={'Unjárga - Nesseby':'Nesseby'},inplace=True)
df_new.rename(index={'Våler (Innlandet)':'Våler – Innlandet'},inplace=True)
df_new.rename(index={'Våler (Viken)':'Våler – Viken'},inplace=True)
df_new['Difference nominally'] = df_new['Females']-df_new['Males']
df_new['Difference share'] = df_new['Females']/df_new['Males']*100
df_new.drop(index={'Not mainland Norway'}, inplace=True)
df_new.to_csv('data_EN/SSB_earningswages_municipality_median_women_men.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data last published: ' + parsed_date.strftime ('%d/%m/%y') + '.'
title_date = str(df.iloc[0,7])
date_string = 'Median montly earnings of those who live in the region. Data for ' + title_date +'.' + ' Full-time employees.'
#Update DW
chartid = '1Bl4h'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
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
#***
