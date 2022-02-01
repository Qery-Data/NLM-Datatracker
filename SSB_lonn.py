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

#Lønnsutviklingen, nominelt og reallønn, 1R4EQ og k0DNV
ssburl = 'https://data.ssb.no/api/v0/no/table/09786/'
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
resultat = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='år', columns='statistikkvariabel',values='value')
ssburl = 'https://data.ssb.no/api/v0/no/table/11449/'
query = {
  "query": [
    {
      "code": "Maaned",
      "selection": {
        "filter": "item",
        "values": [
          "90"
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
resultat = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df2 = dataset.write('dataframe')
df2_new = df2.pivot(index='år', columns='statistikkvariabel',values='value')
ssburl = 'https://data.ssb.no/api/v0/no/table/03014/'
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
resultat = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df3 = dataset.write('dataframe')
df3_new = df3.pivot(index='år', columns='statistikkvariabel', values='value')
df4_new = pd.concat([df_new,df2_new.reindex(df_new.index),df3_new.reindex(df_new.index)], axis=1)
df4_new['Lønn*KPI']= df4_new['Årslønn, påløpt (1 000 kr)'] / (df4_new['Konsumprisindeks (1998=100)']/100)
df4_new['Lønn*KPI>2016']= df4_new['Årslønn, påløpt (1 000 kr)'] / (df4_new['Konsumprisindeks (2015=100)']/100)
df4_new['Real=<16'] = df4_new['Lønn*KPI'].pct_change(periods=1)*100
df4_new['Real>16'] = df4_new['Lønn*KPI>2016'].pct_change(periods=1)*100
df4_new.iat[47,6] = df4_new.iat[47,7]
df4_new.iat[48,6] = df4_new.iat[48,7]
df4_new.iat[49,6] = df4_new.iat[49,7]
df4_new.iat[50,6] = df4_new.iat[50,7]
df5_new = df4_new.iloc[:,[0,1,6]]
df5_new.rename(columns = {'Real=<16':'Reallønn. Endring fra året før i prosent'}, inplace=True)
df6_new = df5_new.iloc[30:]
df6_new.to_csv('data/SSB_lonn_aarligvekst_real_nominell.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
#Update DW
chartid = '1R4EQ'
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y') + '*Årlig vekst i nominell lønn (fra Nasjonalregnskapet). Inkluderer avtalt lønn, bonuser og uregelmessige tillegg, men ekslusiv overtidstillegg. '
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

chartid = 'k0DNV'
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y') + '*Reallønnsvekst er lønnsvekst fratrukket prisvekst. Positivt reallønnsvekst betyr at kjøpekraften økes.'
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

#Gjennomsnittlig lønn måned yrker nivå 4
ssburl = 'https://data.ssb.no/api/v0/no/table/11418/'
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
resultat = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='yrke',columns='år',values='value')
df_new['Endring sist år'] = (df_new.iloc[:,4]-df_new.iloc[:,3])/df_new.iloc[:,3]*100
df_new['Endring siste 5 år'] = (df_new.iloc[:,4]-df_new.iloc[:,0])/df_new.iloc[:,0]*100
df_new.dropna(inplace=True)
df_new.to_csv('data/SSB_lonn_yrke.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato=str(df_new.columns[4])
date_string = 'Gjennomsnittlig månedslønn. Tall for ' + dato +'.'
#Update DW
chartid = 'CTQph'
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

#Gjennomsnittlig lønn måned yrker nivå 4 sektor Gfff7
ssburl = 'https://data.ssb.no/api/v0/no/table/11418/'
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
          1
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
resultat = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='yrke',columns='sektor',values='value')
df_new.dropna(thresh=3, inplace=True)
df_new = df_new[['Sum alle sektorer','Privat sektor og offentlige eide foretak','Statsforvaltningen','Kommuneforvaltningen']]
df_new.to_csv('data/SSB_lonn_yrke_sektor.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y') + ' Tall ikke tilgjengelig for enkelte sektorer og yrkesgrupper på grunn av at tall ikke er mulig å oppgi eller det ikke kan vises på grunn av konfidensialitetshensyn.'
dato=str(df.iloc[0,6])
date_string = 'Gjennomsnittlig månedslønn etter sektor. Tall for ' + dato +' fordelt på statsforvaltningen, kommuneforvaltningen og privat sektor (inklusive offentlig eide foretak.)'
#Update DW
chartid = 'Gfff7'
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

#Lønnsforskjeller kjønn sist år gjennomsnitt og median RFHWE
ssburl = 'https://data.ssb.no/api/v0/no/table/11418/'
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
resultat = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df = dataset.write('dataframe')
df['målarbeidstid']=df['statistikkmål']+df['avtalt/vanlig arbeidstid per uke']
df_new=df.pivot(index='målarbeidstid', columns='kjønn', values='value')
df_new = df_new.reindex(['GjennomsnittI alt','GjennomsnittHeltidsansatte','MedianI alt','MedianHeltidsansatte'])
df_new.to_csv('data/SSB_lonn_kjonn_sistaar.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato=str(df.iloc[0,6])
date_string = 'Tall for ' + dato +'.' + ' Etter ulike beregningsmåter:'
#Update DW
chartid = 'RFHWE'
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

#Lønnsutviklingen kvinner og menn gjennomsnitt og median GDCRK (graf) og Fn94r (tabell)
ssburl = 'https://data.ssb.no/api/v0/no/table/11418/'
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
resultat = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df = dataset.write('dataframe')
df['målkjønn'] = df['statistikkmål']+df['kjønn']
df_new = df.pivot(index='år', columns='målkjønn',values='value')

#heltidsansatte 
ssburl = 'https://data.ssb.no/api/v0/no/table/11418/'
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
        "filter": "item",
        "values": [
          "2016",
          "2017",
          "2018",
          "2019",
          "2020"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
resultat = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df2 = dataset.write('dataframe')
df2['målkjønn'] = df['statistikkmål']+df['kjønn']
df2_new = df2.pivot(index='år', columns='målkjønn',values='value')
df2_new.rename(columns={"GjennomsnittKvinner": "GjennomsnittKvinnerHeltid", "GjennomsnittMenn": "GjennomsnittMennHeltid", 'MedianKvinner': 'MedianKvinnerHeltid', 'MedianMenn': 'MedianMennHeltid'}, inplace=True)
df3_new = pd.concat([df_new, df2_new],axis=1)
df3_new['Forskjell gjennomsnitt'] = (df3_new['GjennomsnittKvinner']/df3_new['GjennomsnittMenn'])*100
df3_new['Forskjell median'] = (df3_new['MedianKvinner']/df3_new['MedianMenn'])*100
df3_new['Forskjell gjennomsnittHeltid'] = (df3_new['GjennomsnittKvinnerHeltid']/df3_new['GjennomsnittMennHeltid']*100)
df3_new['Forskjell medianHeltid'] = (df3_new['MedianKvinnerHeltid']/df3_new['MedianMennHeltid']*100)
df4_new = df3_new.filter(['Forskjell gjennomsnitt', 'Forskjell gjennomsnittHeltid', 'Forskjell median', 'Forskjell medianHeltid'], axis=1)
df4_new.to_csv('data/SSB_lonn_kjonn_andel_utvikling.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato=str(df.iloc[4,6])
date_string = 'Tall for ' + dato +'.' + ' Kvinners lønn som andel av menns lønn etter ulike beregningsmåter:'
#Update DW 1
chartid = 'Fn94r'
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
#Endring 
df5_new = df4_new.transpose()
df5_new['Sist år'] = df5_new.iloc[:,4]
df5_new['Endring sist år'] = df5_new.iloc[:,4]-df5_new.iloc[:,3]
df5_new['Endring siste fem år'] = df5_new.iloc[:,4]-df5_new.iloc[:,0]
df5_new.to_csv('data/SSB_lonn_kjonn_andel_sistaar_endring.csv', index=True)
#Update DW
chartid = 'GDCRK'
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


#Median månedslønn kvinner og menn yrker 4 sifret liYSg
ssburl = 'https://data.ssb.no/api/v0/no/table/11418/'
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
          1
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
resultat = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df = dataset.write('dataframe')
df['målkjønn'] = df['statistikkmål']+df['kjønn']
df_new = df.pivot(index='yrke', columns='målkjønn', values='value')
df_new['Kvinners lønn som andel av menns']=df_new['MedianKvinner']/df_new['MedianMenn']*100
df_new['Kvinners lønn sammenlignet med menns']=df_new['MedianKvinner']-df_new['MedianMenn']
df_new['Andel kvinner'] = df_new['Antall arbeidsforhold med lønnKvinner']/(df_new['Antall arbeidsforhold med lønnKvinner']+df_new['Antall arbeidsforhold med lønnMenn'])*100
df_new.dropna(inplace=True)
df_new.rename(columns={'MedianKvinner': 'Kvinner','MedianMenn': 'Menn',}, inplace=True)
df_new.to_csv('data/SSB_lonn_kjonn_yrke_median.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato=str(df.iloc[0,6])
date_string = 'Målt ved median månedslønn.' + ' Tall for ' + dato +'.' + ' Tabellen kan sorteres ved å klikke på overskriftene.'
#Update DW
chartid = 'liYSg'
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

#Median månedslønn heltid kvinner og menn yrker 4 sifret Z2gjw
ssburl = 'https://data.ssb.no/api/v0/no/table/11418/'
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
resultat = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df = dataset.write('dataframe')
df['målkjønn'] = df['statistikkmål']+df['kjønn']
df_new = df.pivot(index='yrke', columns='målkjønn', values='value')
df_new['Kvinners lønn som andel av menns']=df_new['MedianKvinner']/df_new['MedianMenn']*100
df_new['Kvinners lønn sammenlignet med menns']=df_new['MedianKvinner']-df_new['MedianMenn']
df_new['Andel kvinner'] = df_new['Antall arbeidsforhold med lønnKvinner']/(df_new['Antall arbeidsforhold med lønnKvinner']+df_new['Antall arbeidsforhold med lønnMenn'])*100
df_new.dropna(inplace=True)
df_new.rename(columns={'MedianKvinner': 'Kvinner','MedianMenn': 'Menn',}, inplace=True)
df_new.to_csv('data/SSB_lonn_kjonn_yrke_heltid_median.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato=str(df.iloc[0,6])
date_string = 'Målt ved median månedslønn for heltidsansatte.' + ' Tall for ' + dato +'.' + ' Tabellen kan sorteres ved å klikke på overskriftene.'
#Update DW
chartid = 'Z2gjw'
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


#Lønnsforskjeller etter utdanningsnivå og næring 55TV7
ssburl = 'https://data.ssb.no/api/v0/no/table/11420/'
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
        "filter": "item",
        "values": [
          "2020"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
resultat = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='næring (SN2007)', columns='utdanningsnivå', values='value')
df_new = df_new.reindex(columns=['I alt', 'Grunnskoleutdanning', 'Videregående utdanning', 'Universitets- og høgskoleutdanning, lavere nivå', 'Universitets- og høgskoleutdanning, høyere nivå, og forskerutdanning'])
df_new.rename(columns={'Grunnskoleutdanning':'Grunnskole','Videregående utdanning': 'Videregående skole','Universitets- og høgskoleutdanning, lavere nivå':'Universitet eller høyskole til og med 4 år', 'Universitets- og høgskoleutdanning, høyere nivå, og forskerutdanning':'Universitet eller høyskole, lengre enn 4 år'}, inplace=True)
df_new.to_csv('data/SSB_lonn_kjonn_utdanning_naring.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato=str(df.iloc[0,7])
date_string = 'Gjennomsnittlig månedslønn i ulike næringer fordelt etter utdanningsnivå.' + ' Tall for ' + dato +'.'
#Update DW
chartid = '55TV7'
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



#***
