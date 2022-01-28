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


#Lønnsutviklingen, nominelt og reallønn, 1R4EQ
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
df5_new.rename(columns = {'Real=<16':'Reallønn. Endring fra året før i prosent'})
df6_new = df5_new.iloc[30:]
df6_new.to_csv('data/SSB_lonn_real_nominell.csv', index=True)

#Gjennomsnittlig årslønn næring nivå og endring sist år 8tWRB  
ssburl = 'https://data.ssb.no/api/v0/no/table/11417/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "item",
        "values": [
          "A-S",
          "A",
          "B",
          "C",
          "D",
          "E",
          "F",
          "G",
          "H",
          "I",
          "J",
          "K",
          "L",
          "M",
          "N",
          "O",
          "P",
          "Q",
          "R",
          "S"
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
df_new = df.pivot(index='næring (SN2007)',columns='statistikkvariabel',values='value')
df_new.rename(index={'Elektrisitets-, gass-, damp- og varmtvannsforsyning': 'Kraftforsyning', 'Faglig, vitenskapelig og teknisk tjenesteyting': 'Faglig, vit. og tekn. tjenesteyting', 'Finansierings- og forsikringsvirksomhet':'Finansiering og forsikring', 'Offentlig administrasjon og forsvar, og trygdeordninger underlagt offentlig forvaltning':'Off.adm., forsvar, sosialforsikring', 'Overnattings- og serveringsvirksomhet':'Overnattings- og serveringsvirks.','Varehandel, reparasjon av motorvogner':'Varehandel, bilverksteder'}, inplace=True)
df_new.to_csv('data/SSB_lonn_aar_nivå_endring.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato=str(df.iloc[0,2])
date_string = 'Tall for ' + dato +'.' + ' Endring fra forrige år i prosent.'
#Update DW
chartid = '8tWRB'
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

#Gjennomsnittlig lønn måned yrker
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
resultat = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(resultat.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='yrke',columns='år',values='value')
df_new['Endring sist år'] = (df_new.iloc[:,4]-df_new.iloc[:,3])/df_new.iloc[:,3]*100
df_new['Endring siste 5 år'] = (df_new.iloc[:,4]-df_new.iloc[:,0])/df_new.iloc[:,0]*100
df_new.dropna(inplace=True)
df_new.to_csv('data/SSB_lonn_maned_yrke.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
dato=str(df_new.columns[4])
date_string = 'Tall for ' + dato +'.' + ' Heltidsansatte.'
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


#***
#Gjennomsnittlig årslønn utvikling 0P7CI
ssburl = 'https://data.ssb.no/api/v0/no/table/09785/'
query = {
  "query": [
    {
      "code": "NACE",
      "selection": {
        "filter": "item",
        "values": [
          "nr23_6"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Arslonn"
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
df_new = df.pivot(index='år',columns='statistikkvariabel',values='value')
df_new.to_csv('data/SSB_lonn_aar_utvikling.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
chartid = '0P7CI'
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

#Gjennomsnittlig årslønn endring og KPI 1R4EQ
ssburl = 'https://data.ssb.no/api/v0/no/table/12880/'
query = {
  "query": [
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Aarslonn",
          "KPI"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [17
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
df_new = df.pivot(index='år',columns='statistikkvariabel',values='value')
df_new['Reallønn']=df_new['Årslønn']-df_new['Konsumprisindeksen (KPI)']
df_new = df_new.iloc[:-4 , :]
df_new.to_csv('data/SSB_lonn_aar_endring.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y') + '<br>*Reallønnsvekst er lønnsvekst fratrukket prisvekst. Positivt reallønnsvekst betyr at kjøpekraften økes.'
#Update DW
chartid = '1R4EQ'
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







#Gjennomsnittlig kontantlønn utvikling totalt Eq2Ke
ssburl = 'https://data.ssb.no/api/v0/no/table/13126/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "item",
        "values": [
          "00-99"
        ]
      }
    },
    {
      "code": "ForelopigEndelig",
      "selection": {
        "filter": "item",
        "values": [
          "02"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "GjsnKontantlonn"
        ]
      }
    },
        {
      "code": "Tid",
      "selection": {
        "filter": "Top",
        "values": [63]
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
df_new = df.pivot(index='måned', columns='statistikkvariabel', values='value')
df_new.to_csv('data/SSB_lonn_kontant_totalt.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
chartid = 'Eq2Ke'
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

#Gjennomsnittlig kontantlønn endring per mnd i pst QDmy4
ssburl = 'https://data.ssb.no/api/v0/no/table/13126/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "item",
        "values": [
          "00-99"
        ]
      }
    },
    {
      "code": "ForelopigEndelig",
      "selection": {
        "filter": "item",
        "values": [
          "02"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "GjsnKontantlonn"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "Top",
        "values": [63]
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
df["endring"] = df["value"].diff()
df["endring i pst"] = df["value"].pct_change()*100
df = df[1:62]
df.to_csv('data/SSB_lonn_kontant_totalt_endring.csv', index=True)
json_object = json.loads(resultat.text)
oppdatert = json_object["updated"]
oppdatert_dato = datetime.strptime(oppdatert, '%Y-%m-%dT%H:%M:%SZ')
riktig_dato = 'Data sist publisert: ' + oppdatert_dato.strftime ('%d/%m/%y')
#Update DW
chartid = 'QDmy4'
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