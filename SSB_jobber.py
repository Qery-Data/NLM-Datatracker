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

#Jobber antall utvikling totalt nzFUM
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
          "AntLonnstakSesong",
          "AntArbForholdSesong"
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
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='måned', columns='statistikkvariabel', values='value')
df_new.to_csv('data/SSB_jobber_totalt.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y' + '.')

#Update DW
chartid = 'nzFUM'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Jobber mnd endring i antall t8TNy
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
          "AntArbForholdSesong"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "Top",
        "values": [
          63
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
df["endring"] = df.loc[:,"value"].diff()
df["endring i pst"] = df.loc[:,"value"].pct_change()*100
df_new = df[37:62]
df_new.to_csv('data/SSB_jobber_totalt_endring.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = 't8TNy'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Jobber antall per næring og endring per næring Wf007/R5eLv/S6QM8
ssburl = 'https://data.ssb.no/api/v0/no/table/13126/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "item",
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
          "90-99"
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
          "AntArbForholdSesong"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          62
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
df_new = df.pivot(index='næring (SN2007)', columns='måned', values='value')
df_new2 = df_new.iloc[:,[0,24,48,59,60]]
total = df_new2.iloc[:,4]
title_date = (total.name)
total.name = 'total'
Change_month = df_new2.iloc[:,4] - df_new2.iloc[:,3]
Change_12month = df_new2.iloc[:,4] - df_new2.iloc[:,2]
Change_covid = df_new2.iloc[:,4] - df_new['2020M02']
Change_3y = df_new2.iloc[:,4] - df_new2.iloc[:,1]
Change_5y = df_new2.iloc[:,4] - df_new2.iloc[:,0]
df_new3 = pd.concat([total, Change_month, Change_12month, Change_covid, Change_3y, Change_5y], axis=1, keys=['Antall','Endring sist mnd','Endring sist år','Endring fra feb.20','Endring siste 3 år','Endring siste 5 år'])
df_new3.to_csv('data/SSB_jobber_naring.csv', index=True)
date_string = title_date.replace("M","")
date_string2 = datetime.strptime(date_string, "%Y%m")
date_string3 = 'Sesongjusterte tall for ' + date_string2.strftime ('%B %Y')
date_string4 = 'Sesongjusterte tall for ' + date_string2.strftime ('%B %Y') + ' sammenlignet med samme måned året før.'
date_string5 = 'Sesongjusterte tall for ' + date_string2.strftime ('%B %Y') + '.'
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')

#Update DW Wf007 (Totalt antall sist mnd)
chartid = 'Wf007'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"describe": {"intro": date_string5}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"annotate": {"notes": chart_date}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Update DW R5eLv (Endring fra feb.20)
chartid = 'R5eLv'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"describe": {"intro": date_string3 + " sammenlignet med februar 2020."}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"annotate": {"notes": chart_date}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Update DW S6QM8 (Tabell endring i antall)
chartid = 'S6QM8'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"describe": {"intro": date_string4}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"annotate": {"notes": chart_date}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Jobber pst endring per næring 96bMF
Change_month_pct = ((df_new2.iloc[:,4] - df_new2.iloc[:,3]) / df_new2.iloc[:,3]*100)
Change_12month_pct = ((df_new2.iloc[:,4] - df_new2.iloc[:,2]) / df_new2.iloc[:,2]*100)
Change_covid_pct = ((df_new2.iloc[:,4] - df_new['2020M02'])/ df_new['2020M02']*100)
Change_3y_pct = ((df_new2.iloc[:,4] - df_new2.iloc[:,1]) / df_new2.iloc[:,1]*100)
Change_5y_pct = ((df_new2.iloc[:,4] - df_new2.iloc[:,0]) / df_new2.iloc[:,0]*100)
df_new4 = pd.concat([total, Change_month_pct, Change_12month_pct, Change_covid_pct, Change_3y_pct, Change_5y_pct], axis=1, keys=['Antall','Endring sist mnd','Endring sist år','Endring fra feb.20','Endring siste 3 år','Endring siste 5 år'])
df_new4.to_csv('data/SSB_jobber_naring_endring.csv', index=True)
date_string = title_date.replace("M","")
date_string2 = datetime.strptime(date_string, "%Y%m")
date_string3 = 'Sesongjusterte tall for ' + date_string2.strftime ('%B %Y') + ' sammenlignet med samme måned året før.'
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
#Update DW
chartid = '96bMF'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"describe": {"intro": date_string3}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {
    "metadata": {"annotate": {"notes": chart_date}}
    }
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

# Antall jobber fylke vuoG4
ssburl = 'https://data.ssb.no/api/v0/no/table/11657/'
query = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:FylkerFastIkkeFast",
        "values": [
          "30",
          "03",
          "34",
          "38",
          "42",
          "11",
          "46",
          "15",
          "50",
          "18",
          "54"
        ]
      }
    },
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
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "AntArbForhold"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [5]
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
df_new0 = df.pivot(index='region', columns='kvartal', values='value')
df_new = df_new0.rename(index={'Nordland - Nordlánnda': 'Nordland', 'Troms og Finnmark - Romsa ja Finnmárku':'Troms og Finnmark', 'Trøndelag - Trööndelage':'Trøndelag'})
df_new2 = df_new.iloc[:,[0,4]]
Change_absolute = df_new2.iloc[:,1] - df_new2.iloc[:,0]
Change_pct = Change_absolute / df_new2.iloc[:,0]*100
df_new3 = pd.concat([df_new2.iloc[:,1],Change_absolute, Change_pct], axis=1, keys=['Antall','Endring i antall', 'Endring i prosent'])
total = df_new2.iloc[:,1]
title_date = (total.name)
df_new3.to_csv('data/SSB_jobber_fylke_kvartal.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
date_string2 = title_date[-1:]
date_string3 = title_date[0:4]
date_int5 = int(date_string3)
date_int6 = date_int5 - 1
date_string7 = str(date_int6)
date_string4 = 'Tall for ' + date_string2 + '.kvartal ' + date_string3 + ' ' + 'sammenlignet med ' + date_string2 + '.kvartal ' + date_string7 + '.'
#Update DW
chartid = 'vuoG4'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Antall jobber kommune mJgIS 
ssburl = 'https://data.ssb.no/api/v0/no/table/11657/'
query = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "agg_single:KommSummerSa",
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
          "0301",
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
          "1101",
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
          "1130",
          "1133",
          "1134",
          "1135",
          "1144",
          "1145",
          "1146",
          "1149",
          "1151",
          "1160",
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
          "1505",
          "1506",
          "1507",
          "1511",
          "1514",
          "1515",
          "1516",
          "1517",
          "1520",
          "1525",
          "1528",
          "1531",
          "1532",
          "1535",
          "1539",
          "1547",
          "1554",
          "1557",
          "1560",
          "1563",
          "1566",
          "1573",
          "1576",
          "1577",
          "1578",
          "1579",
          "5001",
          "5006",
          "5007",
          "5014",
          "5020",
          "5021",
          "5022",
          "5025",
          "5026",
          "5027",
          "5028",
          "5029",
          "5031",
          "5032",
          "5033",
          "5034",
          "5035",
          "5036",
          "5037",
          "5038",
          "5041",
          "5042",
          "5043",
          "5044",
          "5045",
          "5046",
          "5047",
          "5049",
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
          "1804",
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
          "1845",
          "1848",
          "1851",
          "1853",
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
          "5444"
        ]
      }
    },
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
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "AntArbForhold"
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
df_new = df.pivot(index='region', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[0,4]]
Change_absolute = df_new2.iloc[:,1] - df_new2.iloc[:,0]
Change_pct = Change_absolute / df_new2.iloc[:,0]*100
df_new3 = pd.concat([df_new2.iloc[:,1], Change_absolute, Change_pct], axis=1, keys=['Antall','Endring i antall', 'Endring i prosent'])
total = df_new2.iloc[:,1]
title_date = (total.name)
df_new3.to_csv('data/SSB_jobber_kommune_kvartal.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
date_string2 = title_date[-1:]
date_string3 = title_date[0:4]
date_int5 = int(date_string3)
date_int6 = date_int5 - 1
date_string7 = str(date_int6)
date_string4 = 'Tall for ' + date_string2 + '.kvartal ' + date_string3 + ' ' + 'sammenlignet med ' + date_string2 + '.kvartal ' + date_string7 + '.'

#Update DW
chartid = 'mJgIS'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Antall jobber etter alder wWRNG
ssburl = 'https://data.ssb.no/api/v0/no/table/11652/'
query = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Landet4",
        "values": []
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
          "999A",
          "-24",
          "25-39",
          "40-54",
          "55-66",
          "67+"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "AntArbForhold"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [5]
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
df_new = df.pivot(index='alder', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[0,4]]
Change_absolute = df_new2.iloc[:,1] - df_new2.iloc[:,0]
Change_pct = Change_absolute / df_new2.iloc[:,0]*100
df_new3 = pd.concat([df_new2.iloc[:,1], Change_absolute, Change_pct], axis=1, keys=['Antall','Endring i antall', 'Endring i prosent'])
total = df_new2.iloc[:,1]
title_date = (total.name)
df_new3.to_csv('data/SSB_jobber_alder_beggekjonn.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
date_string2 = title_date[-1:]
date_string3 = title_date[0:4]
date_int5 = int(date_string3)
date_int6 = date_int5 - 1
date_string7 = str(date_int6)
date_string4 = 'Tall for ' + date_string2 + '.kvartal ' + date_string3 + ' ' + 'sammenlignet med ' + date_string2 + '.kvartal ' + date_string7 + '.'

#Update DW
chartid = 'wWRNG'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Antall jobber etter alder,menn zIE1z
ssburl = 'https://data.ssb.no/api/v0/no/table/11652/'
query = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Landet4",
        "values": []
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "1"
        ]
      }
    },
    {
      "code": "Alder",
      "selection": {
        "filter": "item",
        "values": [
          "999A",
          "-24",
          "25-39",
          "40-54",
          "55-66",
          "67+"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "AntArbForhold"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [5]
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
df_new = df.pivot(index='alder', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[0,4]]
Change_absolute = df_new2.iloc[:,1] - df_new2.iloc[:,0]
Change_pct = Change_absolute / df_new2.iloc[:,0]*100
df_new3 = pd.concat([df_new2.iloc[:,1], Change_absolute, Change_pct], axis=1, keys=['Antall','Endring i antall', 'Endring i prosent'])
total = df_new2.iloc[:,1]
title_date = (total.name)
df_new3.to_csv('data/SSB_jobber_alder_menn.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
date_string2 = title_date[-1:]
date_string3 = title_date[0:4]
date_int5 = int(date_string3)
date_int6 = date_int5 - 1
date_string7 = str(date_int6)
date_string4 = 'Tall for ' + date_string2 + '.kvartal ' + date_string3 + ' ' + 'sammenlignet med ' + date_string2 + '.kvartal ' + date_string7 + '.'

#Update DW
chartid = 'zIE1z'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Antall jobber etter alder,kvinner uRrvM
ssburl = 'https://data.ssb.no/api/v0/no/table/11652/'
query = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Landet4",
        "values": []
      }
    },
    {
      "code": "Kjonn",
      "selection": {
        "filter": "item",
        "values": [
          "2"
        ]
      }
    },
    {
      "code": "Alder",
      "selection": {
        "filter": "item",
        "values": [
          "999A",
          "-24",
          "25-39",
          "40-54",
          "55-66",
          "67+"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "AntArbForhold"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [5]
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
df_new = df.pivot(index='alder', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[0,4]]
Change_absolute = df_new2.iloc[:,1] - df_new2.iloc[:,0]
Change_pct = Change_absolute / df_new2.iloc[:,0]*100
df_new3 = pd.concat([df_new2.iloc[:,1], Change_absolute, Change_pct], axis=1, keys=['Antall','Endring i antall', 'Endring i prosent'])
total = df_new2.iloc[:,1]
title_date = (total.name)
df_new3.to_csv('data/SSB_jobber_alder_kvinner.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
date_string2 = title_date[-1:]
date_string3 = title_date[0:4]
date_int5 = int(date_string3)
date_int6 = date_int5 - 1
date_string7 = str(date_int6)
date_string4 = 'Tall for ' + date_string2 + '.kvartal ' + date_string3 + ' ' + 'sammenlignet med ' + date_string2 + '.kvartal ' + date_string7 + '.'

#Update DW
chartid = 'uRrvM'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Antall jobber etter innvandringskategori PpYio
ssburl = 'https://data.ssb.no/api/v0/no/table/12315/'
query = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Landet4",
        "values": [
          "Ialt"
        ]
      }
    },
    {
      "code": "InnvandrKat",
      "selection": {
        "filter": "item",
        "values": [
          "2",
          "25",
          "49"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "AntArbForhold"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [21]
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
df_new = df.pivot(index='innvandringskategori', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[4,8,12,16,20]]
total = df_new2.iloc[:,4]
title_date = (total.name)
df_new2.to_csv('data/SSB_jobber_innvandringskategori_kvartal.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
date_string2 = title_date[-1:]
date_string3 = title_date[0:4]
date_int5 = int(date_string3)
date_int6 = date_int5 - 1
date_string7 = str(date_int6)
date_string4 = 'Tall for ' + date_string2 + '.kvartal de siste fem årene.'
#Update DW
chartid = 'PpYio'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)

#Antall jobber etter innvandringskategori endring 3UutT
ssburl = 'https://data.ssb.no/api/v0/no/table/12315/'
query = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Landet4",
        "values": [
          "Ialt"
        ]
      }
    },
    {
      "code": "InnvandrKat",
      "selection": {
        "filter": "item",
        "values": [
          "95",
          "2",
          "25",
          "49"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "AntArbForhold"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [5]
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
df_new = df.pivot(index='innvandringskategori', columns='kvartal', values='value')
df_new2 = df_new.iloc[:,[0,4]]
Change_absolute = df_new2.iloc[:,1] - df_new2.iloc[:,0]
Change_pct = Change_absolute / df_new2.iloc[:,0]*100
df_new3 = pd.concat([df_new2.iloc[:,1], Change_absolute, Change_pct], axis=1, keys=['Antall','Endring i antall', 'Endring i prosent'])
total = df_new2.iloc[:,1]
title_date = (total.name)
df_new3.to_csv('data/SSB_jobber_innvandringskategori_endring.csv', index=True)
json_object = json.loads(result.text)
raw_date = json_object["updated"]
parsed_date = datetime.strptime(raw_date, '%Y-%m-%dT%H:%M:%SZ')
chart_date = 'Data sist publisert: ' + parsed_date.strftime ('%d/%m/%y')
date_string2 = title_date[-1:]
date_string3 = title_date[0:4]
date_int5 = int(date_string3)
date_int6 = date_int5 - 1
date_string7 = str(date_int6)
date_string4 = 'Tall for ' + date_string2 + '.kvartal ' + date_string3 + ' ' + 'sammenlignet med ' + date_string2 + '.kvartal ' + date_string7 + '.'

#Update DW
chartid = '3UutT'
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"annotate": {"notes": chart_date}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)
url = "https://api.datawrapper.de/v3/charts/" + chartid + '/'
payload = {"metadata": {"describe": {"intro": date_string4}}}
headers = {
    "Authorization": ("Bearer " + access_token),
    "Accept": "*/*",
    "Content-Type": "application/json"
    }
response = requests.request("PATCH", url, json=payload, headers=headers)