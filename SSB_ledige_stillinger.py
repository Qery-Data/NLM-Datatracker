from pyjstat import pyjstat
import requests
import os
os.makedirs('data', exist_ok=True)
ssburl = 'https://data.ssb.no/api/v0/no/table/11587/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "vs:NACE2007ledstillNN3",
        "values": [
          "01-96"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "LedigeStillinger"
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
df.to_csv('data/SSB_ledige_stillinger.csv', index=False)
 
