from tqdm import tqdm
import requests
import pandas as pd
import time
from urllib.error import HTTPError
from http.client import RemoteDisconnected

def query_wikidata(list_of_dicts):
    url = 'https://query.wikidata.org/sparql' 
    for i, osoba in enumerate(tqdm(list_of_dicts)):
        while True:
            try:
                viaf = osoba['VIAF_ID']
                sparql_query = f"""PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT distinct ?autor ?autorLabel ?birthplaceLabel ?deathplaceLabel ?birthdate ?deathdate ?sexLabel ?pseudonym ?occupationLabel ?genreLabel ?aliasLabel WHERE {{ 
                  ?autor wdt:P214 "{viaf}" ;
                  optional {{ ?autor wdt:P19 ?birthplace . }}
                  optional {{ ?autor wdt:P569 ?birthdate . }}
                  optional {{ ?autor wdt:P570 ?deathdate . }}
                  optional {{ ?autor wdt:P20 ?deathplace . }}
                  optional {{ ?autor wdt:P21 ?sex . }}
                  optional {{ ?autor wdt:P106 ?occupation . }}
                  optional {{ ?autor wdt:P742 ?pseudonym . }}
                  optional {{ ?autor wdt:P136 ?genre . }}
                  optional {{ ?autor rdfs:label ?alias . }}
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl". }}}}"""    
                results = requests.get(url, params = {'format': 'json', 'query': sparql_query})
                results = results.json()
                results_df = pd.json_normalize(results['results']['bindings'])
                columns = [e for e in results_df.columns.tolist() if 'value' in e]
                results_df = results_df[results_df.columns.intersection(columns)]       
                for column in results_df.drop(columns='autor.value'):
                    results_df[column] = results_df.groupby('autor.value')[column].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
                results_df = results_df.drop_duplicates().reset_index(drop=True)   
                result = results_df.to_dict('records')
                list_of_dicts[i]['wikidata_ID'] = result
                time.sleep(1)
            except (AttributeError, KeyError, ValueError):
                time.sleep(1)
            except (HTTPError, RemoteDisconnected) as error:
                print(error)# time.sleep(61)
                time.sleep(5)
                continue
            break
    return list_of_dicts
    






