from tqdm import tqdm
import requests
import pandas as pd
import time
from urllib.error import HTTPError
from http.client import RemoteDisconnected
import regex as re

def query_wikidata(list_of_dicts):
    url = 'https://query.wikidata.org/sparql' 
    for i, osoba in enumerate(tqdm(list_of_dicts)):
        while True:
            try:
                viaf = osoba['VIAF_ID']
                sparql_query = f"""PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT distinct ?author ?authorLabel ?birthplaceLabel ?deathplaceLabel ?birthdate ?deathdate ?sexLabel ?pseudonym ?occupationLabel ?genreLabel ?birthNameLabel ?aliasLabel ?birthplace ?deathplace WHERE {{ 
                  ?author wdt:P214 "{viaf}" ;
                  optional {{ ?author wdt:P19 ?birthplace . }}
                  optional {{ ?author wdt:P569 ?birthdate . }}
                  optional {{ ?author wdt:P570 ?deathdate . }}
                  optional {{ ?author wdt:P20 ?deathplace . }}
                  optional {{ ?author wdt:P21 ?sex . }}
                  optional {{ ?author wdt:P106 ?occupation . }}
                  optional {{ ?author wdt:P742 ?pseudonym . }}
                  optional {{ ?author wdt:P136 ?genre . }}
                  optional {{ ?author rdfs:label ?alias . }}
                  optional {{ ?author wdt:P1477 ?birthName . }}
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl". }}}}"""    
                results = requests.get(url, params = {'format': 'json', 'query': sparql_query})
                results = results.json()
                results_df = pd.json_normalize(results['results']['bindings'])
                columns = [e for e in results_df.columns.tolist() if 'value' in e]
                results_df = results_df[results_df.columns.intersection(columns)]       
                for column in results_df.drop(columns='author.value'):
                    results_df[column] = results_df.groupby('author.value')[column].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
                results_df = results_df.drop_duplicates().reset_index(drop=True)   
                result = results_df.to_dict('records')
                list_of_dicts[i]['wikidata_ID'] = result[0]
                time.sleep(1)
            except (AttributeError, KeyError, ValueError):
                time.sleep(1)
            except (HTTPError, RemoteDisconnected) as error:
                print(error)# time.sleep(61)
                time.sleep(5)
                continue
            break
    return list_of_dicts
    
def query_wikidata_for_country(place_url):
    try:
        place_id = re.findall('Q\d+', place_url)[-1]
        url = 'https://query.wikidata.org/sparql'
        sparql_query = f"""PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                    SELECT distinct ?country ?countryLabel WHERE {{
                      wd:{place_id} wdt:P17 ?country .
                      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl". }}}}"""
        results = requests.get(url, params = {'format': 'json', 'query': sparql_query})
        results = results.json()            
        results_df = pd.json_normalize(results['results']['bindings'])            
        columns = [e for e in results_df.columns.tolist() if 'value' in e]
        results_df = results_df[results_df.columns.intersection(columns)].drop_duplicates().reset_index(drop=True)
        results_df['place.value'] = place_url   
        result = results_df.to_dict('records')
        return result 
    except ValueError:
        pass
             
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                

