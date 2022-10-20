from SPUB_importer_read_data import read_MARC21
from SPUB_query_wikidata import wikidata_simple_dict_resp
from my_functions import marc_parser_dict_for_field, simplify_string
from tqdm import tqdm
import fuzzywuzzy
import pandas as pd
import jellyfish
import pymongo
import requests
from concurrent.futures import ThreadPoolExecutor
from SPARQLWrapper import SPARQLWrapper, JSON
from collections import defaultdict
import sys
from urllib.error import HTTPError, URLError
from flatten_json import flatten

#%%def
def read_mrk(path):
    records = []
    with open(path, 'r', encoding='utf-8') as mrk:
        record_dict = {}
        for line in mrk.readlines():
            line = line.replace('\n', '')
            if line.startswith('=LDR'):
                if record_dict:
                    records.append(record_dict)
                    record_dict = {}
                record_dict[line[1:4]] = [line[6:]]
            elif line.startswith('='):
                key = line[1:4]
                if key in record_dict:
                    record_dict[key] += [line[6:]]
                else:
                    record_dict[key] = [line[6:]]
        records.append(record_dict)
    return records

def harvest_geonames(set_of_places):
    set_of_places = places
    place = list(set_of_places)[0]
    
    
    old, city, country = ov_list
# places_geonames_extra_2 = {}
# errors = []
# for old, city, country in tqdm(ov_geonames):
    # old, city, country = ov_geonames[264]
    if pd.notnull(city) and city in places_geonames and isinstance(places_geonames[city], dict):
        places_geonames_extra_2[old] = places_geonames[city]
    elif all(pd.notnull(e) for e in [city, country]): 
        try:
            url = 'http://api.geonames.org/searchJSON?'
            q_country = ccodes[country.lower()]
            params = {'username': 'crosinski', 'q': city, 'featureClass': 'P', 'country': q_country}
            result = requests.get(url, params=params).json()
            places_geonames_extra_2.update({old: max(result['geonames'], key=lambda x: x['population'])})
        except (KeyError, ValueError):
            errors.append((old, city, country))
    elif pd.isnull(city):
        try:
            url = 'http://api.geonames.org/searchJSON?'
            q_country = ccodes[country.lower()]
            params = {'username': 'crosinski', 'q': old, 'featureClass': 'P', 'country': q_country}
            result = requests.get(url, params=params).json()
            places_geonames_extra_2.update({old: max(result['geonames'], key=lambda x: x['population'])})
        except (KeyError, ValueError):
            errors.append((old, city, country))
    elif pd.isnull(country):
        try:
            url = 'http://api.geonames.org/searchJSON?'
            params = {'username': 'crosinski', 'q': city, 'featureClass': 'P'}
            result = requests.get(url, params=params).json()
            places_geonames_extra_2.update({old: max(result['geonames'], key=lambda x: x['population'])})
        except (KeyError, ValueError):
            errors.append((old, city, country))
    else: errors.append((old, city, country))

def query_wikidata_person_with_viaf(viaf_id):
    # viaf_id = 49338782
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
    sparql.setQuery(f"""PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT distinct ?author ?authorLabel ?birthplaceLabel ?deathplaceLabel ?birthdate ?deathdate ?sexLabel ?pseudonym ?occupationLabel ?genreLabel ?birthNameLabel ?aliasLabel ?birthplace ?deathplace WHERE {{ 
                  ?author wdt:P214 "{viaf_id}" ;
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
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl". }}}}""")
    sparql.setReturnFormat(JSON)
    while True:
        try:
            results = sparql.query().convert()
            break
        except HTTPError:
            time.sleep(2)
        except URLError:
            time.sleep(5)
    results = wikidata_simple_dict_resp(results)  
    return results

def query_for_wikidata_place(wikidata_url):
    # wikidata_url = 'http://www.wikidata.org/entity/Q1799'
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
    place_id = re.findall('Q\d+', wikidata_url)[-1]
    sparql.setQuery(f"""PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT distinct ?placeLabel ?country ?countryLabel ?countryStarttime ?countryEndtime ?officialName ?officialNameStarttime ?officialNameEndtime ?coordinates ?geonamesID ?names WHERE {{
                  wd:{place_id} rdfs:label ?placeLabel 
                  filter(lang(?placeLabel) = 'pl' || lang(?placeLabel) = 'en') .
                  optional {{ wd:{place_id} skos:altLabel ?names 
                  filter(lang(?names) = 'pl') . }}
                  optional {{ wd:{place_id} p:P17 ?countryStatement . }}
                  bind(coalesce(?countryStatement,"brak danych" ^^xsd:string) as ?countryTest)
                  optional {{ ?countryTest ps:P17 ?country . }}
                  optional {{ ?countryTest pq:P580 ?countryStarttime . }}
                  optional {{ ?countryTest pq:P582 ?countryEndtime . }}
                  optional {{ wd:{place_id} wdt:P625 ?coordinates . }}
                  optional {{ wd:{place_id} wdt:P1566 ?geonamesID . }}
                  optional {{ wd:{place_id} p:P1448 ?statement . }}
                  bind(coalesce(?statement,"brak danych" ^^xsd:string) as ?test)
                  optional {{ ?test ps:P1448 ?officialName . }}
                  optional {{ ?test pq:P580 ?officialNameStarttime . }}
                  optional {{ ?test pq:P582 ?officialNameEndtime . }}
                  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl". }}}}""")
    sparql.setReturnFormat(JSON)
    while True:
        try:
            results = sparql.query().convert()
            break
        except HTTPError:
            time.sleep(2)
        except URLError:
            time.sleep(5)
    # results = wikidata_simple_dict_resp(results) 
    
    results = [{k:v for k,v in flatten(e, separator='.').items() if k in wiki_columns} for e in results['results']['bindings']]
    [e.update({'place.value':wikidata_url}) for e in results]
    return results

def put_result_in_dict(wikidata_url):
    wikidata_places_dict.update({wikidata_url: query_for_wikidata_place(wikidata_url)})

wiki_columns = ['placeLabel.xml:lang', 'placeLabel.value', 'place.value', 'country.value', 'countryLabel.xml:lang', 'countryLabel.value', 'countryStarttime.value', 'countryEndtime.value', 'coordinates.value', 'geonamesID.value', 'officialName.xml:lang', 'officialName.value', 'officialNameStarttime.value', 'officialNameEndtime.value', 'names.xml:lang', 'names.value']
#%% load

country_codes = pd.read_excel('translation_country_codes.xlsx')
country_codes = [list(e[-1]) for e in country_codes.iterrows()]
country_codes = dict(zip([e[0] for e in country_codes], [{'MARC_name': e[1], 'iso_alpha_2': e[2], 'Geonames_name': e[-1]} for e in country_codes]))
#%% main

#%% miejsca z osób z wikidaty
client = pymongo.MongoClient()
mydb = client['pbl-ibl-waw-pl_db']
mycol = mydb['people']

final_result = []
[final_result.append(e) for e in mycol.find()]
# final_result[0]

places_from_wiki = []
[places_from_wiki.append(e.get('wikidata_result')) for e in final_result if e.get('wikidata_result')]
places_from_wiki = [{k:v for k,v in e.items()if k in ['birthplace', 'deathplace']} for e in places_from_wiki]

places_from_wiki = set([elemen for elemen in [eleme for sub in [elem for elem in [[ele.get('value') for ele in [el for sub in list(e.values()) for el in sub] if ele] for e in places_from_wiki] if elem] for eleme in sub] if 'entity' in elemen])

wikidata_places_dict = {}
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(put_result_in_dict,places_from_wiki), total=len(places_from_wiki)))
    
df = pd.concat([pd.DataFrame(e) for e in wikidata_places_dict.values()])    
df = df[wiki_columns]
df.to_excel('SPUB_miejsca_z_osob.xlsx', index=False) 

#%%

# zderzyć miejsca wydania z miejscami z osób
# jeśli są nowe, to pozyskać dla nich informacje
# przygotować tabelę do manualnej pracy















dir(jellyfish)

records = read_mrk('bn_harvested_2021_05_12.mrk')
records[0]

test = [{k:v for k,v in e.items() if k in ['001', '008', '260']} for e in records]
[e.update({'country_code_marc21': e.get('008')[0][15:18].replace('\\', '')}) for e in test]
[e.update({'iso_alpha_2': country_codes.get(e.get('country_code_marc21')).get('iso_alpha_2')}) if e.get('country_code_marc21') in country_codes else e.update({'iso_alpha_2': "boom"}) for e in test]

#dodać do słownika nazwę
# zrobić słownik nazw
# dodać identyfikator nazwy do rekordu, żeby mieć powiązanie
# poszukać, jak zrobiłem próbkę kartoteki miejsc: C:\Users\Cezary\Documents\SPUB-project\SPUB_query_wikidata.py -- tu jest droga z wikidaty

# dodać też miejsca z osób: urodzenie + śmierć

test[0]

test[0].update()

{'001': ['b0000002769128'],
 '008': ['130220s2013\\\\\\\\ru\\\\\\\\\\\\\\\\\\\\\\|000\\0\\ruso\\'],
 '260': ['\\\\$aMoskva :$b"Centr knigi Rudomino",$c2011.'],
 'country_marc_code': 'ru',
 'country': 'Russia',
 'place_name': 'Moskva'
 }

MARC_code	MARC_name	iso_alpha_2	Geonames_name

ru	Russia (Federation)	RU	Russia


# test = [e for e in records if '773' in e]
# test[0]

places = []
for record in tqdm(records):

    record_places = [marc_parser_dict_for_field(e, '\\$') for e in record.get('260')]
    record_places = [[e.get('$a') for e in el if '$a' in e] for el in record_places]
    record_places = [e for sub in record_places for e in sub]
    places.append(record_places)
    
places = set([e for sub in places for e in sub])
# sprawdzić wyrywkowo rzeczy w []

# test = [e for e in places if any(x in e for x in ['[', ']'])]

places = set([e.replace('[etc.]', '').replace('[!]', '') for e in places])
places = set([''.join([l for l in el if l.isalnum() or l in['-', ' ', '(', ')']]).strip() for el in places])









[etc.]
[!]


''.join([e for e in list(test)[1] if e.isalnum() or e == ''])

dir('a')

name_in_question = "albinany"
for allowed in allowed_names: # you would probably want to narrow the allowed names list down based on other data or the first charater
    percent_match_to_allowed = fuzz.ratio(allowed, name_in_question)
    if percent_match_to_allowed > 90:
         name_in_question = allowed


test = [simplify_string(e, with_spaces=True, nodiacritics=False) for e in test]


[e.get('$a') for e in test if '$a' in e]

[[el['$a'].replace('-','').strip().split(' ')[0] for el in marc_parser_dict_for_field(e, '\$') if '$a' in el]



marc21_records = read_MARC21('bn_harvested_2021_05_12.mrk')