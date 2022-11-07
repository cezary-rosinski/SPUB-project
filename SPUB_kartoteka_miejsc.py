import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from SPUB_importer_read_data import read_MARC21
from SPUB_query_wikidata import wikidata_simple_dict_resp
from my_functions import marc_parser_dict_for_field, simplify_string, gsheet_to_df, create_google_worksheet
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
import pickle
import regex as re
import time
from geonames_accounts import geonames_users
import random
from collections import Counter
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
import datetime

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
    
def query_geonames(m):
    # m = 'Addis Ababa'
    url = 'http://api.geonames.org/searchJSON?'
    params = {'username': random.choice(geonames_users), 'q': m, 'featureClass': 'P', 'style': 'FULL'}
    result = requests.get(url, params=params).json()
    if 'status' in result:
        time.sleep(5)
        query_geonames(m)
    else:
        geonames_resp = [[e['geonameId'], e['name'], e['lat'], e['lng'], e['score']] for e in result['geonames']]
        if len(geonames_resp) == 0:
            miejscowosci_total[m] = geonames_resp
        if len(geonames_resp) == 1:
            miejscowosci_total[m] = geonames_resp[0]
        elif len(geonames_resp) > 1:    
            geonames_resp = max(geonames_resp, key=lambda x: x[-1])
            miejscowosci_total[m] = geonames_resp

def query_wikidata_place_id_with_geonames(geonames_id):
    # geonames_id = 3096472
# for geonames_id in biblio_places_geonames_ids:
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
    sparql.setQuery(f"""PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT distinct ?place WHERE {{
                  ?place wdt:P1566 "{geonames_id}"}}""")
    sparql.setReturnFormat(JSON)
    while True:
        try:
            results = sparql.query().convert()
            break
        except HTTPError:
            time.sleep(2)
        except URLError:
            time.sleep(5)
    results = [{v for k,v in flatten(e, separator='.').items() if k in wiki_columns} for e in results['results']['bindings']]
    if results:
        wikidata_ids_set.add(results[0].pop())

wiki_columns = ['placeLabel.xml:lang', 'placeLabel.value', 'place.value', 'country.value', 'countryLabel.xml:lang', 'countryLabel.value', 'countryStarttime.value', 'countryEndtime.value', 'coordinates.value', 'geonamesID.value', 'officialName.xml:lang', 'officialName.value', 'officialNameStarttime.value', 'officialNameEndtime.value', 'names.xml:lang', 'names.value']
#%% load

country_codes = pd.read_excel('translation_country_codes.xlsx')
country_codes = [list(e[-1]) for e in country_codes.iterrows()]
country_codes = dict(zip([e[0] for e in country_codes], [{'MARC_name': e[1], 'iso_alpha_2': e[2], 'Geonames_name': e[-1]} for e in country_codes]))
#%% main

#%% miejsca z osób z wikidaty
# client = pymongo.MongoClient()
# mydb = client['pbl-ibl-waw-pl_db']
# mycol = mydb['people']

# final_result = []
# [final_result.append(e) for e in mycol.find()]
# # final_result[0]

# places_from_wiki = []
# [places_from_wiki.append(e.get('wikidata_result')) for e in final_result if e.get('wikidata_result')]
# places_from_wiki = [{k:v for k,v in e.items()if k in ['birthplace', 'deathplace']} for e in places_from_wiki]

# places_from_wiki = set([elemen for elemen in [eleme for sub in [elem for elem in [[ele.get('value') for ele in [el for sub in list(e.values()) for el in sub] if ele] for e in places_from_wiki] if elem] for eleme in sub] if 'entity' in elemen])

# wikidata_places_dict = {}
# with ThreadPoolExecutor() as executor:
#     list(tqdm(executor.map(put_result_in_dict,places_from_wiki), total=len(places_from_wiki)))

# with open('places_from_wikidata.pickle', 'wb') as handle:
#     pickle.dump(wikidata_places_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
with open('places_from_wikidata.pickle', 'rb') as handle:
    bio_places_dict = pickle.load(handle)
    
# df = pd.concat([pd.DataFrame(e) for e in wikidata_places_dict.values()])    
# df = df[wiki_columns]
# df.to_excel('SPUB_miejsca_z_osob.xlsx', index=False) 

df = pd.read_excel('SPUB_miejsca_z_osob.xlsx')

#%%

# zderzyć miejsca wydania z miejscami z osób
# jeśli są nowe, to pozyskać dla nich informacje
# przygotować tabelę do manualnej pracy

records = read_mrk('bn_harvested_2021_05_12.mrk')
records[0]

# test = [{k:v for k,v in e.items() if k in ['001', '008', '260']} for e in records]
# [e.update({'country_code_marc21': e.get('008')[0][15:18].replace('\\', '')}) for e in test]
# [e.update({'iso_alpha_2': country_codes.get(e.get('country_code_marc21')).get('iso_alpha_2')}) if e.get('country_code_marc21') in country_codes else e.update({'iso_alpha_2': None}) for e in test]

# test = [e for e in test if '260' in e]

# [eleme.update({'places':[elem for sub in [[''.join([ele for ele in el.get('$a').replace('[etc.]', '').replace('[!]', '') if ele.isalnum() or ele in ['-', ' ', '(', ')']]).strip() for el in marc_parser_dict_for_field(e, '\\$') if '$a' in el] for e in eleme.get('260')] for elem in sub]}) for eleme in test]

# # Store data (serialize)
# with open('places_from_biblio.pickle', 'wb') as handle:
#     pickle.dump(test, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
# unique_places = set([el for sub in [e.get('places') for e in test] for el in sub])

# with open('places_from_biblio_unique.pickle', 'wb') as handle:
#     pickle.dump(unique_places, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('places_from_biblio.pickle', 'rb') as handle:
    biblio_places = pickle.load(handle)
with open('places_from_biblio_unique.pickle', 'rb') as handle:
    biblio_places_unique = pickle.load(handle)
biblio_places_unique = set([e for e in biblio_places_unique if e])


#%% query geonames

# miejscowosci_total = {}
# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(query_geonames, biblio_places_unique),total=len(biblio_places_unique)))
    
# with open('miejscowosci_biblio_geonames.pickle', 'wb') as handle:
#     pickle.dump(miejscowosci_total, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
with open('miejscowosci_biblio_geonames.pickle', 'rb') as handle:
    biblio_places_geonames = pickle.load(handle)    
    
biblio_places_geonames = {k:v for k,v in biblio_places_geonames.items() if v}


#tutaj sprawdzić, czy są miejsca, które wcześniej nie pojawiły się w zbiorach z wikidaty
#jak to sprawdzić? przez obecność geonames

bio_places_set_for_geonames = set()
[[bio_places_set_for_geonames.add(int(d.get('geonamesID.value'))) if d.get('geonamesID.value') else None for d in e] for e in bio_places_dict.values()]

biblio_places_for_update = {k:v for k,v in biblio_places_geonames.items() if v[0] not in bio_places_set_for_geonames}

biblio_places_for_update = set([tuple(e) for e in biblio_places_for_update.values()])
# Counter([e[0] for e in biblio_places_for_update]).most_common(10)
# [e for e in biblio_places_for_update if e[0] == 2925533]
biblio_places_geonames_ids = set([e[0] for e in biblio_places_for_update])

wikidata_ids_set = set()
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(query_wikidata_place_id_with_geonames,biblio_places_geonames_ids), total=len(biblio_places_geonames_ids)))

test_places_file = gsheet_to_df('1Ruu8fa-wzZ2fwj86S4UhWn_J3_xREjaw_B-P_B7OOvs', 'out')

places_to_be_found = set([e for e in wikidata_ids_set if e not in bio_places_dict and e not in test_places_file['id'].to_list()])

wikidata_places_dict = {}
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(put_result_in_dict,places_to_be_found), total=len(places_to_be_found)))

places_dict = bio_places_dict | wikidata_places_dict

places_dict = {k:v for k,v in places_dict.items() if k not in test_places_file['id'].to_list()}

df = pd.concat([pd.DataFrame(e) for e in places_dict.values()]) 
df = df[wiki_columns]
df.to_excel('SPUB_miejsca_z_osob.xlsx', index=False) 

#%% przetwarzanie kartoteki miejsc

df = pd.read_excel('SPUB_miejsca_z_osob.xlsx')

grouped = df.groupby('place.value')

date_cols = ['countryStarttime.value', 'countryEndtime.value', 'officialNameStarttime.value', 'officialNameEndtime.value']

final_df = pd.DataFrame()
miejsca_bez_dat_df = pd.DataFrame()
for name, group in tqdm(grouped, total=len(grouped)):
    # name = 'http://www.wikidata.org/entity/Q690039'
    # name = 'http://www.wikidata.org/entity/Q25430'
    # group = grouped.get_group(name)
    pl_labels = group.loc[group['placeLabel.xml:lang'] == 'pl'].shape[0]
    en_labels = group.loc[group['placeLabel.xml:lang'] == 'en'].shape[0]
    if pl_labels == en_labels:
        group = group.loc[group['placeLabel.xml:lang'] == 'pl']
    final_df = pd.concat([final_df, group])
final_df = final_df.reset_index(drop=True)    

grouped = final_df.groupby('place.value')
final_df = pd.DataFrame()
for name, group in tqdm(grouped, total=len(grouped)):
    # name = 'http://www.wikidata.org/entity/Q1001326'
    # group = grouped.get_group(name)
    if [e for e in group['coordinates.value'].to_list() if pd.notnull(e)] and [e for e in group['geonamesID.value'].to_list() if pd.notnull(e)]:
        group = group.loc[group['coordinates.value'] == min(set(group['coordinates.value'].to_list()))]
        group = group.loc[group['geonamesID.value'] == min(set(group['geonamesID.value'].to_list()))]
    final_df = pd.concat([final_df, group])
final_df = final_df.reset_index(drop=True)


    
#1 wiersz są okej

#2 więcej wierszy i daty puste

#3 daty

#4 reszta?
test_dict = {'1 wiersz': pd.DataFrame(),
             'jeden geonames, jeden point, daty country': pd.DataFrame(),
             'więcej wierszy i daty puste': pd.DataFrame(),
             'są daty': pd.DataFrame()}
grouped = final_df.groupby('place.value')  
for name, group in tqdm(grouped, total=len(grouped)): 
    # group = test_dict.get('są daty')[3:98]
    if len(group) == 1:
        test_dict['1 wiersz'] = pd.concat([test_dict['1 wiersz'], group])
    elif group.drop(columns='coordinates.value').drop_duplicates().shape[0] == 1 and len(group['coordinates.value'].unique()) > 1:
        group = group.head(1)
        test_dict['1 wiersz'] = pd.concat([test_dict['1 wiersz'], group])
    elif group.drop(columns='geonamesID.value').drop_duplicates().shape[0] == 1 and len(group['geonamesID.value'].unique()) > 1:
        group = group.nsmallest(1, 'geonamesID.value')
        test_dict['1 wiersz'] = pd.concat([test_dict['1 wiersz'], group])
    elif group.drop(columns=['geonamesID.value', 'coordinates.value']).drop_duplicates().shape[0] == 1 and len(group['geonamesID.value'].unique()) > 1 and len(group['coordinates.value'].unique()) > 1:
        group = group.nsmallest(1, 'geonamesID.value')
        test_dict['1 wiersz'] = pd.concat([test_dict['1 wiersz'], group])
    elif len(group['geonamesID.value'].unique()) == 1 and len(group['coordinates.value'].unique()) == 1 and [e for sub in group[['countryStarttime.value', 'countryEndtime.value']].values.tolist() for e in sub if pd.notnull(e)] and not [e for sub in group[['officialNameStarttime.value', 'officialNameEndtime.value']].values.tolist() for e in sub if pd.notnull(e)]:
        test_dict['jeden geonames, jeden point, daty country'] = pd.concat([test_dict['jeden geonames, jeden point, daty country'], group])
    elif not [e for sub in group[date_cols].values.tolist() for e in sub if pd.notnull(e)]:
        test = group.drop(columns=['names.xml:lang','names.value']).drop_duplicates()
        unique_pl = list(set(group['names.value'].to_list()))
        test['dodatkowe pl'] = test.apply(lambda x: unique_pl, axis=1)
        test_dict['więcej wierszy i daty puste'] = pd.concat([test_dict['więcej wierszy i daty puste'], test])
    elif [e for sub in group[date_cols].values.tolist() for e in sub if pd.notnull(e)]:
        test = group.drop(columns=['names.xml:lang','names.value']).drop_duplicates()
        unique_pl = tuple(set(group['names.value'].to_list()))
        test['dodatkowe pl'] = test.apply(lambda x: unique_pl, axis=1)
        test_dict['są daty'] = pd.concat([test_dict['są daty'], test])
        
        test = group.drop(columns=['officialName.xml:lang', 'officialName.value', 'officialNameStarttime.value', 'officialNameEndtime.value']).drop_duplicates()
        test['countryStarttime.value'] = test['countryStarttime.value'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').date() if pd.notnull(x) else x)
        test = test.sort_values('countryStarttime.value')
        
        ttt = group[['officialName.xml:lang', 'officialName.value', 'officialNameStarttime.value', 'officialNameEndtime.value']].drop_duplicates().to_dict(orient='index')
        ttt = tuple(ttt.values())
        test['xxx'] = test.apply(lambda x: ttt, axis=1)
        
#CEL
#wydobyć wszystkie państwa, które nie mają polskiej wersji, przetłumaczyć je i dodać
#połączyć te odcinki czasu dla nazw oficjalnych, które korespondują z przynależnością do państwa i na wypełnić tylko te wiersze w tabeli, w których zachodzi taka relacja

datetime.datetime.strptime('1867-03-30T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ').date()
        group.columns.values

#%% google sheets upload
gc = gs.oauth()
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

sheet = gc.create('SPUB – kartoteka miejsc – prace manualne', '1gafJy33s2zE_yOBxCYS3zRvWCCVvhqib')

for k,v in test_dict.items():
    create_google_worksheet(sheet.id, k, v)






















