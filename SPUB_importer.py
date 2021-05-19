from SPUB_importer_read_data import read_MARC21, get_list_of_people, get_list_of_records
from SPUB_importer_query_national_library import query_national_library





#read data

marc21_records = read_MARC21('bn_harvested_2021_05_12.mrk')

people_list = get_list_of_people(marc21_records, ('=100'), '(\$e|\$t).*', 200)

bibliographical_records = get_list_of_records(marc21_records, people_list, ('=100'), '(\$e|\$t).*')

people_list = get_list_of_people(bibliographical_records, ('=100', '=600', '=700'), '(\$x|\$4|\$e|\.\$t).*')

#query national library

people_list_of_dicts = query_national_library(people_list)





































# przygotować testową listę rekordów bibliograficznych
# 300 ksiażek z bn (jakie?)
# wydobyć z nich osoby i związać z kartoteką wzorcową BN
# połączyć dane z viaf i wikidatą oraz wydobyć dodatkowe informacje do schematu PBL
import pandas as pd
import io
from google_drive_research_folders import PBL_folder
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from my_functions import cSplit, df_to_mrc, mrk_to_df, mrc_to_mrk
import datetime
import regex as re
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from tqdm import tqdm
import glob
import time
import json
from collections import Counter
import requests
from urllib.error import HTTPError
from http.client import RemoteDisconnected

viaf = '49338782'
# 04 - utożsamienie z wikidatą
url = 'https://query.wikidata.org/sparql'     

sparql_query = """SELECT ?property ?propertyLabel ?value WHERE {    
?property wikibase:propertyType wikibase:ExternalId .    
?property wikibase:directClaim ?propertyclaim .   
OPTIONAL {?property wdt:P1630 ?formatterURL .}   wd:Q680 ?propertyclaim ?_value .    BIND(IF(BOUND(?formatterURL), IRI(REPLACE(?formatterURL, "\\$", ?_value)) , ?_value) AS ?value) SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }                                     }"""

  
for i, osoba in enumerate(tqdm(osoby_z_rekordow_lista_slownikow, total=len(osoby_z_rekordow_lista_slownikow))):
    while True:
        try:
            viaf = osoba['VIAF ID']
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
            osoby_z_rekordow_lista_slownikow[i]['wikidata ID'] = result
            time.sleep(1)
        except (AttributeError, KeyError, ValueError):
            time.sleep(1)
        except (HTTPError, RemoteDisconnected) as error:
            print(error)# time.sleep(61)
            time.sleep(5)
            continue
        break
print("--- %s seconds ---" % (time.time() - start_time))


for r in rekordy:
    for e in r:
        if '$aRodziewiczówna, Maria$d(1864-1944).' in e:
            print(e)




import xml.etree.cElementTree as ET

def generate_XML(file):
    pbl = ET.Element("pbl")
    files = ET.SubElement(pbl, "files")
    people = ET.SubElement(files, "people")
    person = ET.SubElement(people, "person", id='id1', creator="c_rosinski").text = 'some text'

    tree = ET.ElementTree(pbl)
    tree.write(file)
    
generate_XML('test.xml')


krasinski = osoby_z_rekordow_lista_slownikow[13]

name_types_dict = {'pseudonym.value':'alias', 'autorLabel.value':'main-name'}

krasinski['wikidata ID'][0].keys()

tablica.get(krasinski['wikidata ID'][0]['pseudonym.value'])

#tablica przekodowania między wikidatą a strukturą xml 

def create_name(parent, data, field_name):
    name = ET.SubElement(parent, "name", transliteration='no', code=name_types_dict.get(field_name)).text = data['wikidata ID'][0][field_name]
    

pbl = ET.Element("pbl")
files = ET.SubElement(pbl, "files")
people = ET.SubElement(files, "people")
person = ET.SubElement(people, "person", id='id1', creator="c_rosinski")
names = ET.SubElement(person, "names")
# name = ET.SubElement(names, "name", transliteration='no', code=name_types_dict.get('autorLabel.value')).text = krasinski['wikidata ID'][0]['autorLabel.value']
# name = ET.SubElement(names, "name", transliteration='no', code=name_types_dict.get('pseudonym.value')).text = krasinski['wikidata ID'][0]['pseudonym.value']
# create_name(names, krasinski, 'autorLabel.value')
# create_name(names, krasinski, 'pseudonym.value')
for element in name_types_dict:
    create_name(names, krasinski, element)

tree = ET.ElementTree(pbl)
tree.write('krasinski.xml', encoding='UTF-8', )
    
person = ET.SubElement(people, "person", id='id1', creator="c_rosinski").text = 'some text'    
    
    
#18.05.2021 - zbudować mechanizm dla osób xml - zapisać pliki 
# przygotować instytucje do kartoteki instytucji    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
                    
# final_list = []
# for lista in rekordy:
#     slownik = {}
#     for el in lista:
#         if el[1:4] in slownik:
#             slownik[el[1:4]] += f"❦{el[6:]}"
#         else:
#             slownik[el[1:4]] = el[6:]
#     final_list.append(slownik)

# df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
# fields = df.columns.tolist()
# fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
# df = df.loc[:, df.columns.isin(fields)]
# fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
# df = df.reindex(columns=fields)   




























