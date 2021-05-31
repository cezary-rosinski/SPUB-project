from SPUB_importer_read_data import read_MARC21, get_list_of_people, get_list_of_records
from SPUB_importer_query_national_library import query_national_library
from SPUB_query_wikidata import query_wikidata, query_wikidata_for_country
from SPUB_stack_in_db import create_db
import datetime
import pandas as pd
from tqdm import tqdm
import json
from geojson import Point
import numpy as np
import regex as re


#%% date
now = datetime.datetime.now()
year = now.year
month = '{:02}'.format(now.month)
day = '{:02}'.format(now.day)



#%% main
#read data

marc21_records = read_MARC21('bn_harvested_2021_05_12.mrk')

people_list = get_list_of_people(marc21_records, ('=100'), '(\$e|\$t).*', 200)

bibliographical_records = get_list_of_records(marc21_records, people_list, ('=100'), '(\$e|\$t).*')

people_list = get_list_of_people(bibliographical_records, ('=100', '=600', '=700'), '(\$x|\$4|\$e|\.\$t).*')

#query national library

people_list_of_dicts = query_national_library(people_list)

#query wikidata
# odpytać jeszcze raz, bo Sapkowski nie dostał wikidaty - czemu?

people_list_of_dicts = query_wikidata(people_list_of_dicts)

#send people to people db

people_list_of_dicts = [pd.json_normalize(e).to_dict(orient='records')[0] for e in people_list_of_dicts]

people_df = pd.concat([pd.json_normalize(e) for e in people_list_of_dicts])
create_db('import.db', [people_df[['name_MARC21', 'name_simple', 'BN_ID', 'VIAF_ID', 'wikidata_ID.author.value']]], ['people'])

#save people JSON

with open('import_people.json', 'w', encoding='utf-8') as f:
    json.dump(people_list_of_dicts, f)

#query places

places_from_people = pd.concat([people_df['wikidata_ID.birthplace.value'], people_df['wikidata_ID.deathplace.value']]).drop_duplicates().dropna().to_list()
places_from_people = [e.split('❦') for e in places_from_people]
places_from_people = list(set([e for sub in places_from_people for e in sub]))

textfile = open("a_file.txt", "w")
for element in places_from_people:
    textfile.write(element + "\n")
textfile.close()

places_from_people_wikidata = []
for i, place in tqdm(enumerate(places_from_people), total=len(places_from_people)):
    places_from_people_wikidata.append(query_wikidata_for_country(place))
    
with open('places_wikidata.json', 'w', encoding='utf-8') as f:
    json.dump(places_from_people_wikidata, f)

#main

with open('import_people.json') as json_file:
    people_list_of_dicts = json.load(json_file)

with open('places_wikidata.json') as json_file:
    places_from_people_wikidata = json.load(json_file)

test = [e for sub in places_from_people_wikidata for e in sub]
test = pd.concat([pd.json_normalize(e) for e in test]).reset_index(drop=True)

def names_if_dates(x):
    a = x['starttime.value']
    b = x['endtime.value']
    c = x['officialName.value']
    if any(pd.notnull(e) for e in [a, b]):
        return c
    else:
        return np.nan

test['officialName.value'] = test.apply(lambda x: names_if_dates(x), axis=1)
test = test.drop_duplicates().reset_index(drop=True)

coord= test.groupby('place.value')['coordinates.value'].max()
test['coordinates.value'] = test['place.value'].map(coord)

test = test.drop_duplicates().reset_index(drop=True)

test['coordinates.value'] = test['coordinates.value'].apply(lambda x: Point(tuple([float(e) for e in re.findall('[\d\.-]+', x)][::-1])) if pd.notnull(x) else np.nan)

#place: id, geo, lat, lon
#name_place: name, date from, date to, country



#chyba miejscowość trzeba tak spiąć, żeby na jednym poziomie było id, geonames, lat i lon, a na drugim poziomie name + country ORAZ wszystkie nazwy z datami



for i, dictionary in enumerate(people_list_of_dicts):
    # i = 375
    # i = 154
    # dictionary = people_list_of_dicts[i]
    try:
        birth = dictionary['wikidata_ID.birthplace.value']
        birth_list = []
        for place in birth.split('❦'):
            # place = birth.split('❦')[0]
            place_dict = {}
            place_dict['place_name'] = []
            df = test[test['place.value'] == place].dropna(how='all', axis=1)
            result = df.to_dict('records')
            try:
                if any(e for e in result if e['placeLabel.value'] == e['officialName.value']):
                    result = [{key: value for key, value in some_dict.items() if key != 'placeLabel.value'} for some_dict in result]
            except KeyError:
                pass

            for ind, element in enumerate(result):
                
                for k,v in element.items():
                    
                    if k in ['place.value', 'geonamesID.value', 'coordinates.value'] and k not in place_dict:
                        place_dict[k] = v
                    else:
                        if pd.notnull(v) and k not in ['place.value', 'geonamesID.value', 'coordinates.value']:
                           
                            try: 
                                place_dict['place_name'][ind].update({k:v})
                            except IndexError:
                                place_dict['place_name'].append({k:v})
            birth_list.append(place_dict)
        people_list_of_dicts[i]['wikidata_ID.birthplace.value'] = birth_list
    except (IndexError, KeyError):
        pass
    try:
        death = dictionary['wikidata_ID.deathplace.value']
        death_list = []
        for place in death.split('❦'):
            # place = death.split('❦')[0]
            place_dict = {}
            place_dict['place_name'] = []
            df = test[test['place.value'] == place].dropna(how='all', axis=1)
            result = df.to_dict('records')
            try:
                if any(e for e in result if e['placeLabel.value'] == e['officialName.value']):
                    result = [{key: value for key, value in some_dict.items() if key != 'placeLabel.value'} for some_dict in result]
            except KeyError:
                pass

            for ind, element in enumerate(result):
                
                for k,v in element.items():
                    
                    if k in ['place.value', 'geonamesID.value', 'coordinates.value'] and k not in place_dict:
                        place_dict[k] = v
                    else:
                        if pd.notnull(v) and k not in ['place.value', 'geonamesID.value', 'coordinates.value']:
                           
                            try: 
                                place_dict['place_name'][ind].update({k:v})
                            except IndexError:
                                place_dict['place_name'].append({k:v})
            death_list.append(place_dict)
        people_list_of_dicts[i]['wikidata_ID.deathplace.value'] = death_list
    except (IndexError, KeyError):
        pass
#send places to places db
        


for i, osoba in enumerate(people_list_of_dicts):
    try: 
        for ind, dictionary in enumerate(osoba['wikidata_ID.birthplace.value']):
            # ind = 1
            # dictionary = osoba['wikidata_ID.birthplace.value'][1]
            for index, place in enumerate(dictionary['place_name']):
                # place = dictionary['place_name'][0]
                if 'placeLabel.value' in place and 'officialName.value' in place:
                    dictionary_1 = {key: value for key, value in place.items() if key != 'placeLabel.value'}
                    dictionary_2 = {key: value for key, value in place.items() if key not in ['officialName.value', 'startime.value', 'endtime.value']}
                    people_list_of_dicts[i]['wikidata_ID.birthplace.value'][ind]['place_name'] = [dictionary_1, dictionary_2]
    except KeyError:
        pass
    try:                
        for ind, dictionary in enumerate(osoba['wikidata_ID.birthplace.value']):
             for index, place in enumerate(dictionary['place_name']):
                if 'placeLabel.value' in place and 'officialName.value' in place:
                    dictionary_1 = {key: value for key, value in place.items() if key != 'placeLabel.value'}
                    dictionary_2 = {key: value for key, value in place.items() if key not in ['officialName.value', 'startime.value', 'endtime.value']}
                    people_list_of_dicts[i]['wikidata_ID.deathplace.value'][ind]['place_name'] = [dictionary_1, dictionary_2]
    except KeyError:
        pass
                
        
    


#create XML

people_list_of_dicts = [pd.json_normalize(e).to_dict(orient='records')[0] for e in people_list_of_dicts]

mickiewicz = pd.json_normalize(people_list_of_dicts[62]).to_dict(orient='records')


lista = []
for e in people_list_of_dicts:
    for k,v in e.items():
        if v == 'http://www.wikidata.org/entity/Q1799':
            lista.append(e)
        


#Nowogródek - placelabelvalue != officialName.value -> wtedy zrobić 2 entry


for i, e in enumerate(people_list_of_dicts):
    if 'Mickiewicz' in e['name_simple']:
        print(i)

osoba = people_list_of_dicts[62].copy() 






















#institutions
    
institutions_list = get_list_of_people(bibliographical_records, ('=110', '=610', '=710'), '(\$x|\$4|\$e|\.\$t).*')
#query national library

institutions_list_of_dicts = query_national_library(institutions_list)

url = 'https://data.bn.org.pl/api/authorities.json?'
list_of_dicts = []
for i, person in tqdm(enumerate(list_of_people), total=len(list_of_people)):
    person_dict = {}
    person_dict['name_MARC21'] = person
    person = marc_parser_dict_for_field(person, '\$')
    person = ' '.join([v for k, v in person.items()])
    person_dict['name_simple'] = person
    params = {'name': person, 'limit':'100'}
    result = requests.get(url, params = params).json()
    authority = [e['marc']['fields'] for e in result['authorities'] if e['title'] == '']
    authority = [[d for d in e if any(a in ['001', '024', '100'] for a in d)] for e in authority]
    person2 = person.replace('?', '')
    if len(authority) == 0:
        person_dict['name_simple_no_question_mark'] = person2
        params = {'name': person2, 'limit':'100'}
        result = requests.get(url, params = params).json()
        authority = [e['marc']['fields'] for e in result['authorities'] if e['title'] == '']
        authority = [[d for d in e if any(a in ['001', '024', '100'] for a in d)] for e in authority]
    for e in authority:
        person_in_authority = [a['100']['subfields'] for a in e if '100' in a]
        person_in_authority = [a for sub in person_in_authority for a in sub]
        person_in_authority = [[v for k,v in a.items()] for a in person_in_authority]
        person_in_authority = ' '.join([a for sub in person_in_authority for a in sub])
        if person_in_authority == person or person_in_authority == person2:
            for d in e:
                if '001' in d:
                    id = d['001']
                    if 'BN_ID' in person_dict:
                        person_dict['BN_ID'] = '❦'.join([id, person_dict['BN_ID']])
                    else:
                        person_dict['BN_ID'] = id
                elif '024' in d:
                    viaf = d['024']['subfields']
                    for el in viaf:
                        for di in el:
                            if 'a' in di:
                                viaf = re.findall('\d+', el['a'])[0]
                                if 'VIAF_ID' in person_dict:
                                    person_dict['VIAF_ID'] = '❦'.join([viaf, person_dict['VIAF_ID']])
                                else:
                                    person_dict['VIAF_ID'] = viaf
                                        
    person_dict = {k:'❦'.join(list(set(v.split('❦')))) for k,v in person_dict.items()}
    list_of_dicts.append(person_dict)





















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




























