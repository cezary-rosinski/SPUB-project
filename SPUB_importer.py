# jeśli osoba nie ma dat - najnowszy period
# jeśli osoba ma datę, ale periodu nie ma - najnowszy period
# mogą być dwie daty urodzin lub śmierci - wziąć ostatnią

# w pliku osobowym atrybut dla płci będzie "code" (teraz jest "value")


# rozwiązanie w kartotece instytucji dla dat startu i końca obowiązywania nazwy
# trzeba dodać atrybuty name='begin', name='end' i tag date trzeba powtórzyć
from SPUB_importer_read_data import read_MARC21, get_list_of_people, get_list_of_records
from SPUB_importer_query_national_library import return_people_dict_bn
from SPUB_query_wikidata import query_wikidata, query_wikidata_for_country, query_wikidata_person_with_viaf
from SPUB_stack_in_db import create_db
from SPUB_XML_file_people import create_person
from SPUB_XML_file_places import create_xml_places_from_gsheet
import pandas as pd
from tqdm import tqdm
import json
from geojson import Point
import numpy as np
import regex as re
import xml.etree.cElementTree as ET
import lxml.etree
from flexidate import parse
from datetime import datetime, timedelta, date
from lxml.builder import E
from lxml.etree import ElementTree
import glob
import io
from my_functions import marc_parser_dict_for_field, gsheet_to_df
from SPUB_XML_file_institutions import create_institution
import time
import pickle 
from concurrent.futures import ThreadPoolExecutor
import pymongo

# wszystkie pliki XML zapisywać jako pretty print
# KARTOTEKA MIEJSC – jeśli period jest pusty, to nie dodawać atrybutów date-from, date-to
# KARTOTEKA OSÓB – jeśli miejsce odwołuje się do periodu, którego nie ma, to nie podajemy atrybutu period
# KARTOTEKA OSÓB – jeśli mamy jedną datę, to hedera sygnalizuje, czy jest to data początkowa, czy końcowa
#%% def

def def_period(x):
    try:
        start = re.findall('.+(?=T)', x['starttime.value'])[0] if pd.notnull(x['starttime.value']) else ''
    except IndexError:
        start = ''
    try:
        end = re.findall('.+(?=T)', x['endtime.value'])[0] if pd.notnull(x['endtime.value']) else ''
    except IndexError:
        end = ''
    return f"{start}|{end}"

def update_mongo_with_wikidata(mongo_record):
    mongo_id = mongo_record['_id']
    if pd.notnull(mongo_record["VIAF_ID"]):
        try:
            viaf_id = re.findall('\d+', mongo_record['VIAF_ID'])[0]
            resp = query_wikidata_person_with_viaf(viaf_id)
            mongo_record.update({'wikidata_result': resp})
            newvalues = { "$set": mongo_record}
            mycol.update_one({'_id': mongo_id}, newvalues)
        except ValueError:
            pass

#%% main
#read data

marc21_records = read_MARC21('bn_harvested_2021_05_12.mrk')

# path = 'F:/Cezary/Documents/IBL/BN/bn_authorities/'
# files = [file for file in glob.glob(path + '*.mrk', recursive=True)]
# encoding = 'utf-8'
# fields_368 = []
# for file in tqdm(files):
#     marc_list = io.open(file, 'rt', encoding = encoding).readlines()
#     temp_list = [marc_parser_dict_for_field(e, '\$') for e in marc_list if e.startswith('=368')]
#     temp_list = [[f['$a'] for f in e if '$a' in f] for e in temp_list]  
#     temp_list = [e for sub in temp_list for e in sub if sub]
#     fields_368.extend(temp_list)
    
# fields_368 = list(set(fields_368))

# with open('typy_instytucji_BN.txt', 'a', encoding='utf-8') as file:
#     for e in fields_368:
#         file.write(e + '\n')


# people_list = get_list_of_people(marc21_records, ('=100'), '(\$e|\$t).*', 200)

# bibliographical_records = get_list_of_records(marc21_records, people_list, ('=100'), '(\$e|\$t).*')

# people_list = get_list_of_people(bibliographical_records, ('=100', '=600', '=700'), '(\$x|\$4|\$e|\.\$t).*')

#query national library

#tu trzeba wprowadzić jakieś ograniczenia, np. po długości stringu
people_list = get_list_of_people(marc21_records, ('=100', '=600', '=700'), '(\$x|\$4|\$e|\.\$t).*')

people_list = [e for e in people_list if len(e) >= 10 or (' ' in e and e.count(' ') <= 2 and len(e) >= 9)]

people_dict, bn_no_response = return_people_dict_bn(people_list)

with open('people_dict.pickle', 'wb') as handle:
    pickle.dump(people_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
with open('bn_no_response.pickle', 'wb') as handle:
    pickle.dump(bn_no_response, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('people_dict.pickle', 'rb') as handle:
    people_dict = pickle.load(handle)

client = pymongo.MongoClient()
mydb = client['pbl-ibl-waw-pl_db']
mycol = mydb['people']
# mydb.drop_collection('people')
# mycol.insert_many([people_dict[e] for e in people_dict])

#!!!TUTAJ!!! - odpytać wikidatę, bo dostałem bana
mongo_len = len(list(mycol.find()))

# [update_mongo_with_wikidata(e) for e in tqdm(mycol.find(), total=mongo_len)]   

with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(update_mongo_with_wikidata,mycol.find()), total=mongo_len))

# missing_wikidata = mycol.find({'$or':[{'wikidata_result': {'$exists':False}}, {'wikidata_result':{}}]})
# len_missing = len(list(missing_wikidata))
# missing_wikidata = mycol.find({'$or':[{'wikidata_result': {'$exists':False}}, {'wikidata_result':{}}]})

# for el in tqdm(missing_wikidata, total = len_missing):
#     update_mongo_with_wikidata(el)








print(client.list_database_names())

for _ in mycol.find():
    print(_)

test = mycol.find()[0]
for _ in mycol.find({"BN_result.marc.fields.100.subfields.a": "De Vita, Giulio"}):
    print(_)
# with open('NW_people.json', 'w', encoding='utf-8') as f:
#     json.dump(people_list_of_dicts, f)

#query wikidata
# odpytać jeszcze raz, bo Sapkowski nie dostał wikidaty - czemu?

people_list_of_dicts = query_wikidata(people_list_of_dicts)
# people_list_of_dicts = people_list_of_dicts[:11]

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
    
def grab_language(identifier, langs=['pl', 'en', 'fr', 'es', 'it']):
    url = 'https://query.wikidata.org/sparql'
    sparql_query = f"""PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                            SELECT distinct ?placeLabel WHERE {{
                              wd:{identifier} rdfs:label ?placeLabel 
                              SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl". }}}}"""
    results = requests.get(url, params = {'format': 'json', 'query': sparql_query})
    time.sleep(2)
    results = results.json()['results']['bindings']
    for el in results:
        for lang in langs:
            if el['placeLabel']['xml:lang'] == lang:
                return (el['placeLabel']['value'],el['placeLabel']['xml:lang'])
                break

for i, el in tqdm(enumerate(places_from_people_wikidata), total=len(places_from_people_wikidata)):
    for ind, ele in enumerate(el):
        try:
            if re.findall('^Q\d+', ele['countryLabel.value']):
                places_from_people_wikidata[i][ind]['countryLabel.value'], places_from_people_wikidata[i][ind]['countryLabel.xml:lang'] = grab_language(ele['countryLabel.value'])
        except KeyError:
            pass

with open('places_wikidata.json', 'w', encoding='utf-8') as f:
    json.dump(places_from_people_wikidata, f)        
                      

#main

with open('import_people.json') as json_file:
    people_list_of_dicts = json.load(json_file)
    
[e for e in people_list_of_dicts if 'BN_ID' in e and e['BN_ID'] == 'a0000002720404']

with open('places_wikidata.json') as json_file:
    places_from_people_wikidata = json.load(json_file)

test = [e for sub in places_from_people_wikidata for e in sub]
test = pd.concat([pd.json_normalize(e) for e in test]).reset_index(drop=True)

def names_if_dates(x):
    a = x['officialNameStarttime.value']
    b = x['officialNameEndtime.value']
    c = x['officialName.value']
    if any(pd.notnull(e) for e in [a, b]):
        return c
    else:
        return np.nan
    
def langs_if_dates(x):
    a = x['officialNameStarttime.value']
    b = x['officialNameEndtime.value']
    c = x['officialName.xml:lang']
    if any(pd.notnull(e) for e in [a, b]):
        return c
    else:
        return np.nan

test['officialName.value'] = test.apply(lambda x: names_if_dates(x), axis=1)
test['officialName.xml:lang'] = test.apply(lambda x: langs_if_dates(x), axis=1)
test = test.drop_duplicates().reset_index(drop=True)

test_grouped = test.groupby('place.value')

test = pd.DataFrame()
for name, group in test_grouped:
    names_name = group['officialName.value'].isna().sum()
    total = group['officialName.value'].value_counts(dropna=False).sum()
    if group.shape[0] > 1 and names_name < total:
        group = group[group['officialName.value'].notnull()]
        test = test.append(group)
    else:
        test = test.append(group)

test['geonamesID.value'] = test.groupby('place.value')['geonamesID.value'].transform('min')
test = test.drop_duplicates().reset_index(drop=True)

coord = test.groupby('place.value')['coordinates.value'].max()
test['coordinates.value'] = test['place.value'].map(coord)

# jedno albo drugie | z dodatkowymi nazwami po polsku lub nie
# test = test.drop_duplicates().reset_index(drop=True)
test = test.drop(columns=['names.xml:lang', 'names.value']).drop_duplicates().reset_index(drop=True)

test['coordinates.value'] = test['coordinates.value'].apply(lambda x: Point(tuple([float(e) for e in re.findall('[\d\.-]+', x)][::-1])) if pd.notnull(x) else np.nan)

test = test.sort_values(['place.value', 'countryStarttime.value', 'officialNameStarttime.value'])
test.to_excel('kartoteka_miejsc_próbka.xlsx', index=False)

#send places to places db
create_db('import.db', [test[['placeLabel.value', 'place.value', 'countryLabel.value', 'country.value', 'geonamesID.value']]], ['places'])

import_places = dict(tuple(test.groupby('place.value')))
import_places = {k:v.to_dict(orient='records') for k,v in import_places.items()}
# ttt = {k:v.dropna(how='all', axis=1).to_dict(orient='records') for k,v in ttt.items()}
# ttt = {k:[{key:value for key,value in e.items() if pd.notnull(value)} for e in v] for k,v in ttt.items()}

#ogarnąć periody na nowo - jak pogodzić periody nazw miejsc i nazw państw???

for key,value in import_places.items():
    # key = 'http://www.wikidata.org/entity/Q1792'
    # key = 'http://www.wikidata.org/entity/Q585'
    # key = 'http://www.wikidata.org/entity/Q1156'
    # key = 'http://www.wikidata.org/entity/Q406'
    # value = import_places[key]
    # print(key)
    place_dict = {}
    place_dict['place_dates'] = {}
    # place_dict['place_names'] = []
    for i, loc in enumerate(value):
        # print(i)
        # i = 0
        # loc = value[i]
       
        period = def_period(loc)
        if period not in place_dict['place_dates']:    
            place_dict['place_dates'].update({period:[{}]})
        else:
            place_dict['place_dates'][period].append({})
        # for k, v in place.items():    
        for k, v in loc.items():
            if pd.notnull(v):
                if k in ['place.value', 'geonamesID.value', 'coordinates.value'] and k not in place_dict:
                    place_dict[k] = v
                elif k not in ['starttime.value', 'endtime.value', 'place.value', 'geonamesID.value', 'coordinates.value']:
                    place_dict['place_dates'][period][-1][k] = v
                    # try: 
                    #     place_dict['place_dates'][period][-1][k] = v
                    # except IndexError:
                    #     place_dict['place_dates'][period][-1].update({k:v})         
    import_places[key] = place_dict

xml_nodes = create_node_structure(['pbl', 'files', 'places'])  
for k, v in import_places.items():
    create_place(xml_nodes['places'], v)   
tree = ET.ElementTree(xml_nodes['pbl'])
tree.write('import_places.xml', encoding='UTF-8')

















for i, dictionary in enumerate(people_list_of_dicts):
    # i = 375
    # i = 15
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

for i, osoba in enumerate(people_list_of_dicts):
    try: 
        for ind, dictionary in enumerate(osoba['wikidata_ID.birthplace.value']):
            # ind = 1
            # dictionary = osoba['wikidata_ID.birthplace.value'][1]
            for index, place in enumerate(dictionary['place_name']):
                # place = dictionary['place_name'][0]
                if 'placeLabel.value' in place and 'officialName.value' in place:
                    dictionary_1 = {key: value for key, value in place.items() if key != 'placeLabel.value'}
                    dictionary_2 = {key: value for key, value in place.items() if key not in ['officialName.value', 'officialName.xml:lang', 'startime.value', 'endtime.value']}
                    people_list_of_dicts[i]['wikidata_ID.birthplace.value'][ind]['place_name'] = [dictionary_1, dictionary_2]
    except KeyError:
        pass
    try:                
        for ind, dictionary in enumerate(osoba['wikidata_ID.birthplace.value']):
             for index, place in enumerate(dictionary['place_name']):
                if 'placeLabel.value' in place and 'officialName.value' in place:
                    dictionary_1 = {key: value for key, value in place.items() if key != 'placeLabel.value'}
                    dictionary_2 = {key: value for key, value in place.items() if key not in ['officialName.value', 'officialName.xml:lang', 'startime.value', 'endtime.value']}
                    people_list_of_dicts[i]['wikidata_ID.deathplace.value'][ind]['place_name'] = [dictionary_1, dictionary_2]
    except KeyError:
        pass
                
#create XML

xml_nodes = create_node_structure(['pbl', 'files', 'people'])
for osoba in tqdm(people_list_of_dicts):    
    xml_nodes['person'] = create_person(xml_nodes['people'], osoba)
    xml_nodes['names'] = ET.SubElement(xml_nodes['person'], "names")          
    create_name(xml_nodes['names'], osoba)   
    xml_nodes['birth'] = ET.SubElement(xml_nodes['person'], "birth")       
    create_birth_death_date(xml_nodes['birth'], osoba)
    create_place(xml_nodes['birth'], osoba)
    xml_nodes['death'] = ET.SubElement(xml_nodes['person'], "death")     
    create_birth_death_date(xml_nodes['death'], osoba, kind='death')
    create_place(xml_nodes['death'], osoba, kind='death')
    create_remark(xml_nodes['person'], osoba)
    create_tags(xml_nodes['person'], osoba)
    create_links(xml_nodes['person'], osoba)

    
tree = ET.ElementTree(xml_nodes['pbl'])
tree.write('import_people.xml', encoding='UTF-8')




#02.08.2021
# kartoteka miejsc
xml_nodes = create_node_structure(['pbl', 'files', 'places'])
list_of_places = create_xml_places_from_gsheet('1Ruu8fa-wzZ2fwj86S4UhWn_J3_xREjaw_B-P_B7OOvs', 'out', xml_nodes['places'])
tree = ET.ElementTree(xml_nodes['pbl'])
tree.write('import_places.xml', encoding='UTF-8')

# test = [e for e in places_from_people if e not in [a['id'] for a in list_of_places]]

# kartoteka osób
with open('import_people.json', encoding='utf-8') as json_file:
    people_list_of_dicts = json.load(json_file)
    
# test = [e for e in people_list_of_dicts if 'Riley' in e['name_simple']]
# test = [e for e in people_list_of_dicts if 'wikidata_ID.deathplace.value' in e and 'http://www.wikidata.org/entity/Q4918'== e['wikidata_ID.deathplace.value']]
    
def handle_dates(v):
    try:
        return [datetime.strptime(v[i], '%Y-%m-%d').date() if v[i] != '' else date(1500, 1, 1) if i == 0 else datetime.now().date() for i, val in enumerate(v)]
    except ValueError:
        pass
        

for ind, dictionary in enumerate(people_list_of_dicts):
    # i = 375
    # i = 0
    # ind = 73
    # dictionary = people_list_of_dicts[ind]
    try:
        birth = dictionary['wikidata_ID.birthplace.value']
        place_id = [e for e in list_of_places if e['id'] in birth.split('❦')][0]
        periods = {k:k.split('❦') for k in place_id['period'].keys()}
        periods = {k:handle_dates(v) for k,v in periods.items()}
        try:
            birth_date = datetime.strptime(dictionary['wikidata_ID.birthdate.value'].split('❦')[0], '%Y-%m-%dT%H:%M:%SZ').date()
            periods = {k:v for k,v in periods.items() if v is not None and v[0] <= birth_date <= v[1]}
        except KeyError:
            max_date = max(periods.items(), key=lambda x: x[-1])
            periods = {}
            periods[max_date[0]] = max_date[-1]
        birth_place = {'id': place_id['id'], 'period': list({k for k,v in periods.items()})[0], 'lang': place_id['period'][list({k for k,v in periods.items()})[0]]['place name lang']}
        people_list_of_dicts[ind]['wikidata_ID.birthplace.value'] = birth_place
    except KeyError:
        pass

    try:
        death = dictionary['wikidata_ID.deathplace.value']
        place_id = [e for e in list_of_places if e['id'] in death.split('❦')][0]
        periods = {k:k.split('❦') for k in place_id['period'].keys()}
        periods = {k:handle_dates(v) for k,v in periods.items()}
        try:
            death_date = datetime.strptime(dictionary['wikidata_ID.deathdate.value'].split('❦')[0], '%Y-%m-%dT%H:%M:%SZ').date()
            periods = {k:v for k,v in periods.items() if v is not None and v[0] <= death_date <= v[1]}
        except KeyError:
            max_date = max(periods.items(), key=lambda x: x[-1])
            periods = {}
            periods[max_date[0]] = max_date[-1]
        death_place = {'id': place_id['id'], 'period': list({k for k,v in periods.items()})[0], 'lang': place_id['period'][list({k for k,v in periods.items()})[0]]['place name lang']}
        people_list_of_dicts[ind]['wikidata_ID.deathplace.value'] = death_place
    except KeyError:
        pass

    
with open('baza_marlena_biogramy_viaf.json', encoding='utf-8') as json_file:
    osoby_slownik = json.load(json_file) 
    
for key,value in osoby_slownik.items(): 
    try:
        proper_viaf = max(value['viafs'], key=lambda x: x['similarity'])
        osoby_slownik[key]['viafs'] = proper_viaf
    except (TypeError, ValueError):
        pass
    

people_viafs = []
for element in people_list_of_dicts:
    try:
        people_viafs.append(element['VIAF_ID'])
    except KeyError:
        pass
    
test = {}
for k,v in osoby_slownik.items():
    try:
        if v['viafs']['viaf'] in people_viafs:
            viaf = v['viafs']['viaf'] 
            bio = v['Biogram']
            test[viaf] = bio
    except TypeError:
        pass
        
for i, dictionary in enumerate(people_list_of_dicts):
    try:
        people_list_of_dicts[i]['bio'] = test[dictionary['VIAF_ID']]    
    except KeyError:
        pass

                
#create XML

people = E.people()
for person in tqdm(people_list_of_dicts):
    try:
        people.append(create_person(person))
    except KeyError:
        pass
import_people = E.pbl(E.files(people))
to_save = ElementTree(import_people)
to_save.write("import_people.xml", xml_declaration=True, encoding='utf-8')     


#institutions
from my_functions import marc_parser_dict_for_field  
import requests 

df_places = gsheet_to_df('1Ruu8fa-wzZ2fwj86S4UhWn_J3_xREjaw_B-P_B7OOvs', 'out')

institutions_list = get_list_of_people(bibliographical_records, ('=110', '=610', '=710', '=810'), '(\$x|\$4|\$e|\.\$t).*')
#query national library

institutions_list_of_dicts = query_national_library(institutions_list, kind='corporation')

for i, e in tqdm(enumerate(institutions_list_of_dicts), total=len(institutions_list_of_dicts)):
    # viaf_id = '132112363' #v['VIAF_ID'] 
    try:
        viaf_id = e['VIAF_ID']
        result = requests.get(f'https://viaf.org/viaf/{viaf_id}/viaf.json').json()
        try:
            occupation = result['occupation']['data']['text']
        except KeyError:
            occupation = np.nan
        try:
            wikipedia_pl = [e['#text'] for e in result['xLinks']['xLink'] if isinstance(e, dict) and 'pl.wikipedia' in e['#text']][0]
        except IndexError:
            wikipedia_pl = np.nan
        try:
            wikidata_id = [e['@nsid'] for e in result['sources']['source'] if 'WKP' in e['#text']][0]
        except TypeError:
            try:
                wikidata_id = {v for k,v in result['sources']['source'].items() if k=='@nsid' and 'WKP' in v}.pop()
            except KeyError:
                wikidata_id = np.nan
        except IndexError:
            wikidata_id = np.nan
        temp_dict = {'viaf_id': viaf_id,
                     'occupation': occupation,
                     'wikipedia': wikipedia_pl}
        temp_dict = {k:v for k,v in temp_dict.items() if pd.notnull(v)}
        institutions_list_of_dicts[i]['VIAF_ID'] = temp_dict
        if pd.notnull(wikidata_id):
            result = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json').json()
            try:
                instance_of = [e['mainsnak']['datavalue']['value']['id'] for e in result['entities'][f'{wikidata_id}']['claims']['P31']]
            except KeyError:
                instance_of = np.nan
            try:
                inception = [e['mainsnak']['datavalue']['value']['time'] for e in result['entities'][f'{wikidata_id}']['claims']['P571']]
            except KeyError:
                inception = np.nan
            try:
                location = [e['mainsnak']['datavalue']['value']['id'] for e in result['entities'][f'{wikidata_id}']['claims']['P276']]
            except KeyError:
                location = np.nan
            try:
                coordinates = [(e['mainsnak']['datavalue']['value']['latitude'], e['mainsnak']['datavalue']['value']['longitude']) for e in result['entities'][f'{wikidata_id}']['claims']['P625']]
            except KeyError:
                coordinates = np.nan
            try:
                replaces = [e['mainsnak']['datavalue']['value']['id'] for e in result['entities'][f'{wikidata_id}']['claims']['P1365']]
            except KeyError:
                replaces = np.nan
            wikidata_dict = {'wikidata_id': wikidata_id,
                             'instance_of': instance_of,
                             'inception': inception,
                             'location': location,
                             'replaces': replaces,
                             'coordinates': coordinates}
            wikidata_dict = {k:v for k,v in wikidata_dict.items() if not(isinstance(v, float))}
            for ke,va in {k:v for k,v in wikidata_dict.items() if k not in ['wikidata_id', 'inception', 'coordinates']}.items():
                for ind, el in enumerate(va):
                    result = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{el}.json').json()
                    try:
                        wikidata_dict[ke][ind] = {el: result['entities'][f'{el}']['labels']['pl']['value']}
                    except KeyError:
                        wikidata_dict[ke][ind] = {el: result['entities'][f'{el}']['labels']['en']['value']}
            institutions_list_of_dicts[i].update({'wikidata_id': wikidata_dict})
    except KeyError:
        pass


import requests
result = requests.get('https://viaf.org/viaf/151912351/viaf.json').json()
result['occupation']['data']['text'] #occupation
[e['#text'] for e in result['xLinks']['xLink'] if isinstance(e, dict) and 'pl.wikipedia' in e['#text']] #wikipedia pl
[e['@nsid'] for e in result['sources']['source'] if 'WKP' in e['#text']]# wikidata id

result = requests.get('https://www.wikidata.org/wiki/Special:EntityData/Q856423.json').json()
[e['mainsnak']['datavalue']['value']['id'] for e in result['entities']['Q856423']['claims']['P31']] #instance of
[e['mainsnak']['datavalue']['value']['time'] for e in result['entities']['Q856423']['claims']['P571']] #inception
[e['mainsnak']['datavalue']['value']['id'] for e in result['entities']['Q856423']['claims']['P276']] #location
[e['mainsnak']['datavalue']['value']['id'] for e in result['entities']['Q856423']['claims']['P1365']] #replaces





institutions = E.institution()
for institution in tqdm(institutions_list_of_dicts):
    try:
        institutions.append(create_institution(institution))
    except KeyError:
        pass
import_institutions = E.pbl(E.files(institutions))

to_save = ElementTree(import_institutions)
to_save.write("import_institutions.xml", xml_declaration=True, encoding='utf-8')   






















    
    
    
    
    
    
    
    
    
    
    
  














