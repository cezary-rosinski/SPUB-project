from tqdm import tqdm
import regex as re
import requests
from my_functions import marc_parser_dict_for_field
from concurrent.futures import ThreadPoolExecutor
from Levenshtein import distance
from collections import ChainMap
from urllib.error import HTTPError
from http.client import RemoteDisconnected
import time
from requests.exceptions import ConnectionError

# executor = ThreadPoolExecutor()
# print(executor._max_workers)


#def

def get_name_from_dict_bn(name):
    return ' '.join([{v for k,v in e.items()}.pop() for e in name]) 

def find_similar_name_in_list(name_a, list_of_names):   
    if any(name_a == e for e in list_of_names):
        return name_a
    else:
        name_a_len = len(name_a.split(' '))
        list_of_names_splitted = [e.split(' ') for e in list_of_names]
        temp_dict = {}
        for i, sublist in enumerate(list_of_names_splitted):
            similarity_lvls = []
            if len(sublist) >= name_a_len:
                iteration = 0
                while iteration + name_a_len <= len(sublist):
                    similarity_lvls.append(distance(name_a, ' '.join(sublist[iteration:iteration+name_a_len])))
                    iteration +=1
                temp_dict[list_of_names[i]] = min(similarity_lvls)
            else:
                similarity_lvls.append(distance(name_a, ' '.join(sublist)))
                temp_dict[list_of_names[i]] = min(similarity_lvls)    
        return min(temp_dict, key=temp_dict.get)
    
def get_viaf_id_bn(person_dict):
    # person_dict = people_dict[k]
    field_024 = [e for e in person_dict['BN_result']['marc']['fields'] if '024' in e]
    try:
        return dict(ChainMap(*[e for e in field_024 if 'viaf' in dict(ChainMap(*e['024']['subfields'])).values()][0]['024']['subfields']))['a']
    except IndexError:
        pass

def add_bn_viaf_id(dictionary):
    if not(isinstance(dictionary['BN_result'], dict)):
        dictionary['BN_result'] = [e for e in dictionary['BN_result'] if e['name'] == find_similar_name_in_list(dictionary['simple_name'], [el['name'] for el in dictionary['BN_result']])][0]
    dictionary['BN_id'] = dictionary['BN_result']['id']
    dictionary['VIAF_ID'] = get_viaf_id_bn(dictionary)    
    
def return_people_dict_bn(people_list):
    people_dict = {}
    errors = []
    for person in tqdm(people_list):
        person_dict = {}
        person_dict['name_MARC21'] = person
        person = marc_parser_dict_for_field(person, '\$')
        person_dict['simple_name'] = get_name_from_dict_bn(person)
        people_dict[person_dict['name_MARC21']] = person_dict
    
    def query_bn(person_key, kind='person', name_dict=None):
    # person_key = '$aBeylin, Karolina$d(1899-1977)'
        if not name_dict:
            name_dict = marc_parser_dict_for_field(people_dict[person_key]['name_MARC21'], '\$')
        url = 'https://data.bn.org.pl/api/authorities.json?'
        params = {'name': get_name_from_dict_bn(name_dict).replace('?', ''), 'limit': '100', 'kind': kind}
        result = requests.get(url, params = params).json()
        authorities = result['authorities']  
        if not result['authorities']:
            name_dict = name_dict[:-1]
            if name_dict:
                query_bn(person_key, kind=kind, name_dict=name_dict)
            else: errors.append(person_key)
        else:
            while result['nextPage'] != '':
                result = requests.get(result['nextPage']).json()
                authorities.extend(result['authorities'])
            #czy mogę zwracać indeks 0 z automatu?
            people_dict[person_key]['BN_result'] = authorities
    
    # for k in tqdm(people_dict):
    #     query_bn(k)  
       
    with ThreadPoolExecutor() as executor:
        list(tqdm(executor.map(query_bn,people_dict.keys()), total=len(people_dict)))
    
    people_dict_error = {k:v for k,v in people_dict.items() if k in errors}
    people_dict = {k:v for k,v in people_dict.items() if k not in errors}
    
    [add_bn_viaf_id(people_dict[e]) for e in tqdm(people_dict)]

    return (people_dict, people_dict_error)
            

























