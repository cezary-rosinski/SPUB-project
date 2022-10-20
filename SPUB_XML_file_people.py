import xml.etree.cElementTree as ET
import lxml.etree
from tqdm import tqdm
import regex as re
from datetime import datetime

from lxml.builder import E
from lxml.etree import ElementTree, tostring

from my_functions import gsheet_to_df

import hashlib
import random
    
# for i, e in enumerate(people_list_of_dicts):
#     if 'Kapuściński' in e['name_simple']:
#         print(i)
#78, 62, 154, 3, 121
# osoba = people_list_of_dicts[388]

# [i for i, e in enumerate(people_list_of_dicts) if 'VIAF_ID' not in e][0]
        
dzialy_pbl = gsheet_to_df('14hkWimoH7iBit_yMAkxmEGGI8vQeZslPDHzslNVSzDQ', 'SPUB_Nowa struktura działów')[['string uproszczony', 'MD5']].to_dict(orient='records')

person_name_types = ['main-name',
                     'family-name',
                     'other-last-name-or-first-name',
                     'monastic-name',
                     'codename', 
                     'alias',
                     'group-alias'
                     ]

name_types_dict = {'wikidata_ID.pseudonym.value':'alias', 
                   'wikidata_ID.autorLabel.value':'main-name',  
                   'wikidata_ID.aliasLabel.value': 'other-last-name-or-first-name',
                   'name_simple': 'main-name',
                   'wikidata_ID.birthNameLabel.value': 'family-name'
                   }

sex_dict = {'mężczyzna': 'male',
            'kobieta': 'female'}

#names

def create_names(dict_data, transliteration='no'):
    names = E.names()
    try:
        for k,v in dict_data.items():
            if k in name_types_dict:
                v = list(set(v.split('❦')))
                for val in v:
                    if name_types_dict[k] in person_name_types:
                        names.append(E.name(val, transliteration=transliteration, code=name_types_dict[k]))
                    else:
                        names.append(E.name(val, code=name_types_dict[k]))
    except KeyError:
        names.append(E.name(val, transliteration=transliteration, code=name_types_dict['name_simple']))
    return names
    
# test = create_names(osoba)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode())   


def create_sex(dict_data):
    try:
        return E.sex(value=sex_dict[dict_data['wikidata_ID.sexLabel.value']])
    except KeyError:
        pass
    
# test = create_sex(osoba)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode())  


# def create_headings(dict_data):
    

# jak spiąć to='', to_bc='false', uncertain='false', in_words='' z wikidatą
# znaleźć na wikidacie przykłady ludzi urodzonych w BC lub z frazą "połowa XVII w."
        
    
# jeśli jest kilka dat, to ma być jeden tag <date>, jeśli wikidata daje kilka informacji, to lepiej dać from i to i uncertain=True
def create_birth_or_death(dict_data, kind='birth'):
    try:
        date_type = E.birth()
        dates = dict_data[f'wikidata_ID.{kind}date.value'].split('❦')
        if len(dates) == 1:
            date = datetime.strptime(dates[0], "%Y-%m-%dT%H:%M:%SZ").date().strftime("%Y-%m-%d")
            temp_dict = {'from':date, 'from-bc':'false', 'uncertain':'false'}
        else:
            dates = sorted([datetime.strptime(e, "%Y-%m-%dT%H:%M:%SZ").date() for e in dates])
            dates = [e.strftime("%Y-%m-%d") for e in dates]
            temp_dict = {'from':dates[0], 'from-bc':'false', 'to':dates[-1], 'to-bc':'false', 'uncertain':'true'}
        date_type.append(E.date(temp_dict))
        date_type.append(E.place(dict_data[f'wikidata_ID.{kind}place.value']))
        if kind == 'death':
            date_type.tag = 'death'
        return date_type
    except KeyError:
        pass

# test = create_birth_or_death(osoba, kind='death')
# print(tostring(test, pretty_print=True, encoding='utf-8').decode())  
    
  

# przypisanie działów przenieść na wcześniejszy etap i dodać do słownika osoby, a tutaj odwoływać się już do słownika
def create_headings():
    list_of_md5 = [e['MD5'] for e in dzialy_pbl if e['string uproszczony'] == '/literatura polska/hasła osobowe']
    list_of_md5.extend([e['MD5'] for e in random.choices(dzialy_pbl, k=2)])
    headings = E.headings()
    for element in list_of_md5:
        headings.append(E.heading({'id':element}))
    return headings

# test = create_headings()
# print(tostring(test, pretty_print=True, encoding='utf-8').decode())  

def create_annotation(dict_data):
    try:
        return E.annotation(dict_data['bio'])
    except KeyError:
        pass

# test = create_annotation(osoba)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode())  

def create_remark(dict_data):
    try:
        return E.remark(dict_data['wikidata_ID.occupationLabel.value'])
    except KeyError:
        pass

# test = create_remark(osoba)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode()) 

def create_tags(dict_data):
    try:
        tags = E.tags()
        for element in dict_data['wikidata_ID.genreLabel.value'].split('❦'):
            tags.append(E.tag(element))
        return tags     
    except KeyError:
        pass
    
# test = create_tags(osoba)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode()) 

def create_links(dict_data, kind='external-identifier'):
    links = E.links()
    links_dict = {}
    for k,v in dict_data.items():
        if k in ['BN_ID', 'VIAF_ID', 'wikidata_ID.author.value']:
            if k == 'BN_ID':
                v = f"http://data.bn.org.pl/api/authorities.xml?id={v}"
            elif k == 'VIAF_ID':
                v = f"https://viaf.org/viaf/{v}/"
            links_dict['access-date'] = datetime.today().date().strftime("%Y-%m-%d")
            links_dict['type'] = kind    
            links.append(E.link(v, links_dict))
    return links
                
# test = create_links(osoba, kind='external-identifier')
# print(tostring(test, pretty_print=True, encoding='utf-8').decode())                 
 
# połączyć wszystko + dodać excepty
# przepuścić całą listę osób

def create_person(dict_data):
    person_id = dict_data['BN_ID']
    person_status = 'published'
    person_creator = 'cezary_rosinski'
    creation_date = datetime.today().date().strftime("%Y-%m-%d")
    publishing_date = datetime.today().date().strftime("%Y-%m-%d")
    # person_origin = ? może wikidata?
    person_dict = {'id': person_id, 'status': person_status, 'creator': person_creator, 'creation-date': creation_date, 'publishing-date': publishing_date}
    try:
        person_viaf = dict_data['VIAF_ID']
        person_dict.update({'viaf': person_viaf})
    except KeyError:
        pass
    person = E.person(person_dict)
    for element in [create_names(dict_data), create_sex(dict_data), create_birth_or_death(dict_data), create_birth_or_death(dict_data, kind='death'), create_headings(), create_annotation(dict_data), create_remark(dict_data), create_tags(dict_data), create_links(dict_data)]:
        if element is not None: person.append(element)
    return person
    
# test = create_person(osoba)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode())       

    


# people = E.people()
# for person in tqdm(people_list_of_dicts):
#     try:
#         people.append(create_person(person))
#     except KeyError:
#         pass
# import_people = E.pbl(E.files(people))
# to_save = ElementTree(import_people)
# to_save.write("import_people.xml", xml_declaration=True, encoding='utf-8')     



# people.append(create_person(osoba))
# print(tostring(people, pretty_print=True, encoding='utf-8').decode())      




# print(tostring(import_people, pretty_print=True, encoding='utf-8').decode())   




















             


 