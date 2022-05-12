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
from my_functions import gsheet_to_df

# dict_data = [e for e in institutions_list_of_dicts if 'Ossolineum' in e['name_simple']][0]



#types type

types_main_codes = ['polish', 'polish-abroad', 'foreign']

types_codes_dict = {'Biblioteka': 'library',
'Biblioteki': 'library',
'Biblioteki akademickie': 'library',
'Biblioteki cyfrowe': 'library',
'Biblioteki dla chorych i niepełnosprawnych': 'library',
'Biblioteki ekonomiczne': 'library',
'Biblioteki fachowe': 'library',
'Biblioteki gminne': 'library',
'Biblioteki historyczne': 'library',
'Biblioteki klasztorne': 'library',
'Biblioteki kościelne': 'library',
'Biblioteki medyczne': 'library',
'Biblioteki miejskie': 'library',
'Biblioteki muzealne': 'library',
'Biblioteki narodowe': 'library',
'Biblioteki naukowe': 'library',
'Biblioteki pedagogiczne': 'library',
'Biblioteki pedgaogiczne': 'library',
'Biblioteki polonijne': 'library',
'Biblioteki powiatowe': 'library',
'Biblioteki prywatne': 'library',
'Biblioteki publiczne': 'library',
'Biblioteki regionalne': 'library',
'Biblioteki specjalne': 'library',
'Biblioteki szkolne': 'library',
'Biblioteki uniwersyteckie': 'library',
'Biblioteki wojewódzkie': 'library',
'Fundacja': 'foundation',
'Fundacja rodzinna': 'foundation',
'Fundacje': 'foundation',
'Fundacje artystyczne': 'foundation',
'Fundacje i stowarzyszenia': 'foundation',
'Fundacje korporacyjne': 'foundation',
'Fundacje polskie': 'foundation',
'Grupy literackie': 'literary-group',
'Instytucje kulturalne': 'cultural-institution',
'Instytucje kultury': 'cultural-institution',
'Instytucje państwowe': 'cultural-institution',
'Instytucje społeczne': 'cultural-institution',
'Instytucje użyteczności publicznej': 'cultural-institution',
'instytucje publiczne': 'cultural-institution',
'Instytut naukowy': 'non-university-institute',
'Instytuty Naukowe': 'non-university-institute',
'Instytuty badawcze': 'non-university-institute',
'Instytuty naukowe': 'non-university-institute',
'instytuty naukowe': 'non-university-institute',
'Komitety naukowe': 'non-university-institute',
'Towarzystwa naukowe': 'non-university-institute',
'Instytuty': 'instutute',
'Instytuty świeckie': 'institute',
'Administacja publiczna': 'public-administration-unit',
'Administracja': 'public-administration-unit',
'Administracja państwowa': 'public-administration-unit',
'Administracja publiczna': 'public-administration-unit',
'Administracja rządowa': 'public-administration-unit',
'Administracja samorzadowa': 'public-administration-unit',
'Administracja samorządowa': 'public-administration-unit',
'Instytucje administracji państwowej': 'public-administration-unit',
'Instytucje administracji publicznej': 'public-administration-unit',
'Jednostki administracji państwowej': 'public-administration-unit',
'Jednostki administracji samorządowej': 'public-administration-unit',
'Jednostki administracyjne': 'public-administration-unit',
'Organy administracji państwowej': 'public-administration-unit',
'Urząd administracji publicznej': 'public-administration-unit',
'Urzędy administacji państwowej': 'public-administration-unit',
'Urzędy administracji państwowej': 'public-administration-unit',
'Urzędy administracji państwowej ': 'public-administration-unit',
'Urzędy administracji publicznej': 'public-administration-unit',
'Urzędy administracji publicznej.': 'public-administration-unit',
'Muzea': 'museum',
'Muzea archeologiczne': 'museum',
'Muzea biograficzne': 'museum',
'Muzea etnograficzne': 'museum',
'Muzea farmacji': 'museum',
'Muzea geologiczne': 'museum',
'Muzea historyczne': 'museum',
'Muzea i zbiory kościelne': 'museum',
'Muzea kościelne': 'museum',
'Muzea literatury': 'museum',
'Muzea lnictwo': 'museum',
'Muzea morskie': 'museum',
'Muzea muzyki': 'museum',
'Muzea narodowe': 'museum',
'Muzea narracyjne': 'museum',
'Muzea prywatne': 'museum',
'Muzea regionalne': 'museum',
'Muzea rolnictwa': 'museum',
'Muzea sztuki': 'museum',
'Muzea teatralne': 'museum',
'Muzea techniki': 'museum',
'Muzea uczelniane': 'museum',
'Muzea wirtualne': 'museum',
'Muzea wojskowe': 'museum',
'Muzeum': 'museum',
'Zbiory i muzea kościelne': 'museum',
'Teatr': 'theatre',
'Teatr amatorski': 'theatre',
'Teatr angielski': 'theatre',
'Teatr bośniacki': 'theatre',
'Teatr dziecięcy i młodzieżowy': 'theatre',
'Teatr francuski': 'theatre',
'Teatr lalek': 'theatre',
'Teatr litewski': 'theatre',
'Teatr muzyczny': 'theatre',
'Teatr młodzieżowy': 'theatre',
'Teatr niemiecki': 'theatre',
'Teatr operowy': 'theatre',
'Teatr polski': 'theatre',
'Teatr rosyjski': 'theatre',
'Teatr tańca': 'theatre',
'Teatr włoski': 'theatre',
'Teatr łotewski': 'theatre',
'Teatry': 'theatre',
'Teatry (instytucje)': 'theatre',
'Teatry amatorskie': 'theatre',
'Teatry studenckie': 'theatre',
'Uniwersytety': 'university',
'Uniwersytety dziecięce': 'university',
'Uniwersytety katolickie': 'university',
'Uniwersytety ludowe': 'university',
'Uniwersytety powszechne': 'university',
'Uniwersytety trzeciego wieku': 'university',
'uniwersytety': 'university',
'Uczelnie Wyższe': 'university',
'Uczelnie wyższe': 'university',
'Wyższe uczelnie': 'university',
'Szkoła wyższa': 'university',
'Szkoły wyższe': 'university',
'Instytucje (wydawnictwa)': 'publishing-house',
'Wydawnictwa': 'publishing-house',
'Wydawnictwa (Instytucje)': 'publishing-house',
'Wydawnictwa (firmy)': 'publishing-house',
'Wydawnictwa (instytucja)': 'publishing-house',
'Wydawnictwa (instytucje)': 'publishing-house',
'Wydawnictwa (instytucje0': 'publishing-house',
'Wydawnictwa (intytucje)': 'publishing-house',
'Wydawnictwa fachowe': 'publishing-house',
'Wydawnictwa instytucje': 'publishing-house',
'Wydawnictwa naukowe': 'publishing-house',
'Wydawnictwo': 'publishing-house',
'Wydawnictwo (Instytucja)': 'publishing-house',
'Wydawnictwo (Instytucje)': 'publishing-house',
'Wydawnictwo (firma)': 'publishing-house',
'Wydawnictwo (instytucja)': 'publishing-house',
'Wydawnictwo (instytucja) ': 'publishing-house',
'Wydawnictwo (instytucja) (Instytucje)': 'publishing-house',
'Wydawnictwo (instytucja) naukowe': 'publishing-house',
'Wydawnictwo (instytucje)': 'publishing-house',
'wydawnictwa': 'publishing-house',
'Wytwórnie filmowe': 'film-company',
'Związki zawodowe': 'creative-association',
'Związki zawodowe.': 'creative-association',
'Związki zawodowy': 'creative-association',
'Związki Zawodowe': 'creative-association',
'Związki zaqwodowe': 'creative-association',
'Centra i kluby integracji społecznej': 'creative-association',
'Dyskusyjne Kluby Książki': 'creative-association',
'Kluby': 'creative-association',
'Kluby dyskusyjne': 'creative-association',
'Kluby seniora': 'creative-association',
'Kluby studenckie': 'creative-association',
'Koła naukowe': 'creative-association',
'Koła zainteresowań': 'creative-association',
'Studenckie Koła Naukowe': 'creative-association',
'Studenckie koła naukowe': 'creative-association',
'Stowarzyszenia artystyczne': 'creative-association',
'Stowarzyszenia kulturalne': 'creative-association',
'Stowarzyszenia naukowe': 'creative-association',
'Stowarzyszenia regionalne': 'creative-association'}
#jeśli będzie występował błąd lub brak danych, to dać 'no-data'

# na sztywno określony typ jako "instytucja polska", zobaczyć, czy da się to zrobić dynamicznie
# może być kilka różnych 'codes'
def create_types(dict_data, main_code='polish'):
    types = E.types()
    if main_code == 'polish':
        try:
            type_code = [e['368']['subfields'][0] for e in dict_data['marc'][0] if '368' in e]
            type_code = [e['a'] for e in type_code]
            for element in type_code:
                try:
                    types.append(E.type({'main-code':main_code, 'code': types_codes_dict[element]}))
                except KeyError:
                    types.append(E.type({'main-code':main_code, 'code': 'no-data'}))
        except IndexError:
            types.append(E.type({'main-code':main_code, 'code': 'no-data'}))
    return types
            
            
# test = create_types(dict_data)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode())          

#newest-name

def create_newest_name(dict_data):
    newest_name = dict_data['name_simple']
    return E.newest_name(newest_name)

# test = create_newest_name(dict_data)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode())  

#history period date 046

#!!!!!!!!!!!!!! uwzględnić historyczne następstwo !!!!!!!!!!!!!!!!!!!!
# cała sekcja history period jest do rozbudowania

def create_date(dict_data):
    try:
        date = [e['046']['subfields'][0]['q'] for e in dict_data['marc'][0] if '046' in e][0]
        date = datetime.strptime(date, "%Y").date().strftime("%Y-%m-%d")
        return E.date({'from': date, 'from-bc': 'False', 'uncertain': 'False'})
    except IndexError:
        pass

# test = create_date(dict_data)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode())  

#history period names name 410
def create_names(dict_data):
    names = E.names()
    names.append(E.name(dict_data['name_simple'], {'type':'base', 'newest':'true'}))
    other_names = [e['410']['subfields'][0] for e in dict_data['marc'][0] if '410' in e]
    other_names = list(set([e['a'] for e in other_names]))
    for element in other_names:
        names.append(E.name(element, {'type': 'other'}))
    return names

# test = create_names(dict_data)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode())  

#history period places place 370

temp_places_dict = {'Kielce (woj. świętokrzyskie)': '39156130882758310393',
'Warszawa (woj. mazowieckie)': '4961156133213358430007',
'Wrocław (woj. dolnośląskie)': '2546156133228658430005',
'Poznań (woj. wielkopolskie)': '9890156133203058430002',
'Toruń (woj. kujawsko-pomorskie)': '3433157416831916710001'}

# df_places = gsheet_to_df('1Ruu8fa-wzZ2fwj86S4UhWn_J3_xREjaw_B-P_B7OOvs', 'out')

def create_places(dict_data):
    places = E.places()
    places_list = [e['370']['subfields'][0] for e in dict_data['marc'][0] if '370' in e]
    places_list = [e['e'] for e in places_list if 'e' in e]
    for place in places_list:
        place = temp_places_dict[place]
        place = df_places.loc()[(df_places['viaf'].notnull()) & (df_places['viaf'] == place)][['id', 'date-from', 'date-to', 'place name lang']].iloc[-1:].squeeze(axis=0)
        try:
            date_from = datetime.strptime(place['date-from'], "%Y-%m-%dT%H:%M:%SZ").date().strftime("%Y-%m-%d")
        except TypeError:
            date_from = ''
        try:
            date_to = datetime.strptime(place['date-to'], "%Y-%m-%dT%H:%M:%SZ").date().strftime("%Y-%m-%d")
        except TypeError:
            date_to = ''
        places.append(E.place({'id': place['id'], 'period': '❦'.join([date_from, date_to]), 'lang': place['place name lang']}))
    if places: return places

# test = create_places(dict_data)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode())  

def create_history_period(dict_data):
    history_period = E.history(E.period())
    for element in [create_date(dict_data), create_names(dict_data), create_places(dict_data)]:
        if element is not None: 
            period = history_period.find('period')
            period.append(element)
    return history_period

# test = create_history_period(dict_data)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode()) 
    
# test = [sub['marc'] for sub in institutions_list_of_dicts if 'marc' in sub]
# test = [e for e in test if e]
# test = [e for sub in test for e in sub]
# test = [[f['370']['subfields'][0] for f in e if '370' in f] for e in test]        
# test = [e for e in test if e]
# test = [[f['e'] for f in e if 'e' in f] for e in test]
# test = list(set([e for sub in test for e in sub if sub]))

#parent date
#subordinates subordinate-institution

#headings heading
# do poprawy
# dzialy_pbl = gsheet_to_df('14hkWimoH7iBit_yMAkxmEGGI8vQeZslPDHzslNVSzDQ', 'Nowa struktura działów')[['string uproszczony', 'MD5']].to_dict(orient='records')

def create_headings():
    list_of_md5 = [e['MD5'] for e in random.choices(dzialy_pbl, k=3)]
    headings = E.headings()
    for element in list_of_md5:
        headings.append(E.heading({'id':element}))
    return headings

# test = create_headings()
# print(tostring(test, pretty_print=True, encoding='utf-8').decode()) 
#annotation 665

def create_annotation(dict_data):
    annotations = [e['665']['subfields'][0] for e in dict_data['marc'][0] if '665' in e]
    annotations = [e['a'] for e in annotations if 'a' in e]
    annotations = '\n'.join(annotations)
    return E.annotation(annotations)

# test = create_annotation(dict_data)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode())     

    
#remarks
#tags tag 667

def create_tags(dict_data):
    try:
        tags = E.tags()
        tags_list = [e['667']['subfields'][0] for e in dict_data['marc'][0] if '667' in e]
        tags_list = [e['a'] for e in tags_list if 'a' in e]
        for tag in tags_list:
            tags.append(E.tag(tag)) 
        return tags
    except KeyError:
        pass
    
# test = create_tags(dict_data)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode()) 
    
#links link 670

def create_links(dict_data, kind='external-identifier'):
    links = E.links()
    links_dict = {}
    for k,v in dict_data.items():
        if k in ['BN_ID', 'VIAF_ID']:
            if k == 'BN_ID':
                v = f"http://data.bn.org.pl/api/authorities.xml?id={v}"
            elif k == 'VIAF_ID':
                v = f"https://viaf.org/viaf/{v}/"
            links_dict['access-date'] = datetime.today().date().strftime("%Y-%m-%d")
            links_dict['type'] = kind    
            links.append(E.link(v, links_dict))
    links_dict = {}
    link_list = [e['670']['subfields'] for e in dict_data['marc'][0] if '670' in e]
    link_list = [e['u'] for sub in link_list for e in sub if 'u' in e]
    for link in link_list:
        links_dict['access-date'] = datetime.today().date().strftime("%Y-%m-%d")
        links_dict['type'] = 'broader-description-access'    
        links.append(E.link(link, links_dict))
    return links

# test = create_links(dict_data)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode())

def create_institution(dict_data):
    institution_id = dict_data['BN_ID']
    institution_status = 'published'
    institution_creator = 'cezary_rosinski'
    creation_date = datetime.today().date().strftime("%Y-%m-%d")
    publishing_date = datetime.today().date().strftime("%Y-%m-%d")
    # person_origin = ? może wikidata?
    institution_dict = {'id': institution_id, 'status': institution_status, 'creator': institution_creator, 'creation-date': creation_date, 'publishing-date': publishing_date}
    try:
        institution_viaf = dict_data['VIAF_ID']
        institution_dict.update({'viaf': institution_viaf})
    except KeyError:
        pass
    institution = E.institution(institution_dict)
    for element in [create_types(dict_data), create_newest_name(dict_data), create_history_period(dict_data), create_headings(), create_annotation(dict_data), create_tags(dict_data), create_links(dict_data)]:
        if element is not None: institution.append(element)
    return institution


# test = create_institution(dict_data)
# print(tostring(test, pretty_print=True, encoding='utf-8').decode()) 


# institutions = create_institution(dict_data)
# import_people = E.pbl(E.files(institutions))
# print(tostring(import_people, pretty_print=True, encoding='utf-8').decode()) 


# institutions = E.institution()
# for institution in tqdm(institutions_list_of_dicts):
#     try:
#         institutions.append(create_institution(institution))
#     except KeyError:
#         pass
# import_institutions = E.pbl(E.files(institutions))

# to_save = ElementTree(import_institutions)
# to_save.write("import_institutions.xml", xml_declaration=True, encoding='utf-8')   






























