from SPUB_importer_read_data import read_MARC21
from my_functions import marc_parser_dict_for_field, simplify_string
from tqdm import tqdm
import fuzzywuzzy
import pandas as pd

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
#%% load

country_codes = pd.read_excel('translation_country_codes.xlsx')
country_codes = [list(e[-1]) for e in country_codes.iterrows()]
country_codes = dict(zip([e[0] for e in country_codes], [{'MARC_name': e[1], 'iso_alpha_2': e[2], 'Geonames_name': e[-1]} for e in country_codes]))
#%% main

records = read_mrk('bn_harvested_2021_05_12.mrk')
records[0]

test = [{k:v for k,v in e.items() if k in ['001', '008', '260']} for e in records]
test[0]

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
places = set([''.join([l for l in el if l.isalnum() or l in['-', ' ', '(', ')']]).strip() for el in places])





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