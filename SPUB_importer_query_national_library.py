from tqdm import tqdm
import regex as re
import requests


#def

def marc_parser_dict_for_field(string, subfield_code):
    subfield_list = re.findall(f'{subfield_code}.', string)
    dictionary_field = {}
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        string = re.sub(f'({subfield_escape})', r'❦\1', string)
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        regex = f'(^)(.*?\❦{subfield_escape}|)(.*?)(\,{{0,1}})((\❦{subfield_code})(.*)|$)'
        value = re.sub(regex, r'\3', string).strip()
        dictionary_field[subfield] = value
    return dictionary_field

def query_national_library(list_of_people):
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
    return list_of_dicts
            























