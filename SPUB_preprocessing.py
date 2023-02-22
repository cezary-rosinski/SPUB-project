#%% note
# plik, w którym przetwarzamy i ewentualnie wzbogadamy dane wejściowe otrzymane od MG

#%% import
import json
from concurrent.futures import ThreadPoolExecutor
from SPUB_wikidata_connector import get_wikidata_label, get_wikidata_coordinates
from tqdm import tqdm
import regex as re

#%% def

def preprocess_places(*paths):
    data = []
    for path in paths:
        with open(path, encoding='utf-8') as f:
            data.extend(json.load(f))
    wikidata_ids = set([e.get('wiki') for e in data if e.get('wiki')])
    with ThreadPoolExecutor() as executor:
        wikidata_response = list(tqdm(executor.map(lambda p: get_wikidata_label(p, ['pl', 'en']), wikidata_ids)))
    wikidata_labels = dict(zip(wikidata_ids, wikidata_response))
    data = [dict(e) for e in set([tuple({k:wikidata_labels.get(e.get('wiki'), v) if k == 'name' else v for k,v in e.items() if k != 'recCount'}.items()) for e in data])]
    return data

def preprocess_people(path):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    [e.update({'dateB': e.get('fromWiki', {}).get('dateB')}) for e in data]
    [e.update({'dateD': e.get('fromWiki', {}).get('dateD')}) for e in data]
    data = [{k:e.get('dateB') if k == 'yearBorn' and isinstance(e.get('dateB'), str) else v for k,v in e.items()} for e in data]
    data = [{k:e.get('dateD') if k == 'yearDeath' and isinstance(e.get('dateD'), str) else v for k,v in e.items()} for e in data]
    [e.update({'placeB': e.get('fromWiki', {}).get('placeB')}) for e in data]
    [e.update({'placeD': e.get('fromWiki', {}).get('placeD')}) for e in data]
    return [{k:v for k,v in e.items() if k not in ['dateB', 'dateD', 'fromWiki', 'recCount']} for e in data]

def preprocess_events(path):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    data = [{k:v for k,v in e.items() if k != 'recCount'} for e in data]
    event_dict = {
        'Konkursy': 'competition',
        'Nagrody polskie': 'prize',
        'Nagrody zagraniczne': 'prize',
        'Odznaczenia': 'decoration',
        'Plebiscyty': 'plebiscite',
        'Wystawy': 'exhibition',
        'Zjazdy, festiwale, sesje w Polsce': 'festival',
        'Zjazdy, festiwale, sesje za granicą': 'festival'
    }
    [e.update({'type': [el for el in event_dict if el in e.get('name')][0] if [el for el in event_dict if el in e.get('name')] else None}) for e in data]
    data = [{k:v.replace(e.get('type')+', ','') if k=='name' and e.get('type') else v for k,v in e.items()} for e in data]
    data = [{k:event_dict.get(v) if k == 'type' and v else v for k,v in e.items()} for e in data]
    event_dict2 = {
        'doktorat honoris causa': 'honorary-doctorate',
        'festiwal': 'festival',
        'konferencja': 'conference',
        'konkurs': 'competition',
        'nagroda': 'prize',
        'odznaczenie': 'decoration',
        'plebiscyt': 'plebiscite',
        'spotkanie autorskie': 'authors-meeting',
        'wystawa': 'exhibition'
    }
    data = [{k:event_dict2.get([el for el in event_dict2 if el in e.get('name').lower()][0]) if k=='type' and [el for el in event_dict2 if el in e.get('name').lower()] else v for k,v in e.items()} for e in data]
    data = [{'type_' if k=='type' else k:v for k,v in e.items()} for e in data]
    return data

def preprocess_publishing_series(path):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    data = [e for e in data if '=490' in e.get('fullrecord')]
    data = [e.get('series') for e in data]
    data = set([ele for sub in [[' ; '.join([el.strip() for el in e[0].split(' ; ')][1:])] if len(e) == 1 and re.findall('\d+ \;', e[0]) else e for e in data] for ele in sub])
    data = set([[el.strip() for el in e.split(';')][0] for e in data])
    data = [{'title': e} for e in data]
    return data

#%% kartotek – ver 1
# !!!miejsca!!!

# #plik od MG, który ma miejsca jako tematy – MG ma wygenerować lepszej jakości plik
# with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-15\pub_places.json", encoding='utf-8') as f:
#     data = json.load(f)

# # data = [{k:v.split('|')[0] if k=='name' else e.get('name').split('|')[-1] if k=='wiki' else v for k,v in e.items()} for e in data]

# wikidata_ids = set([e.get('wiki') for e in data if e.get('wiki')])

# with ThreadPoolExecutor() as executor:
#     wikidata_response = list(tqdm(executor.map(lambda p: get_wikidata_label(p, ['pl', 'en']), wikidata_ids)))
    
# # with ThreadPoolExecutor() as executor:
# #     wikidata_coordinates = list(tqdm(executor.map(get_wikidata_coordinates, wikidata_ids)))

# wikidata_labels = dict(zip(wikidata_ids, wikidata_response))
# # wikidata_coordinates = dict(zip(wikidata_ids, wikidata_coordinates))

# data = [dict(e) for e in set([tuple({k:wikidata_labels.get(e.get('wiki'), v) if k == 'name' else v for k,v in e.items() if k != 'recCount'}.items()) for e in data])]
# # data = [dict(e) for e in set([tuple({k:wikidata_coordinates.get(e.get('wiki'), v) if k == 'coordinates' else v for k,v in e.items() if k != 'recCount'}.items()) for e in data])]

# !!!osoby!!!
# with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-08\persons.json", encoding='utf-8') as f:
#     data = json.load(f)

# [e.update({'dateB': e.get('fromWiki', {}).get('dateB')}) for e in data]
# [e.update({'dateD': e.get('fromWiki', {}).get('dateD')}) for e in data]
# data = [{k:e.get('dateB') if k == 'yearBorn' and isinstance(e.get('dateB'), str) else v for k,v in e.items()} for e in data]
# data = [{k:e.get('dateD') if k == 'yearDeath' and isinstance(e.get('dateD'), str) else v for k,v in e.items()} for e in data]
# [e.update({'placeB': e.get('fromWiki', {}).get('placeB')}) for e in data]
# [e.update({'placeD': e.get('fromWiki', {}).get('placeD')}) for e in data]

# #przejmujemy daty z wiki i nadpisujemy yearBorn i yearDeath, jak wiki puste, to zostaje to, co było

# #co jeśli jedna osoba (ten sam wiki id) ma kilka nazw? czy to w ogóle się zdarza?

# # len([e.get('wiki') for e in data if e.get('wiki')])
# # len(set([e.get('wiki') for e in data if e.get('wiki')]))
# data = [{k:v for k,v in e.items() if k not in ['dateB', 'dateD', 'fromWiki', 'recCount']} for e in data]

# !!!wydarzenia!!!

# with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-15\events.json", encoding='utf-8') as f:
#     data = json.load(f)
    
# data = [{k:v for k,v in e.items() if k != 'recCount'} for e in data]

# event_dict = {
#     'Konkursy': 'competition',
#     'Nagrody polskie': 'prize',
#     'Nagrody zagraniczne': 'prize',
#     'Odznaczenia': 'decoration',
#     'Plebiscyty': 'plebiscite',
#     'Wystawy': 'exhibition',
#     'Zjazdy, festiwale, sesje w Polsce': 'festival',
#     'Zjazdy, festiwale, sesje za granicą': 'festival'
# }

# [e.update({'type': [el for el in event_dict if el in e.get('name')][0] if [el for el in event_dict if el in e.get('name')] else None}) for e in data]
# data = [{k:v.replace(e.get('type')+', ','') if k=='name' and e.get('type') else v for k,v in e.items()} for e in data]
# data = [{k:event_dict.get(v) if k == 'type' and v else v for k,v in e.items()} for e in data]

# event_dict2 = {
#     'doktorat honoris causa': 'honorary-doctorate',
#     'festiwal': 'festival',
#     'konferencja': 'conference',
#     'konkurs': 'competition',
#     'nagroda': 'prize',
#     'odznaczenie': 'decoration',
#     'plebiscyt': 'plebiscite',
#     'spotkanie autorskie': 'authors-meeting',
#     'wystawa': 'exhibition'
# }
# data = [{k:event_dict2.get([el for el in event_dict2 if el in e.get('name').lower()][0]) if k=='type' and [el for el in event_dict2 if el in e.get('name').lower()] else v for k,v in e.items()} for e in data]

# data = [{'type_' if k=='type' else k:v for k,v in e.items()} for e in data]

# !!!serie wydawnicze!!!

# path = r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-16\biblio.json"

# with open(path, encoding='utf-8') as f:
#     data = json.load(f)

# series_data = [e for e in data if '=490' in e.get('fullrecord')]
# series_data = [e.get('series') for e in series_data]
# series_data = set([ele for sub in [[' ; '.join([el.strip() for el in e[0].split(' ; ')][1:])] if len(e) == 1 and re.findall('\d+ \;', e[0]) else e for e in series_data] for ele in sub])
# series_data = set([[el.strip() for el in e.split(';')][0] for e in series_data])
# series_data = [{'title': e} for e in series_data]













