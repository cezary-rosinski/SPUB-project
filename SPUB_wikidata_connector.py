import requests


def get_wikidata_label(wikidata_id, list_of_languages):
    if not wikidata_id.startswith('Q'):
        wikidata_id = f'Q{wikidata_id}'
    r = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json').json()
    record_languages = set(r.get('entities').get(wikidata_id).get('labels').keys())
    for language in list_of_languages:
        if language in record_languages:
            return r.get('entities').get(wikidata_id).get('labels').get(language).get('value')
        else:
            return r.get('entities').get(wikidata_id).get('labels').get(list(record_languages)[0]).get('value')
        
def get_wikidata_coordinates(wikidata_id):
    if not wikidata_id.startswith('Q'):
        wikidata_id = f'Q{wikidata_id}'
    r = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json').json()
    lon = r.get('entities').get(wikidata_id).get('claims').get('P625')[0].get('mainsnak').get('datavalue').get('value').get('longitude')
    lat = r.get('entities').get(wikidata_id).get('claims').get('P625')[0].get('mainsnak').get('datavalue').get('value').get('latitude')
    return f'{lat},{lon}'