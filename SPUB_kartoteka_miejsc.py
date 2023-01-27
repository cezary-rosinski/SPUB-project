import json
from SPUB_wikidata_connector import get_wikidata_label
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import xml.etree.cElementTree as ET
from datetime import datetime
from SPUB_functions import give_fake_id



#%% main

#plik od MG, który ma miejsca jako tematy – MG ma wygenerować lepszej jakości plik
with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-01-17\pub_places.json", encoding='utf-8') as f:
    data = json.load(f)

wikidata_ids = set([e.get('wiki') for e in data if e.get('wiki')])

with ThreadPoolExecutor() as executor:
    wikidata_response = list(tqdm(executor.map(lambda p: get_wikidata_label(p, ['pl', 'en']), wikidata_ids)))

wikidata_labels = dict(zip(wikidata_ids, wikidata_response))

data = [dict(e) for e in set([tuple({k:wikidata_labels.get(e.get('wiki'), v) if k == 'name' else v for k,v in e.items() if k != 'recCount'}.items()) for e in data])]
    
class Place:
    
    def __init__(self, id_, lat, lon, geonames='', name=''):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"
        self.lat = lat
        self.lon = lon
        self.geonames = geonames
        
        self.periods = [self.PlacePeriod(name=name)]
        
    class PlacePeriod:
        
        def __init__(self, date_from='', date_to='', name='', country='', lang='pl'):
            self.date_from = date_from
            self.date_to = date_to
            self.name = name
            self.country = country
            self.lang = lang    
            
        def __repr__(self):
            return "PlacePeriod('{}', '{}', '{}', '{}', '{}')".format(self.date_from, self.date_to, self.name, self.country, self.lang)
        
        def to_xml(self):
            period_xml = ET.Element('period', {'date_from': self.date_from, 'date_to': self.date_to})
            for attr_tag, value in zip(['name', 'country'], [self.name, self.country]):
                if value:
                    ET.SubElement(period_xml, attr_tag, {'lang': self.lang}).text = value
            return period_xml
            
    @classmethod
    def from_dict(cls, place_dict):
        id_ = place_dict.get('wiki')
        lat, lon = place_dict.get('coordinates').split(',') if place_dict.get('coordinates') else ['', '']
        name = place_dict.get('name')
        return cls(id_, lat, lon, name=name)
    
    def to_xml(self):
        place_xml = ET.Element('place', {k:v for k,v in self.__dict__.items() if k != 'periods'})
        for period in self.periods:
            place_xml.append(period.to_xml())
        return place_xml
        
places = [Place.from_dict(e) for e in data]
give_fake_id(places)

# for place in places:
#     print(place.__dict__)

places_xml = ET.Element('pbl')
files_node = ET.SubElement(places_xml, 'files')
places_node = ET.SubElement(files_node, 'places')
for place in places:
    places_node.append(place.to_xml())

tree = ET.ElementTree(places_xml)

ET.indent(tree, space="\t", level=0)
tree.write(f'import_places_{datetime.today().date()}.xml', encoding='UTF-8')



























