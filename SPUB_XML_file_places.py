import xml.etree.cElementTree as ET
import lxml.etree
from tqdm import tqdm
import regex as re
import datetime
from collections import Counter




# for k, v in ttt.items():
#     for e in v['place_names']:
#         try:
#             if e['placeLabel.value'] == 'Gdańsk':
#                 print(k)
#                 break
#         except KeyError:
#             pass

# test_place = ttt['http://www.wikidata.org/entity/Q1792']

def create_node_structure(xml_nodes_names):
    xml_nodes = {}
    for id, name in enumerate(xml_nodes_names):
        if id == 0:
            element = ET.Element(name)
            xml_nodes[name] = element
        else:
            subelement = ET.SubElement(xml_nodes[xml_nodes_names[id-1]], name)
            xml_nodes[name] = subelement
    return xml_nodes


place_dict = {'coordinates.value':'lat_lon',
              # 'countryLabel.value':'country',
              'endtime.value':'date-to',
              'starttime.value':'date-from',
              'place.value':'id',
              # 'placeLabel.value':'name',
              # 'officialName.value':'name',
              'geonamesID.value':'geonames',
              'officialName.xml:lang':'lang',
              'countryLabel.xml:lang':'lang',
              'placeLabel.xml:lang':'lang'}

def create_place(parent, dict_data):
    # parent = xml_nodes['places']
    # dict_data = ttt['http://www.wikidata.org/entity/Q1001225']
    # dict_data = ttt['http://www.wikidata.org/entity/Q1156']
    # dict_data = test_place.copy()
    geo_dict = {}
    for key, value in dict_data.items():
        if key in ['geonamesID.value', 'place.value']:
            geo_dict[place_dict[key]] = value
        elif key == 'coordinates.value':
            geo_dict['lat'] = str(value['coordinates'][0])
            geo_dict['lon'] = str(value['coordinates'][1])
    place = ET.SubElement(parent, 'place', geo_dict)
    for ke, va in dict_data['place_dates'].items():
        empty_dict2 = {}
        empty_dict2['date-from'] = ke.split('|')[0]
        empty_dict2['date-to'] = ke.split('|')[-1]
        period = ET.SubElement(place, 'period', empty_dict2)
        list_of_place_names, list_of_countries_names = [], []
        for i, el in enumerate(va):
            for k,v in el.items():
                if k == 'placeLabel.xml:lang':
                    list_of_place_names.append([place_dict[k], v, el['placeLabel.value']])
                elif k == 'officialName.xml:lang':
                    list_of_place_names.append([place_dict[k], v, el['officialName.value']])
                elif k == 'countryLabel.xml:lang':
                    list_of_countries_names.append([place_dict[k], v, el['countryLabel.value']])
            list_of_place_names = [list(x) for x in set(tuple(x) for x in list_of_place_names)]
            list_of_countries_names = [list(x) for x in set(tuple(x) for x in list_of_countries_names)]
        for element in list_of_place_names:
            pl_name = ET.SubElement(period, 'name', {element[0]:element[1]})
            pl_name.text = element[-1]
        for element in list_of_countries_names:
            pl_country = ET.SubElement(period, 'country', {element[0]:element[1]})
            pl_country.text = element[-1]
                    
    
# xml_nodes = create_node_structure(['pbl', 'files', 'places'])
# create_place(xml_nodes['places'], test_place)   
# tree = ET.ElementTree(xml_nodes['pbl'])

# [elem.tag for elem in xml_nodes['pbl'].iter()]

tree.write('test.xml', encoding='UTF-8')


xml_nodes = create_node_structure(['pbl', 'files', 'places'])  
for k, v in ttt.items():
    create_place(xml_nodes['places'], v)   
tree = ET.ElementTree(xml_nodes['pbl'])
tree.write('test.xml', encoding='UTF-8')
    
    


# translator = google_translator()
# translator.translate('İstanbul', lang_src='tr', lang_tgt='pl')





































