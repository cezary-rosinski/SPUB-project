import xml.etree.cElementTree as ET
import lxml.etree
from tqdm import tqdm
import regex as re
import datetime
from collections import Counter




for k, v in ttt.items():
    for e in v['place_names']:
        if e['placeLabel.value'] == 'Gdańsk':
            print(k)
            break

test_place = ttt['http://www.wikidata.org/entity/Q1792']

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

#dodać atrybuty językowe

place_dict = {'coordinates.value':'lat_lon',
              'countryLabel.value':'country',
              'endtime.value':'date-to',
              'starttime.value':'date-from',
              'place.value':'id',
              'placeLabel.value':'name',
              'officialName.value':'name',
              'geonamesID.value':'geonames',
              'officialName.xml:lang':'lang',
              'countryLabel.xml:lang':'country-lang',
              'placeLabel.xml:lang':'lang'}

def create_place(parent, dict_data):
    # parent = xml_nodes['places']
    # dict_data = ttt['http://www.wikidata.org/entity/Q1001225']
    # dict_data = ttt['http://www.wikidata.org/entity/Q1156']
    list_for_place = []
    for key,value in dict_data.items():
        if key in ['coordinates.value', 'geonamesID.value', 'place.value']:
            list_for_place.append([place_dict[key], value])
    empty_dict = {}
    for name, val in list_for_place:
        if name == 'lat_lon':
            empty_dict['lat'] = str(val['coordinates'][0])
            empty_dict['lon'] = str(val['coordinates'][1])
        else:
            empty_dict[name] = val
    place = ET.SubElement(parent, 'place', empty_dict)
    list_for_place2 = []
    for i, el in enumerate(dict_data['place_names']):
        empty_dict2 = {}
        empty_dict3 = {}
        for k,v in el.items():
            if k in ['placeLabel.value', 'countryLabel.value', 'placeLabel.xml:lang', 'countryLabel.xml:lang']:
                empty_dict2.update({place_dict[k]:v})
            if k not in ['placeLabel.value', 'placelabel.xml:lang']:
                try:
                    if k in ['starttime.value', 'endtime.value']:
                        try:
                            v = re.findall('.+(?=T)', v)[0]
                        except IndexError:
                            pass
                    empty_dict3.update({place_dict[k]:v})
                except KeyError:
                    pass
        list_for_place2.append(empty_dict2)
        list_for_place2.append(empty_dict3)
    list_for_place2 = [{k: v for k,v in e.items()} for e in list_for_place2 if e] 
    list_for_place2 = [e for e in list_for_place2 if 'name' in e]
    list_for_place2 = list({v['name']:v for v in list_for_place2 if v['name']}.values()) 
    for ind, x in enumerate(list_for_place2):
        list_for_place2[ind]['id'] = str(ind+1)
    list_for_place2 = [x for ind, x in enumerate(list_for_place2)]
    for element in list_for_place2:
        place_name = ET.SubElement(place, 'name', element)
    return place
        
        

    
xml_nodes = create_node_structure(['pbl', 'files', 'places'])
create_place(xml_nodes['places'], test_place)   
tree = ET.ElementTree(xml_nodes['pbl'])

# [elem.tag for elem in xml_nodes['pbl'].iter()]

tree.write('test.xml', encoding='UTF-8')
   

translator = google_translator()
translator.translate('İstanbul', lang_src='tr', lang_tgt='pl')











xml_nodes = create_node_structure(['pbl', 'files', 'people'])
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

# [elem.tag for elem in xml_nodes['pbl'].iter()]

tree.write('test.xml', encoding='UTF-8')




























