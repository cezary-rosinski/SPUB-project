import xml.etree.cElementTree as ET
import lxml.etree
from tqdm import tqdm
import regex as re
import datetime


    
for i, e in enumerate(people_list_of_dicts):
    if 'Sapkowski' in e['name_simple']:
        print(i)
#78, 62, 154, 3, 121
osoba = people_list_of_dicts[37]

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
                   'wikidata_ID.sexLabel.value': 'sex', 
                   'wikidata_ID.aliasLabel.value': 'other-last-name-or-first-name',
                   'name_simple': 'main-name',
                   'wikidata_ID.birthNameLabel.value': 'family-name'
                   }
def today():
    now = datetime.datetime.now()
    year = now.year
    month = '{:02}'.format(now.month)
    day = '{:02}'.format(now.day)
    return f'{year}-{month}-{day}'

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

def create_person(parent, dict_data, status='published'):
    empty_dict = {}
    # empty_dict['id'] = ?
    try:
        empty_dict['viaf'] = dict_data['VIAF_ID']
    except KeyError:
        pass
    empty_dict['status'] = status
    empty_dict['creator'] = 'cezary_rosinski'
    empty_dict['creation-date'] = empty_dict['publishing-date'] = today()
    person = ET.SubElement(parent,'person', empty_dict)
    return person
        
def create_name(parent, dict_data, transliteration='no'):
    try:
        for k,v in dict_data.items():
            if k in name_types_dict:
                v = v.split('❦')
                for val in v:
                    if name_types_dict[k] in person_name_types:
                        name = ET.SubElement(parent, "name", transliteration=transliteration, code=name_types_dict[k])
                    else:
                        name = ET.SubElement(parent, "name", code=name_types_dict[k])
                    name.text = val
    except KeyError:
        name = ET.SubElement(parent, "name", transliteration=transliteration, code=name_types_dict['name_simple'])
        name.text = dict_data['name_simple']

place_dict = {'coordinates.value':'lat_lon',
              'countryLabel.value':'country',
              'endtime.value':'date_to',
              'starttime.value':'date_from',
              'place.value':'id',
              'placeLabel.value':'name',
              'officialName.value':'name',
              'geonamesID.value':'geonames'}
        
def create_place(parent, dict_data, kind='birth'):
    try:
        # dict_data = people_list_of_dicts[3]
        for element in dict_data[f'wikidata_ID.{kind}place.value']:
            list_for_place = []
            for k,v in element.items():
                if k in ['coordinates.value', 'geonamesID.value', 'place.value']:
                    list_for_place.append([place_dict[k], v])
            empty_dict = {}
            for name, value in list_for_place:
                if name == 'lat_lon':
                    empty_dict['lat'] = str(value['coordinates'][0])
                    empty_dict['lon'] = str(value['coordinates'][1])
                else:
                    empty_dict[name] = value
            place = ET.SubElement(parent, 'place', empty_dict)
            list_for_place2 = []
            for el in element['place_name']:
                for k,v in el.items():
                    try:
                        if k in ['starttime.value', 'endtime.value']:
                            try:
                                v = re.findall('.+(?=T)', v)[0]
                            except IndexError:
                                pass
                        list_for_place2.append([place_dict[k], v])
                    except KeyError:
                        pass
                empty_dict2 = {}
                for name, value in list_for_place2:
                    empty_dict2[name] = value
                place_name = ET.SubElement(place, 'name', empty_dict2)        
        return place
    except KeyError:
        pass
              
def create_birth_death_date(parent, dict_data, kind='birth', to_bc='false', uncertain='false', in_words=''):
    try:
        empty_dict = {}
        empty_dict['from'] = re.findall('.+(?=T)', dict_data[f'wikidata_ID.{kind}date.value'])[0]
        if empty_dict['from'][0] == '-':
            empty_dict['from-bc'] = 'true'
        else:
            empty_dict['from-bc'] = 'false'
        empty_dict['to-bc'], empty_dict['uncertain'], empty_dict['in_words'] = to_bc, uncertain, in_words
        date = ET.SubElement(parent, 'date', empty_dict)
        return date
    except KeyError:
        pass

def create_annotation(parent, dict_data):
    pass

def create_remark(parent, dict_data):
    try:
        remark = ET.SubElement(parent, "remark")
        remark.text = dict_data['wikidata_ID.occupationLabel.value']
        return remark
    except KeyError:
        pass
    
def create_tags(parent, dict_data):
    try:
        tags = ET.SubElement(parent, 'tags')
        for element in dict_data['wikidata_ID.genreLabel.value'].split('❦'):
            tag = ET.SubElement(tags, 'tag')
            tag.text = element
        return tags
    except KeyError:
        pass

def create_links(parent, dict_data, kind='external-identifier'):
    try:
        links = ET.SubElement(parent, 'links')
        empty_dict = {}
        for k,v in dict_data.items():
            if k in ['BN_ID', 'VIAF_ID', 'wikidata_ID.author.value']:
                if k == 'BN_ID':
                    v = f"http://data.bn.org.pl/api/authorities.xml?id={v}"
                elif k == 'VIAF_ID':
                    v = f"https://viaf.org/viaf/{v}/"
                empty_dict['access-date'] = today()
                empty_dict['type'] = kind
                link = ET.SubElement(links, 'link', empty_dict)
                link.text = v
        return links
    except KeyError:
        pass
                
    
        
# tree = ET.ElementTree(date)
# tree.write('test2.xml', encoding='UTF-8')       
        
#     except KeyError:


# xml_nodes = create_node_structure(['pbl', 'files', 'people'])
# xml_nodes['person'] = create_person(xml_nodes['people'], osoba)
# xml_nodes['names'] = ET.SubElement(xml_nodes['person'], "names")          
# create_name(xml_nodes['names'], osoba)   
# xml_nodes['birth'] = ET.SubElement(xml_nodes['person'], "birth")       
# create_birth_death_date(xml_nodes['birth'], osoba)
# create_place(xml_nodes['birth'], osoba)
# xml_nodes['death'] = ET.SubElement(xml_nodes['person'], "death")     
# create_birth_death_date(xml_nodes['death'], osoba, kind='death')
# create_place(xml_nodes['death'], osoba, kind='death')
# create_remark(xml_nodes['person'], osoba)
# create_tags(xml_nodes['person'], osoba)
# create_links(xml_nodes['person'], osoba)

# tree = ET.ElementTree(xml_nodes['pbl'])

# # [elem.tag for elem in xml_nodes['pbl'].iter()]

# tree.write('test.xml', encoding='UTF-8')

             


 