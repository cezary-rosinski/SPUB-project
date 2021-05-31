import xml.etree.cElementTree as ET
import lxml.etree
from tqdm import tqdm



def generate_XML_file_people(file, xml_nodes):
    


    
    
for i, e in enumerate(people_list_of_dicts):
    if 'Prus' in e['name_simple']:
        print(i)

osoba = people_list_of_dicts[78]

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
        
def create_name(parent, dict_data, transliteration):
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
        
def create_place(dict_data, kind='birth'):
    dict_data = people_list_of_dicts[3]
    for element in dict_data[f'wikidata_ID.{kind}place.value']:
        list_for_place = []
        for k,v in element.items():
            try:
                test = place_dict[k]
                test2 = v
                list_for_place.append([test, test2])
            except KeyError:
                pass
        empty_dict = {}
        for name, value in list_for_place:
            if name == 'lat_lon':
                empty_dict['lat'] = str(value['coordinates'][0])
                empty_dict['lon'] = str(value['coordinates'][1])
            else:
                empty_dict[name] = value
        place = ET.Element('place', empty_dict)
          
tree = ET.ElementTree(place)
tree.write('test2.xml', encoding='UTF-8')

def create_place_alt_names():
    
     
def create_birth(parent, dict_data, date_from, date_from_bc=False, date_to='', date_to_bc='', date_uncertain='False', date_in_words='', place_id, place_geonames, place_lat, place_lon):
    try:
        for k,v in dict_data.items():
            if k == 'wikidata_ID.birthdate.value':
                
            
            if k == 'birthplaceLabel.value':
                birth = ET.SubElement(parent, "birth", code="place")
                birth.text = v
            elif
            
        
#     except KeyError:


xml_nodes = create_node_structure(['pbl', 'files', 'people'])
xml_nodes['person'] = ET.SubElement(xml_nodes['people'], "person")
xml_nodes['names'] = ET.SubElement(xml_nodes['person'], "names")          
create_name(xml_nodes['names'], osoba, 'no')                    
  
# for osoba in tqdm(people_list_of_dicts):    
#     xml_nodes['person'] = ET.SubElement(xml_nodes['people'], "person") 
#     xml_nodes['names'] = ET.SubElement(xml_nodes['person'], "names")          
#     create_name(xml_nodes['names'], osoba, 'no')

    
tree = ET.ElementTree(xml_nodes['pbl'])
tree.write('test.xml', encoding='UTF-8')






def generate_XML(file):
    pbl = ET.Element("pbl")
    files = ET.SubElement(pbl, "files")
    people = ET.SubElement(files, "people")
    person = ET.SubElement(people, "person", id='id1', creator="c_rosinski").text = 'some text'

    tree = ET.ElementTree(pbl)
    tree.write(file)
    
generate_XML('test.xml')


krasinski = osoby_z_rekordow_lista_slownikow[13]

name_types_dict = {'pseudonym.value':'alias', 'autorLabel.value':'main-name'}

krasinski['wikidata ID'][0].keys()

tablica.get(krasinski['wikidata ID'][0]['pseudonym.value'])

#tablica przekodowania między wikidatą a strukturą xml 

def create_name(parent, data, field_name):
    name = ET.SubElement(parent, "name", transliteration='no', code=name_types_dict.get(field_name)).text = data['wikidata ID'][0][field_name]
    

pbl = ET.Element("pbl")
files = ET.SubElement(pbl, "files")
people = ET.SubElement(files, "people")
person = ET.SubElement(people, "person", id='id1', creator="c_rosinski")
names = ET.SubElement(person, "names")
# name = ET.SubElement(names, "name", transliteration='no', code=name_types_dict.get('autorLabel.value')).text = krasinski['wikidata ID'][0]['autorLabel.value']
# name = ET.SubElement(names, "name", transliteration='no', code=name_types_dict.get('pseudonym.value')).text = krasinski['wikidata ID'][0]['pseudonym.value']
# create_name(names, krasinski, 'autorLabel.value')
# create_name(names, krasinski, 'pseudonym.value')
for element in name_types_dict:
    create_name(names, krasinski, element)

tree = ET.ElementTree(pbl)
tree.write('krasinski.xml', encoding='UTF-8', )
    
person = ET.SubElement(people, "person", id='id1', creator="c_rosinski").text = 'some text'    