import xml.etree.cElementTree as ET
import lxml.etree
from tqdm import tqdm



def generate_XML_file_people(file, xml_nodes):
    


    
    
for i, e in enumerate(people_list_of_dicts):
    if 'Prus' in e['name_simple']:
        print(i)

osoba = people_list_of_dicts[254]

person_name_types = ['main-name',
                     'family-name',
                     'other-last-name-or-first-name',
                     'monastic-name',
                     'codename', 
                     'alias',
                     'group-alias'
                     ]

name_types_dict = {'pseudonym.value':'alias', 
                   'autorLabel.value':'main-name',  
                   'sexLabel.value': 'sex', 
                   'aliasLabel.value': 'other-last-name-or-first-name',
                   'name_simple': 'main-name',
                   'birthNameLabel.value': 'family-name'
                   #dodać nazewnictwo z urodzenia (dla Prusa)
                   }

birth_death_dict = {'birthdate.value',
                    'birthplaceLabel.value',
                    'deathdate.value',
                    'deathplaceLabel.value'
                    #wydobyć kraj
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
        for k,v in dict_data['wikidata_ID'].items():
            if k in name_types_dict:
                v = v.split('❦')
                for val in v:
                    if name_types_dict[k] in person_name_types:
                        name = ET.SubElement(parent, "name", transliteration=transliteration, code=name_types_dict[k])
                    else:
                        name = ET.SubElement(parent, "name", code=name_types_dict.get(k))
                    name.text = val
    except KeyError:
        name = ET.SubElement(parent, "name", transliteration=transliteration, code=name_types_dict['name_simple'])
        name.text = dict_data['name_simple']

#query wikidaty nie działa poprawnie - kraj urodzenia i kraj zgonu!!!        
def create_birth_death(parent, dict_data):
    try:
        for k,v in dict_data['wikidata_ID'].items():
            if k == 'birthplaceLabel.value':
                birth = ET.SubElement(parent, "birth", code="place")
                birth.text = v
            elif
            
        
    except KeyError:


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