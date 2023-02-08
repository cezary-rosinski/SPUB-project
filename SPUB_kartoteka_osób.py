import json
from SPUB_wikidata_connector import get_wikidata_label
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import xml.etree.cElementTree as ET
from datetime import datetime
from SPUB_functions import give_fake_id


#%% main

#plik od MG, który ma miejsca jako tematy – MG ma wygenerować lepszej jakości plik
with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-08\persons.json", encoding='utf-8') as f:
    data = json.load(f)

[e.update({'dateB': e.get('fromWiki', {}).get('dateB')}) for e in data]
[e.update({'dateD': e.get('fromWiki', {}).get('dateD')}) for e in data]
data = [{k:e.get('dateB') if k == 'yearBorn' and isinstance(e.get('dateB'), str) else v for k,v in e.items()} for e in data]
data = [{k:e.get('dateD') if k == 'yearDeath' and isinstance(e.get('dateD'), str) else v for k,v in e.items()} for e in data]

#przejmujemy daty z wiki i nadpisujemy yearBorn i yearDeath, jak wiki puste, to zostaje to, co było

#co jeśli jedna osoba (ten sam wiki id) ma kilka nazw? czy to w ogóle się zdarza?

# len([e.get('wiki') for e in data if e.get('wiki')])
# len(set([e.get('wiki') for e in data if e.get('wiki')]))
data = [{k:v for k,v in e.items() if k not in ['dateB', 'dateD', 'fromWiki', 'recCount']} for e in data]

class Person:
    
    def __init__(self, id_, viaf, name='', birth_date='', death_date=''):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"if id_ else None
        self.viaf = f"https://viaf.org/viaf/{viaf}" if viaf else None
        self.creator = 'cezary_rosinski'
        self.status = 'published'
        self.date = str(datetime.today().date())
        self.publishing_date = self.date
        self.sex = None
        self.headings = ['f56c40ddce1076f01ab157bed1da7c85']
        
        self.names = [self.PersonName(value=name)]
        self.links = []
        for el in [self.id, self.viaf]:
            self.add_person_link(el)
        self.birth_date = self.PersonDate(date_from=birth_date) if birth_date else None
        self.death_date = self.PersonDate(date_from=death_date) if death_date else None
    
    class XmlRepresentation:
        
        def to_xml(self):
            match self.__class__.__name__:
                case 'PersonName':
                    name_xml = ET.Element('name', {'transliteration': self.transliteration, 'code': self.code})
                    name_xml.text = self.value
                    return name_xml
                case 'PersonDate':
                    date_xml = ET.Element('date', {'from': self.date_from, 'from-bc': self.date_from_bc, 'date-uncertain': self.date_uncertain})
                    return date_xml
                case 'PersonLink':
                    link_xml = ET.Element('link', {'access-date': self.access_date, 'type': self.type})
                    link_xml.text = self.link
                    return link_xml
    
    class PersonName(XmlRepresentation):
        
        def __init__(self, value):
            self.value = value
            self.transliteration = 'no'
            self.code = 'main-name'
            
        def __repr__(self):
            return "PersonName('{}')".format(self.value)
        
    class PersonDate(XmlRepresentation):
        
        def __init__(self, date_from='', date_from_bc='', date_to='', date_to_bc='', date_uncertain='', date_in_words='', place_id='', place_period='', place_lang=''):
            self.date_from = date_from
            self.date_from_bc = 'false' if self.date_from else date_from_bc
            self.date_to = date_to
            self.date_to_bc = date_to_bc
            self.date_uncertain = 'false' if self.date_from else date_uncertain
            self.date_in_words = date_in_words
            #place zależy od rozwoju kartoteki miejsc – dodanie miejsc wiki
            
        def __repr__(self):
            return "PersonDate(date_from='{}', date_from_bc='{}', date_uncertain='{}')".format(self.date_from, self.date_from_bc, self.date_uncertain)
        
    class PersonLink(XmlRepresentation):
        
        def __init__(self, person_instance, link):
            self.access_date = person_instance.date
            self.type = 'external-identifier'
            self.link = link
            
        def __repr__(self):
            return "PersonLink('{}', '{}', '{}')".format(self.access_date, self.type, self.link)
        
    @classmethod
    def from_dict(cls, person_dict):
        id_ = person_dict.get('wiki')
        viaf = person_dict.get('viaf')
        name = person_dict.get('name')
        birth_date = person_dict.get('yearBorn')
        death_date = person_dict.get('yearDeath')
        return cls(id_, viaf, name, birth_date, death_date)
    
    def add_person_link(self, person_link):
        if person_link:
            self.links.append(self.PersonLink(person_instance=self, link=person_link))
            
    def to_xml(self):
        person_dict = {k:v for k,v in {'id': self.id, 'status': self.status, 'creator': self.creator, 'creation-date': self.date, 'publishing-date': self.publishing_date, 'viaf': self.viaf}.items() if v}
        person_xml = ET.Element('person', person_dict)
        for element, el_name in zip([self.names, self.links], ['names', 'links']):
            if element:
                higher_node = ET.Element(el_name)
                for node in element:
                    higher_node.append(node.to_xml())
                person_xml.append(higher_node)
        
        if self.sex:
            person_xml.append(ET.Element('sex', {'value': self.sex}))
        if self.birth_date:
            birth_xml = ET.Element('birth')
            birth_xml.append(self.birth_date.to_xml())
            person_xml.append(birth_xml)
        if self.death_date:
            death_xml = ET.Element('death')
            death_xml.append(self.death_date.to_xml())
            person_xml.append(death_xml)
        headings_xml = ET.Element('headings')
        for heading in self.headings:
            headings_xml.append(ET.Element('heading', {'id': heading}))
        person_xml.append(headings_xml)
        return person_xml

persons = [Person.from_dict(e) for e in data]
# [e.__dict__ for e in persons]
give_fake_id(persons)
# [e.__dict__ for e in persons]

persons_xml = ET.Element('pbl')
files_node = ET.SubElement(persons_xml, 'files')
people_node = ET.SubElement(files_node, 'people')
for person in persons:
    people_node.append(person.to_xml())

tree = ET.ElementTree(persons_xml)

ET.indent(tree, space="\t", level=0)
tree.write(f'import_people_{datetime.today().date()}.xml', encoding='UTF-8')

# #print tests
# test_xml = persons[0].names[0].to_xml()
# test_xml = persons[0].birth_date.to_xml()
# test_xml = persons[0].death_date.to_xml()
# test_xml = persons[0].links[0].to_xml()
# test_xml = persons[-1].to_xml()

# from xml.dom import minidom
# xmlstr = minidom.parseString(ET.tostring(test_xml)).toprettyxml(indent="   ")
# print(xmlstr)



















