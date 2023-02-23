#%% note
# 1. uruchamiamy klasy w odpowiedniej kolejności
# 2. łączymy elementy tak, by skorzystać z wcześniej powołanych klas, np. miejsca wydarzeń początkowo są str, ale później stają się obiektem Place
# 3. generujemy XML z wzbogaconych klas

#%% import

from SPUB_preprocessing import preprocess_places, preprocess_people, preprocess_events, preprocess_publishing_series, preprocess_creative_works
# from SPUB_kartoteki_klasy import Place, Person, Event, PublishingSeries
from SPUB_kartoteka_miejsc import Place
from SPUB_kartoteka_osob import Person
from SPUB_kartoteka_wydarzen import Event
from SPUB_kartoteka_serii_wydawniczych import PublishingSeries
from SPUB_kartoteka_utworow import CreativeWork
from SPUB_functions import give_fake_id
import xml.etree.cElementTree as ET
from datetime import datetime



#%% import data
places_data = preprocess_places(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-16\pub_places.json", r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-16\event_places.json")

person_data = preprocess_people(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-16\persons.json")

events_data = preprocess_events(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-16\events.json")

series_data = preprocess_publishing_series(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-16\biblio.json")

creative_works_data = preprocess_creative_works(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-16\biblio.json")

#%% create class

places = [Place.from_dict(e) for e in places_data]
give_fake_id(places)

persons = [Person.from_dict(e) for e in person_data]
give_fake_id(persons)
for person in persons:
    person.connect_with_places(places)

events = [Event.from_dict(e) for e in events_data]
give_fake_id(events)
for event in events:
    event.connect_with_places(places) 

publishing_series_list = [PublishingSeries.from_dict(e) for e in series_data]
give_fake_id(publishing_series_list)

creative_works = [CreativeWork.from_dict(e) for e in creative_works_data]
give_fake_id(creative_works)
for creative_work in creative_works:
    creative_work.connect_with_persons(persons)
#%% enrich classes


#%% export xml



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

persons_xml = ET.Element('pbl')
files_node = ET.SubElement(persons_xml, 'files')
people_node = ET.SubElement(files_node, 'people')
for person in persons:
    people_node.append(person.to_xml())

tree = ET.ElementTree(persons_xml)

ET.indent(tree, space="\t", level=0)
tree.write(f'import_people_{datetime.today().date()}.xml', encoding='UTF-8')

events_xml = ET.Element('pbl')
files_node = ET.SubElement(events_xml, 'files')
events_node = ET.SubElement(files_node, 'events')
for event in events:
    events_node.append(event.to_xml())

tree = ET.ElementTree(events_xml)

ET.indent(tree, space="\t", level=0)
tree.write(f'import_events_{datetime.today().date()}.xml', encoding='UTF-8')

publishing_series_list_xml = ET.Element('pbl')
files_node = ET.SubElement(publishing_series_list_xml, 'files')
publishing_series_list_node = ET.SubElement(files_node, 'publishing-series-list')
for publishing_series in publishing_series_list:
    publishing_series_list_node.append(publishing_series.to_xml())
    
tree = ET.ElementTree(publishing_series_list_xml)

ET.indent(tree, space="\t", level=0)
tree.write(f'import_publishing_series_list_{datetime.today().date()}.xml', encoding='UTF-8')

creative_works_xml = ET.Element('pbl')
files_node = ET.SubElement(creative_works_xml, 'files')
creative_works_node = ET.SubElement(files_node, 'creative_works')
for creative_work in creative_works:
    creative_works_node.append(creative_work.to_xml())

tree = ET.ElementTree(creative_works_xml)

ET.indent(tree, space="\t", level=0)
tree.write(f'import_creative_works_{datetime.today().date()}.xml', encoding='UTF-8')

















