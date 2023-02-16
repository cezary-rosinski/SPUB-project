#%% note
# 1. uruchamiamy klasy w odpowiedniej kolejności
# 2. łączymy elementy tak, by skorzystać z wcześniej powołanych klas, np. miejsca wydarzeń początkowo są str, ale później stają się obiektem Place
# 3. generujemy XML z wzbogaconych klas

#%% import

from SPUB_preprocessing import preprocess_places, preprocess_events
from SPUB_kartoteki_klasy import Place, Event
from SPUB_functions import give_fake_id
import xml.etree.cElementTree as ET
from datetime import datetime



#%% import data
places_data = preprocess_places(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-16\pub_places.json")

events_data = preprocess_events(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-15\events.json")


#%% create class

places = [Place.from_dict(e) for e in places_data]
give_fake_id(places)

events = [Event.from_dict(e) for e in events_data]
give_fake_id(events)
[e.connect_with_places(places) for e in events]
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


events_xml = ET.Element('pbl')
files_node = ET.SubElement(events_xml, 'files')
events_node = ET.SubElement(files_node, 'events')
for event in events:
    events_node.append(event.to_xml())

tree = ET.ElementTree(events_xml)

ET.indent(tree, space="\t", level=0)
tree.write(f'import_events_{datetime.today().date()}.xml', encoding='UTF-8')