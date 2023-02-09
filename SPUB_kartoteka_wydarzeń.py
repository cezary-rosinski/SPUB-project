import json
from SPUB_wikidata_connector import get_wikidata_label
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import xml.etree.cElementTree as ET
from datetime import datetime
from SPUB_functions import give_fake_id
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/Global-trajectories-of-Czech-Literature')
from marc21_parser import parse_marc21_field
from collections import ChainMap
import regex as re

#%% main

with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-09\biblio.json", encoding='utf-8') as f:
    biblio_data = json.load(f)

event_data = [e for e in biblio_data if any(el in e.get('fullrecord') for el in ['=111', '=611', '=711'])]

data = []
for el in event_data:
    ttt = el.get('fullrecord')
    record_dict = {}
    for line in ttt.split('\n'):
        if line.startswith('=LDR'):
            if record_dict:
                records.append(record_dict)
                record_dict = {}
            record_dict[line[1:4]] = [line[6:]]
        elif line.startswith('='):
            key = line[1:4]
            if key in record_dict:
                record_dict[key] += [line[6:]]
            else:
                record_dict[key] = [line[6:]]
    record_dict = [v[0] for k,v in record_dict.items() if k in ['111', '611', '711']][0]
    data.append(record_dict)

data = [[{'name' if k == '$a' else 'year' if k == '$d' else 'place' if k == '$c' else k:v for k,v in el.items()} for el in parse_marc21_field(e, '\\$')] for e in data]

data = [dict(ChainMap(*e)) for e in data]
data = [{k:re.findall('\d+', v)[0] if k=='year' else v.replace(')','') if k=='place' else v for k,v in e.items()} for e in data]

event_dict = {
    'Konkursy': 'competition',
    'Nagrody polskie': 'prize',
    'Nagrody zagraniczne': 'prize',
    'Odznaczenia': 'decoration',
    'Plebiscyty': 'plebiscite',
    'Wystawy': 'exhibition',
    'Zjazdy, festiwale, sesje w Polsce': 'festival',
    'Zjazdy, festiwale, sesje za granicą': 'festival'
}

[e.update({'type': [el for el in event_dict if el in e.get('name')][0] if [el for el in event_dict if el in e.get('name')] else None}) for e in data]
data = [{k:v.replace(e.get('type')+', ','') if k=='name' and e.get('type') else v for k,v in e.items()} for e in data]
data = [{k:event_dict.get(v) if k == 'type' and v else v for k,v in e.items()} for e in data]

event_dict2 = {
    'doktorat honoris causa': 'honorary-doctorate',
    'festiwal': 'festival',
    'konferencja': 'conference',
    'konkurs': 'competition',
    'nagroda': 'prize',
    'odznaczenie': 'decoration',
    'plebiscyt': 'plebiscite',
    'spotkanie autorskie': 'authors-meeting',
    'wystawa': 'exhibition'
}
data = [{k:event_dict2.get([el for el in event_dict2 if el in e.get('name').lower()][0]) if k=='type' and [el for el in event_dict2 if el in e.get('name').lower()] else v for k,v in e.items()} for e in data]

data = [{'type_' if k=='type' else k:v for k,v in e.items()} for e in data]

class Event:
    
    def __init__(self, id_='', viaf='', name='', year='', place='', type_=''):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"if id_ else None
        self.viaf = f"https://viaf.org/viaf/{viaf}" if viaf else None
        self.creator = 'cezary_rosinski'
        self.status = 'published'
        self.date = str(datetime.today().date())
        self.publishing_date = self.date
        self.headings = ['f56c40ddce1076f01ab157bed1da7c85']
        
        self.names = [self.EventName(value=name)]
        self.type = type_
        
        self.year = year
        self.place = place
        
    class XmlRepresentation:
        
        pass
    
    class EventName(XmlRepresentation):
        
        def __init__(self, value):
            self.value = value
            self.type = 'base'
            self.newest = 'true'
            self.transliteration = 'false'
            
        def __repr__(self):
            return "EventName('{}')".format(self.value)
        
    class EventYear(XmlRepresentation):
        
        def __init__(self, year):
            self.year = year
            
        def __repr__(self):
            return "EventYear('{}')".format(self.year)
        
    class EventLink(XmlRepresentation):
        
        def __init__(self, event_instance, link):
            self.access_date = event_instance.date
            self.type = 'external-identifier'
            self.link = link
            
        def __repr__(self):
            return "EventLink('{}', '{}', '{}')".format(self.access_date, self.type, self.link)
        
        #TUTAJ
        #date and place
        
    @classmethod
    def from_dict(cls, event_dict):
        # title = journal_dict.get('name')
        # issn = journal_dict.get('issn')
        # years = journal_dict.get('years')
        # return cls(title=title, issn=issn, years=years)
        return cls(**event_dict)

events = [Event.from_dict(e) for e in data]
give_fake_id(events)
[e.__dict__ for e in events]    

#%% schemat
<events>
            <event id="TuJestZewnetrznyId" status="published|draft|prepared" createor="c_rosinski" creation-date="2021-01-01" publishing-date="2021-01-01" origin="IdentyfikatorŹródła">

                <!-- honorary-doctorate | festival | conference | competition | prize | decoration | plebiscite | authors-meeting | exhibition-->
                <type value="authors-meeting"/>

                <names>
                    <name type="base|other" transliteration="true">Nike</name>
                    <name type="base|other" transliteration="true" newest="true">Literacka Nagroda Gazety Wyborczej</name>
                </names>

                <edition>XXI</edition>

                <date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words="" name="date-for-period"/>
                <date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words=""/>

                <places>
                    <place id="id_1" period="" lang=""/>
                    <place id="id_2"/>
                </places>

                <event-series-list>
                    <event-series id="id_serii_wydarzeń_1">
                    <event-series id="id_serii_wydarzeń_2">
                </event-series-list>

                <parent id="ZewnętzneId">
                    <date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words="" name="begin"/>
                    <date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words="" name="end"/>
                </parent>

                <subordinates>
                    <subordinate-event id="ZewnetrzneId_1"/>
                    <subordinate-event id="ZewnetrzneId_2"/>
                    <!-- ... -->
                </subordinates>

                <previous id="XXX"/>
                <next id="YYY">
                    <date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words="" name="begin"/>
                    <date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words="" name="end"/>
                </next>

                <organizers>
                    <person id="id_1"/>
                    <person id="id_2"/>
                    <!-- ... -->

                    <institution id="id_1"/>
                    <institution id="id_2"/>
                    <!-- ... -->

                    <journal id="id_1"/>
                    <journal id="id_2"/>
                    <!-- ... -->
                </organizers>

                <!-- list below can be empty - without heading-->
                <headings>
                    <heading id="lit-pol"/>
                    <heading id="teor-lit"/>
                    <!-- ... -->
                </headings>

                <annotation>To jest jakaś adnotacja</annotation>

                <remark>To jest jakiś komentarz</remark>


                <tags>
                    <tag>#WaznyInstytut</tag>
                    <tag>#nicMiNiePrzychodzi</tag>
                    <!-- ... -->
                </tags>

                <links>
                    <link access-date="12.05.2021" type="external-identifier|broader-description-access|online-access">http://pbl.poznan.pl/</link>
                    <link access-date="18.05.2021" type="...">http://ibl.pbl.waw.pl/</link>
                    <!-- ... -->
                </links>

                <linked-objects>
                    <record id="id"/>
                    <!-- ... -->
                    <creative-work id="id"/>
                    <!-- ... -->
                    <series id="id"/>
                    <!-- ... -->
                    <institution id="id"/>
                    <!-- ... -->
                    <event id="id"/>
                    <!-- ... -->
                    <place id="" period="" lang=""/>
                    <!-- ... -->
                    <journal-source id="id"/>
                    <!-- ... -->
                </linked-objects>

                <awards>
                    <award id="" description="">
                        <record id=""/>
                        <!-- ... -->
                        <creative-work id=""/>
                        <!-- ... -->
                    </award>

                    <!-- ... -->
                </awards>


            </event>
        </events>