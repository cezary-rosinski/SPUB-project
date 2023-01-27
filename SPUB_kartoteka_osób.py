import json
from SPUB_wikidata_connector import get_wikidata_label
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import xml.etree.cElementTree as ET
from datetime import datetime
from SPUB_functions import give_fake_id


#%% main

#plik od MG, który ma miejsca jako tematy – MG ma wygenerować lepszej jakości plik
with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-01-17\persons.json", encoding='utf-8') as f:
    data = json.load(f)

#co jeśli jedna osoba (ten sam wiki id) ma kilka nazw? czy to w ogóle się zdarza?

# len([e.get('wiki') for e in data if e.get('wiki')])
# len(set([e.get('wiki') for e in data if e.get('wiki')]))
data = [{k:v for k,v in e.items() if k != 'recCount'} for e in data]

class Person:
    
    def __init__(self, id_, viaf, name=''):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"if id_ else None
        self.viaf = f"https://viaf.org/viaf/{viaf}" if viaf else None
        self.creator = 'cezary_rosinski'
        self.status = 'published'
        self.date = datetime.today().date()
        self.publishing_date = self.date
        self.sex = None
        self.headings = ['f56c40ddce1076f01ab157bed1da7c85']
        
        self.names = [self.PersonName(value=name)]
        self.links = []
        for el in [self.id, self.viaf]:
            self.add_person_link(el)
    
    class PersonName:
        
        def __init__(self, value):
            self.value = value
            self.transliteration = 'no'
            self.code = 'main-name'
            
        def __repr__(self):
            return "PersonName('{}')".format(self.value)
    #tutaj    
    class PersonBirthOrDeath:
        
        def __init__(self, date_from, date_from_bc, date_to, date_to_bc, date_uncertain, date_in_words, place_id, place_period, place_lang):
            self.date_from = date_from
            self.date_from_bc = date_from_bc
            self.date_to = date_to
            self.date_to_bc = date_to_bc
            self.date_uncertain = date_uncertain
            self.date_in_words = date_in_words
        
    class PersonLink:
        
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
        return cls(id_, viaf, name)
    
    def add_person_link(self, person_link):
        if person_link:
            self.links.append(self.PersonLink(person_instance=self, link=person_link))

persons = [Person.from_dict(e) for e in data]
[e.__dict__ for e in persons]
give_fake_id(persons)
[e.__dict__ for e in persons]

<date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words=""/>
					<place id="" period="1111" lang=""/>







#%% schemat
# <person id="TuJestZewnetrznyId" viaf="13373997" status="published|draft|prepared" createor="c_rosinski" creation-date="2021-05-25" publishing-date="2021-05-25" origin="IdentyfikatorŹródła">
# 				<names>
# 					<!-- codes: main-name, family-name, other-last-name-or-first-name, monastic-name, codename, alias, group-alias, -->
# 					<name code="main-name" transliteration="no" presentation-name="yes">Jan Kowalski</name>
# 					<name code="codename">J.K.</name>
# 					<name code="alias" main-name="yes">Jasiu</name>
# 					<name code="alias">Janek</name>
# 					<!-- ... -->
# 				</names>
# 				<!-- male, female, unknown, null-->
# 				<sex value="male"/>
# 				<!-- list below can be empty - without heading-->
# 				<headings>
# 					<heading id="lit-pol"/>
# 					<heading id="teor-lit"/>
# 					<!-- ... -->
# 				</headings>
# 				<birth>
# 					<date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words=""/>
# 					<place id="" period="1111" lang=""/>

# 				</birth>
# 				<death>
# 					<date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words=""/>
# 					<place id="" period="1111" lang=""/>
# 				</death>
# 				<annotation>To jest jakaś adnotacja</annotation>
# 				<remark>To jest jakiś komentarz</remark>
# 				<tags>
# 					<tag>#gwiadkowicz</tag>
# 					<tag>#nicMiNiePrzychodzi</tag>
# 					<!-- ... -->
# 				</tags>
# 				<links>
# 					<link access-date="2021-05-12" type="external-identifier|broader-description-access|online-access">http://pl.wikipedia.org/jan_kowalski</link>
# 					<link access-date="2021-05-18" type="...">http://viaf.org/viaf/13373997/#Kowalski,_Jan_(1930-2018)</link>
# 					<!-- ... -->
# 				</links>