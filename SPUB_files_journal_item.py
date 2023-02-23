import json
import regex as re
from collections import ChainMap
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/Global-trajectories-of-Czech-Literature')
from geotagging_for_MARC21 import marc_parser_for_field

#%% def

def parse_mrk(mrk):
    records = []
    record_dict = {}
    for line in mrk.split('\n'):
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
    records.append(record_dict)
    return records

#%%


def parse_java(path):
    with open(path, encoding='utf-8') as f:
        test = f.read().split('\n\n')
    test = [e for e in test if 'language:   pl' in e]
    return {[re.findall('(?<=value\:\s{6})(.+$)',el)[0] for el in e.split('\n') if 'value' in el][0]: 
             [re.findall('(?<=code\:\s{7})(.+$)',el)[0] for el in e.split('\n') if 'code' in el][0] for e in test}
    

java_record_types = parse_java(r"C:\Users\Cezary\Downloads\pbl_record_types.txt")
java_cocreators = parse_java(r"C:\Users\Cezary\Downloads\pbl_co-creator_types.txt")

with open(r"C:\Users\Cezary\Downloads\language_map_iso639-1.ini", encoding='utf-8') as f:
    language_codes = {e.split(' = ')[-1].strip(): e.split(' = ')[0].strip() for e in f.readlines() if e}







path = r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-16\biblio.json"

with open(path, encoding='utf-8') as f:
    origin_data = json.load(f)

origin_data = [e for e in origin_data if 'Journal article' in e.get('format_major')]


records_types = [{e.get('id'): [ele for sub in [el.get('655') for el in parse_mrk(e.get('fullrecord'))] for ele in sub] if [el.get('655') for el in parse_mrk(e.get('fullrecord'))][0] else [el.get('655') for el in parse_mrk(e.get('fullrecord'))]} for e in origin_data]
records_types = dict(ChainMap(*records_types))

records_types = {k:[[el.get('$a') for el in marc_parser_for_field(e, '\\$') if '$a' in el][0] if not isinstance(e, type(None)) else e for e in v] for k,v in records_types.items()}

records_types = {k:[test.get(e) for e in test if any(e in el.lower() for el in v)] if not isinstance(v[0], type(None)) else [test.get('inne')] for k,v in records_types.items()}
records_types = {k:v if v else ['other'] for k,v in records_types.items()}

languages = [{e.get('id'): [language_codes.get(el) for el in e.get('language')]} for e in data]

linked_objects = {e.get('id'): [ele for sub in [el.get('856') for el in parse_mrk(e.get('fullrecord'))] for ele in sub] if [el.get('856') for el in parse_mrk(e.get('fullrecord'))][0] else [el.get('856') for el in parse_mrk(e.get('fullrecord'))] for e in origin_data}
#tutaj wydobyć linki do libri
linked_objects = {k:[[el.get('$u') for el in marc_parser_for_field(e, '\\$') if '$u' in el][0] if not isinstance(e, type(None)) else e for e in v] for k,v in linked_objects.items()}

#%%
class JournalItem:
    
    def __init__(self, id_, types=None, author_id='', author_name='', languages=None, linked_ids=None):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"if id_ else None
        self.creator = 'cezary_rosinski'
        self.status = 'published'
        self.date = str(datetime.today().date())
        self.publishing_date = self.date
        self.origin = ''
        self.flags = ''
        
        if types:
            self.record_types = types
            
        self.author_id = author_id
        self.authors = [self.CreativeWorkAuthor(author_id=author_id, author_name=author_name)]
        
        self.general_materials = 'false'
        
        if languages:
            self.languages = languages
            
        self.headings = ['f56c40ddce1076f01ab157bed1da7c85']
        
        if linked_ids:
            self.linked_objects = linked_ids
        
        class JournalItemAuthor(XmlRepresentation):
            
            def __init__(self, author_id, author_name):
                self.author_id = f"http://www.wikidata.org/entity/Q{author_id}"if author_id else None
                self.juvenile = 'false'
                self.co_creator = 'false'
                self.principal = 'true'
                self.author_name = author_name

            def __repr__(self):
                return "JournalItemAuthor('{}', '{}')".format(self.author_id, self.author_name)
        
        class JournalItemTitle(XmlRepresentation):
            
            def __init__(self, value):
                self.value = value
                self.code = 'base'
                self.newest = 'true'
                self.transliteration = 'false'
                
            def __repr__(self):
                return "JournalItemTitle('{}')".format(self.value) 
        #dodać później współautorów
        
        def connect_with_persons(self, list_of_persons_class):
            for author in self.authors:
                if not author.author_id:
                    match_person = [e for e in list_of_persons_class if author.author_name in [el.value for el in e.names]]
                    if match_person:
                        author.author_id = match_person[0].id




#%% schemat XML

<journal-item id="journal-item-id-01" status="published" creator="a_margraf" creation-date="2022-12-01" publishing-date="2022-12-03" origin="CW-src-id-01" flags="123">

                <record-types>
                    <record-type code="poem"/>
                    <record-type code="novel"/>
                </record-types>

                <general-materials value="true" />

                <authors anonymous="false" author-company="false">
                    <author id="a0000001758844" juvenile="false" co-creator="true" principal="true"/>
                    <author id="person-id-02" juvenile="false" co-creator="true" />
                    <author id="person-id-03" juvenile="true" co-creator="false" />
                </authors>

                <titles>
                    <title code="base" transliteration="false" newest="true">Pozycja w czasopiśmie @{"Kazimierzu-Przerwa Tetmajerze"|person_id="przerwa_id"}</title>
                    <title code="incipit" transliteration="true">PC</title>
                </titles>

                <languages>
                    <language code="pl"/>
                </languages>

                <anthology-description>Opis antologii</anthology-description>

                <!--
                <anthology-descriptions>

                </anthology-descriptions>
                -->



                <mailings>
                    <mailing>
                        <sender>
                            <author id="person-id-11" juvenile="false" co-creator="true" />
                            <author id="person-id-12" juvenile="false" co-creator="true" />
                            <author id="person-id-13" juvenile="true" co-creator="false" />

                            <institution id="institution-id-11" />
                            <institution id="institution-id-12" />
                        </sender>

                        <addressee>
                            <author id="person-id-14" juvenile="false" co-creator="true" />

                            <institution id="institution-id-13" />
                            <institution id="institution-id-14" />
                            <institution id="institution-id-15" />
                        </addressee>
                        <description>Opis pierwszego mailingu</description>
                    </mailing>

                    <mailing>
                        <sender>
                            <author id="person-id-21" juvenile="false" co-creator="true" />
                        </sender>
                        <addressee>
                            <institution id="institution-id-22" />
                        </addressee>
                        <description>Opis drugiego mailingu</description>
                    </mailing>
                </mailings>

                <interviews>
                    <interview>
                        <interviewer>
                            <author id="person-id-11" juvenile="false" co-creator="true" />
                        </interviewer>
                        <interlocutor>
                            <institution id="institution-id-15" />
                        </interlocutor>
                        <description>Opis pierwszego mailingu</description>
                    </interview>
                </interviews>

                <co-creators>
                    <co-creator>
                        <type code="translation" />
                        <type code="adaptation" />
                        <person id="person-id-01" />
                    </co-creator>

                    <co-creator>
                        <type code="adaptation" />
                        <person id="person-id-02" />
                    </co-creator>
                </co-creators>




                <headings>
                    <heading id="7320f7ca8352d748b386ab4e4913e3ee"/>
                    <heading id="a31385e80345e59e06b208e998bcaeab"/>
                    <heading id="e99e5257f8af377ba21568d1fb73e368"/>
                </headings>

                <annotation>To jest tekst o @{"Kazimierzu-Przerwa Tetmajerze"|person_id="przerwa_id"} oraz o jego przyjacielu @{"Franku Kowalskim"|person_id="franciszek_kowalski_id"}. Pewnie możnaby tu jeszcze coś uzupełnić o @{"Oli Sroce"|person_id="ola_sroka_id"}. Ich firmat to @{"Firemka sp. z o.o."|institution_id="firemka_id"} właśnie.</annotation>

                <remark>Tutaj można wpisać tekst komentarza dla Creative works. Komentarze są niewidoczne na stronie</remark>


                <tags>
                    <tag>#ji-nike</tag>
                    <tag>#ji-nicMiNiePrzychodzi</tag>
                    <!-- ... -->
                </tags>

                <links>
                    <link access-date="12.05.2021" type="external-identifier">http://pbl.poznan.pl/</link>
                    <link access-date="18.05.2021" type="broader-description-access">http://ibl.pbl.waw.pl/</link>
                    <!-- ... -->
                </links>


                <linked-objects>
                    <record-ref id="r-id-01"/>
                    <!-- ... -->
                    <creative-work-ref id="cw-id-01"/>
                    <!-- ... -->
                    <series-ref id="s-id-01"/>
                    <!-- ... -->
                    <institution-ref id="https://viaf.org/viaf/000000002/"/>
                    <institution-ref id="https://viaf.org/viaf/000000003/"/>
                    <!-- ... -->
                    <event-ref id="e-id-01"/>
                    <!-- ... -->
                    <place-ref id="p-id-01" period="" lang=""/>
                    <!-- ... -->
                    <journal-source-ref id="js-id"/>
                    <!-- ... -->
                </linked-objects>