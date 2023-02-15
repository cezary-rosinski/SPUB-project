import json
from SPUB_wikidata_connector import get_wikidata_label
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import xml.etree.cElementTree as ET
from datetime import datetime
from SPUB_functions import give_fake_id
import regex as re
from SPUB_kartoteka_numerów_czasopism import JournalNumber

#%% main

# with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-15\magazines.json", encoding='utf-8') as f:
#     data2 = json.load(f)
    
# data2 = [{k:v for k,v in e.items() if k != 'recCount'} for e in data2]
# mg_titles = [e.get('name') for e in data2]

with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-02-15\biblio.json", encoding='utf-8') as f:
    biblio_data = json.load(f)

biblio_data = [{k:v for k,v in e.items() if k in ['article_resource_str_mv', 'source_publication', 'article_issn_str', 'datesort_str_mv', 'article_resource_related_str_mv']} for e in biblio_data if e.get('format_major')[0] == 'Journal article']

def get_number(x: str):
    patterns = ('(?>nr )(\d+)', '(?>\d+, )(.+?)(?=,)', '(?>^)R\. \d+', '(?>\[Nr\] )(\d+)', '(?>Nr )(\d+)')
    for pattern in patterns:
        try:
            return re.findall(pattern, x)[0]
        except IndexError:
            continue
    else:
        return "no match"
            
data = {}
for el in biblio_data:
    name = el.get('article_resource_str_mv')[0] if 'article_resource_str_mv' in el else el.get('source_publication')
    if name not in data:
        test_dict = {}
        test_dict['name'] = name
        test_dict['issn'] = el.get('article_issn_str')
        year = el.get('datesort_str_mv')[0]
        number = get_number(el.get('article_resource_related_str_mv')[0])
        test_dict['years'] = {year: set([number])}
        data[name] = test_dict
    else:
        year = el.get('datesort_str_mv')[0]
        number = get_number(el.get('article_resource_related_str_mv')[0])
        if year in data[name]['years']:       
            data[name]['years'][year].add(number)
        else:
            data[name]['years'].update({year: set([number])})

data = list(data.values())
# titles = [e.get('name') for e in data] 

# journals_data = [{e.get('article_resource_str_mv')[0] if 'article_resource_str_mv' in e else e.get('source_publication'):e.get('datesort_str_mv')[0]} for e in biblio_data if e.get('format_major')[0] == 'Journal article']

# test = [e for e in biblio_data if e.get('article_resource_str_mv') and e.get('format_major')[0] == 'Journal article']

# biblio_journals = {}
# for e in journals_data:
#     k,v = tuple(e.items())[0]
#     if k not in biblio_journals:
#         biblio_journals[k] = set([v])
#     else:
#         biblio_journals[k].add(v)

# [e.update({'years': biblio_journals.get(e.get('name'))}) for e in data]
# data = [{'title' if k == 'name' else k:v for k,v in e.items()} for e in data]






#%%

class Journal:
    
    def __init__(self, id_='', viaf='', title='', issn='', years=None, character=''):
        self.id = f"http://www.wikidata.org/entity/Q{id_}"if id_ else None
        self.viaf = f"https://viaf.org/viaf/{viaf}" if viaf else None
        self.creator = 'cezary_rosinski'
        self.status = 'published'
        self.date = str(datetime.today().date())
        self.publishing_date = self.date
        self.headings = ['f56c40ddce1076f01ab157bed1da7c85']
        self.issn = issn
        self.status = 'source'
        
        self.titles = [self.JournalTitle(value=title)]
        if years:
            self.years = [self.JournalYear(year=year) for year in years]
        self.characters = [self.JournalCharacter(character=character)]
        self.links = []
        for el in [self.id, self.viaf]:
            self.add_journal_link(el)
    
    class XmlRepresentation:
        
        def to_xml(self):
            match self.__class__.__name__:
                case 'JournalTitle':
                    title_xml = ET.Element('title', {'type': self.type, 'lang': self.lang, 'principal': self.principal, 'transliteration': self.transliteration})
                    title_xml.text = self.value
                    return title_xml
                case 'JournalLink':
                    link_xml = ET.Element('link', {'access-date': self.access_date, 'type': self.type})
                    link_xml.text = self.link
                    return link_xml
                case 'JournalYear':
                    return ET.Element('year', {'year': self.year})
                case 'JournalCharacter':
                    return ET.Element('character', {'value': self.character})
    
    class JournalTitle(XmlRepresentation):
        
        def __init__(self, value):
            self.value = value
            self.type = 'base'
            self.lang = 'pl'
            self.principal = 'true'
            self.transliteration = 'false'
            
        def __repr__(self):
            return "JournalTitle('{}')".format(self.value)
        
    class JournalYear(XmlRepresentation):
        
        def __init__(self, year):
            self.year = year
            
        def __repr__(self):
            return "JournalYear('{}')".format(self.year)
    
    #newest-journal-number --> zostawiamy/co zrobić?
    
    #publishing-series --> najpierw kartoteka publishing-series
    
    #date --> później
    
    #places --> później
    
    class JournalCharacter(XmlRepresentation):
        
        def __init__(self, character):
            self.character = 'literary'
    
        def __repr__(self):
            return "JournalCharacter('{}')".format(self.character)
        
    class JournalLink(XmlRepresentation):
        
        def __init__(self, journal_instance, link):
            self.access_date = journal_instance.date
            self.type = 'external-identifier'
            self.link = link
            
        def __repr__(self):
            return "JournalLink('{}', '{}', '{}')".format(self.access_date, self.type, self.link)
    
    @classmethod
    def from_dict(cls, journal_dict):
        # title = journal_dict.get('name')
        # issn = journal_dict.get('issn')
        # years = journal_dict.get('years')
        # return cls(title=title, issn=issn, years=years)
        return cls(**journal_dict)
    
    def add_journal_link(self, journal_link):
        if journal_link:
            self.links.append(self.JournalLink(journal_instance=self, link=journal_link))

    def to_xml(self):
        journal_dict = {'id': self.id, 'status': self.status, 'creator': self.creator, 'creation-date': self.date, 'publishing-date': self.publishing_date}
        journal_xml = ET.Element('journal', journal_dict)
        
        if self.links:
            links_xml = ET.Element('links')
            for link in self.links:
                links_xml.append(link.to_xml())
            journal_xml.append(links_xml)
        
        if self.titles:
            titles_xml = ET.Element('titles', {'without-title': 'false'})
            for title in self.titles:
                titles_xml.append(title.to_xml())
        else: titles_xml = ET.Element('titles', {'without-title': 'true'})
        journal_xml.append(titles_xml)
        
        headings_xml = ET.Element('headings')
        for heading in self.headings:
            headings_xml.append(ET.Element('heading', {'id': heading}))
        journal_xml.append(headings_xml)
        
        issn_xml = ET.Element('issn')
        issn_xml.text = self.issn
        journal_xml.append(issn_xml)
        
        journal_xml.append(ET.Element('status', {'id': self.status}))
        
        return journal_xml
    

journals = [Journal.from_dict(e) for e in data]
give_fake_id(journals)
[e.__dict__ for e in journals]

journals_xml = ET.Element('pbl')
files_node = ET.SubElement(journals_xml, 'files')
journals_node = ET.SubElement(files_node, 'journals')
for journal in journals:
    journals_node.append(journal.to_xml())

tree = ET.ElementTree(journals_xml)

ET.indent(tree, space="\t", level=0)
tree.write(f'import_journals_{datetime.today().date()}.xml', encoding='UTF-8')

# #print tests
# test_xml = journals[0].to_xml()

# from xml.dom import minidom
# xmlstr = minidom.parseString(ET.tostring(test_xml)).toprettyxml(indent="   ")
# print(xmlstr)

#%% schemat
# <journal id="TuJestZewnetrznyId" status="published|draft|prepared" createor="c_rosinski" creation-date="2021-01-01" publishing-date="2021-01-01">

#                 <!-- type = {base,other} -->
#                 <titles without-title="true|false">
#                     <title transliteration="true|false" lang="pl" type="base" principal="true">Pan Tadeusz</title>
#                     <title transliteration="true|false" lang="pl" type="other">Pan Tadeusz</title>
#                     <title transliteration="true|false" lang="pl" type="">Pan Tadeusz</title>
#                     <!-- ... -->
#                 </titles>

#                 <years>
#                     <year id="IdRocznika01"/>
#                     <year id="IdRocznika02"/>
#                     <year id="IdRocznika03"/>
#                     <!-- ... -->
#                 </years>

#                 <newest-journal-number id="IdNajnowszegoNumeru"/>

#                 <publishing-series id="IdSeriiWydawniczej"/>

#                 <abbreviation>Skrót</abbreviation>


#                 <date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words="" name="published-from"/>
#                 <date from="2021-05-25" from-bc="True|False" to="" to-bc="True|False" uncertain="True|False" in-words="" name="published-to"/>

#                 <places>
#                     <place id="id_1" period="" lang=""/>
#                     <place id="id_2"/>
#                 </places>

#                 <issn>XXXX-XXXX</issn>


#                 <parent id="ZewnętzneId" />

#                 <subordinates>
#                     <subordinate-journal id="ZewnetrzneId_1"/>
#                     <subordinate-journal id="ZewnetrzneId_2"/>
#                     <!-- ... -->
#                 </subordinates>

#                 <previous id="XXX"/>
#                 <next id="YYY"/>



#                 <!-- list below can be empty - without heading-->
#                 <headings subject-department="t|f">
#                     <heading id="lit-pol"/>
#                     <heading id="teor-lit"/>
#                     <!-- ... -->
#                 </headings>


#                 <!-- literary,film,theater,radio,scientific-society,scientific,scientific-notes,city-chronics,pedagogical,political,
#                     information-bulletins,literary-studies,cultural,regional,.........,no-data
#                  -->
#                 <characters>
#                     <character value="" />
#                     <character value="" />
#                     <!-- ... -->
#                 </characters>

#                 <annotation>To jest jakaś adnotacja</annotation>

#                 <remark>To jest jakiś komentarz</remark>


#                 <tags>
#                     <tag>#WaznyInstytut</tag>
#                     <tag>#nicMiNiePrzychodzi</tag>
#                     <!-- ... -->
#                 </tags>

#                 <links>
#                     <link access-date="12.05.2021" type="external-identifier|broader-description-access|online-access">http://pbl.poznan.pl/</link>
#                     <link access-date="18.05.2021" type="...">http://ibl.pbl.waw.pl/</link>
#                     <!-- ... -->
#                 </links>


#                 <availabilities national-library="t|f">
#                     <library id="institutionId01" signature=""/>
#                     <library id="institutionId02" signature=""/>
#                     <library id="institutionId03" signature=""/>
#                     <!-- ... -->
#                 </availabilities>

#                 <status id="{source|potential_source|indirect_information|other}"/>

#                 <provenience z-autopsji="t|f">
#                     <journal-source journal="id-czasopisma-1" year="" number="" pages="zakres"/>
#                     <other-source id="id-zrodła" pages="lista-stron">nazwa źródła</other-source>
#                 </provenience>

#             </journal>





















