# https://allysonota.weebly.com/uploads/5/7/9/6/57968819/procedure_for_crossref_doi_project.pdf

# co z doi_batch i head?
# dodać usuwanie pustych tagów np. jak nie ma autora

#%% libraries

import gspread as gs
from gspread_dataframe import get_as_dataframe

from lxml.etree import ElementTree, tostring
from lxml.builder import E


#%% main

def create_person_metadata(row):
    if row['ORCID']: orcid = 'https://orcid.org/' + row['ORCID']
    else: orcid = ''
    return E.person_name(
                            E.given_name(row['given_name']),
                            E.surname(row['surname']),
                            E.ORCID(orcid),
                            sequence=row['sequence'],
                            contributor_role=row['contributor_role']
                        )

def create_journal_metadata(row):
    return E.journal_meatadata(
                                E.full_title(row['full_title']),
                                E.abbrev_title(row['abbrev_title']),
                                E.issn(row['issn'], media_type='print'),
                                E.doi_data(
                                    E.doi(row['doi']),
                                    E.resource(row['resource'])
                                    )
                                )

def create_issue_metadata(row):
    return E.journal_issue(
                            E.publication_date(
                                               E.year(row['year']),
                                               media_type='print'
                                               ),
                            E.publication_date(
                                                E.year(row['year']),
                                                media_type='online'
                                               ),
                            E.journal_volume(
                                E.volume(row['volume'])
                                ),
                            E.doi_data(
                                E.doi(row['doi.1']),
                                E.resource(row['resource.1'])
                                )
                            )

def create_article_metadata(row):
    return E.journal_article(
                            E.titles(
                                E.title(row['title'])
                                ),
                            E.contributors(
                                create_person_metadata(row)
                                ),
                            E.publication_date(
                                E.year(row['year']), 
                                media_type='print'),
                            E.publication_date(
                                E.year(row['year']), 
                                media_type='online'),
                            E.pages(
                                E.first_page(row['first_page']),
                                E.last_page(row['last_page'])
                                ),
                            E.doi_data(
                                E.doi(row['doi.2']),
                                E.resource(row['resource.2'])
                                ),
                            publication_type='full_text'
                            )


gc = gs.oauth()
sheet = gc.open_by_key('1_M95dOw-sXxNAtL9A9Z0WHYAiXcjr2sY6hzUTEr_28I')
worksheet = sheet.get_worksheet(0)
df = get_as_dataframe(worksheet, evaluate_formulas=True).fillna('').astype(str)

journal_xmls = {}

for index, row in df.iterrows():
    key = row['year'] + '-' + row['volume']
    if key in journal_xmls:
        for item in journal_xmls[key].findall('journal_article'):
           if row['doi.2'] == item.find('doi_data').find('doi').text:
               item.find('contributors').append(create_person_metadata(row))
               break
        else:
            journal_xmls[key].append(create_article_metadata(row))
    else:
        journal_xmls[key] = E.journal(
                                    create_journal_metadata(row),
                                    create_issue_metadata(row),
                                    create_article_metadata(row)
                                    )

output_xml = E.body()
for key, value in journal_xmls.items():
    output_xml.append(value)                                
                                        
        
        
#%% export to .xml
to_save = ElementTree(output_xml)
to_save.write(r"C:\Users\Nikodem\Desktop\test_xml.xml", xml_declaration=True, encoding='utf-8') 



#%% tests
root = E.root()       
to_add = E.dodany('ggg')
root.append(to_add)
print(root)
print(tostring(root))

tostring(E.dodany('ggg')) in tostring(root)








