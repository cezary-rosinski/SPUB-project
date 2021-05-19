from SPUB_importer_read_data import read_MARC21, get_list_of_people, get_list_of_records
from SPUB_importer_query_national_library import query_national_library
from SPUB_query_wikidata import query_wikidata





#read data

marc21_records = read_MARC21('bn_harvested_2021_05_12.mrk')

people_list = get_list_of_people(marc21_records, ('=100'), '(\$e|\$t).*', 200)

bibliographical_records = get_list_of_records(marc21_records, people_list, ('=100'), '(\$e|\$t).*')

people_list = get_list_of_people(bibliographical_records, ('=100', '=600', '=700'), '(\$x|\$4|\$e|\.\$t).*')

#query national library

people_list_of_dicts = query_national_library(people_list)

#query wikidata

people_list_of_dicts = query_wikidata(people_list_of_dicts)

#create XML

xml_nodes_names = ['pbl', 'files', 'people']
































# przygotować testową listę rekordów bibliograficznych
# 300 ksiażek z bn (jakie?)
# wydobyć z nich osoby i związać z kartoteką wzorcową BN
# połączyć dane z viaf i wikidatą oraz wydobyć dodatkowe informacje do schematu PBL
import pandas as pd
import io
from google_drive_research_folders import PBL_folder
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from my_functions import cSplit, df_to_mrc, mrk_to_df, mrc_to_mrk
import datetime
import regex as re
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from tqdm import tqdm
import glob
import time
import json
from collections import Counter
import requests
from urllib.error import HTTPError
from http.client import RemoteDisconnected






import xml.etree.cElementTree as ET

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
    
    
#18.05.2021 - zbudować mechanizm dla osób xml - zapisać pliki 
# przygotować instytucje do kartoteki instytucji    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
                    
# final_list = []
# for lista in rekordy:
#     slownik = {}
#     for el in lista:
#         if el[1:4] in slownik:
#             slownik[el[1:4]] += f"❦{el[6:]}"
#         else:
#             slownik[el[1:4]] = el[6:]
#     final_list.append(slownik)

# df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
# fields = df.columns.tolist()
# fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
# df = df.loc[:, df.columns.isin(fields)]
# fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
# df = df.reindex(columns=fields)   




























