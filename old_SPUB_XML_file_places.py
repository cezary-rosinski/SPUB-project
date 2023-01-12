import xml.etree.cElementTree as ET
import lxml.etree
from tqdm import tqdm
import regex as re
from datetime import datetime
from collections import Counter
from my_functions import gsheet_to_df
import ast
import pandas as pd
import numpy as np
from geojson import Point


# for k, v in ttt.items():
#     for e in v['place_names']:
#         try:
#             if e['placeLabel.value'] == 'Gdańsk':
#                 print(k)
#                 break
#         except KeyError:
#             pass

# test_place = ttt['http://www.wikidata.org/entity/Q1792']

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


# place_dict = {'coordinates.value':'lat_lon',
#               # 'countryLabel.value':'country',
#               'endtime.value':'date-to',
#               'starttime.value':'date-from',
#               'place.value':'id',
#               # 'placeLabel.value':'name',
#               # 'officialName.value':'name',
#               'geonamesID.value':'geonames',
#               'officialName.xml:lang':'lang',
#               'countryLabel.xml:lang':'lang',
#               'placeLabel.xml:lang':'lang'}

def create_place(parent, dict_data):
    # parent = xml_nodes['places']
    # dict_data = ttt['http://www.wikidata.org/entity/Q1001225']
    # dict_data = ttt['http://www.wikidata.org/entity/Q1156']
    # dict_data = test_place.copy()
    geo_dict = {}
    for key, value in dict_data.items():
        if key in ['geonamesID.value', 'place.value']:
            geo_dict[place_dict[key]] = value
        elif key == 'coordinates.value':
            geo_dict['lat'] = str(value['coordinates'][0])
            geo_dict['lon'] = str(value['coordinates'][1])
    place = ET.SubElement(parent, 'place', geo_dict)
    for ke, va in dict_data['place_dates'].items():
        empty_dict2 = {}
        empty_dict2['date-from'] = ke.split('|')[0]
        empty_dict2['date-to'] = ke.split('|')[-1]
        period = ET.SubElement(place, 'period', empty_dict2)
        list_of_place_names, list_of_countries_names = [], []
        for i, el in enumerate(va):
            for k,v in el.items():
                if k == 'placeLabel.xml:lang':
                    list_of_place_names.append([place_dict[k], v, el['placeLabel.value']])
                elif k == 'officialName.xml:lang':
                    list_of_place_names.append([place_dict[k], v, el['officialName.value']])
                elif k == 'countryLabel.xml:lang':
                    list_of_countries_names.append([place_dict[k], v, el['countryLabel.value']])
            list_of_place_names = [list(x) for x in set(tuple(x) for x in list_of_place_names)]
            list_of_countries_names = [list(x) for x in set(tuple(x) for x in list_of_countries_names)]
        for element in list_of_place_names:
            pl_name = ET.SubElement(period, 'name', {element[0]:element[1]})
            pl_name.text = element[-1]
        for element in list_of_countries_names:
            pl_country = ET.SubElement(period, 'country', {element[0]:element[1]})
            pl_country.text = element[-1]
                    


place_dict = {'place name': 'name',
              'place name lang': 'lang',
              'country name': 'country',
              'country name lang': 'lang'}

    
def create_xml_places_from_gsheet(gsheetID, worksheet, parent):   
    gsheetID = '109-0UT-wJzZ8HVP9OwEy953T2tD51DCecfNryRQ3wKA'
    worksheet = 'out'
    df = gsheet_to_df(gsheetID, worksheet)
    try:
        df['koordynaty'] = df['koordynaty'].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else np.nan)
    except SyntaxError:
        df['koordynaty'] = df['koordynaty'].str.replace(' ', ',')
        df['koordynaty'] = df['koordynaty'].apply(lambda x: Point(ast.literal_eval(x.replace('Point',''))) if pd.notnull(x) else np.nan)
    df['lat'], df['lon'] = zip(*df['koordynaty'].apply(lambda x: x['coordinates'] if pd.notnull(x) else (np.nan, np.nan)))
    # df = df.replace(np.nan, '', regex=True)
    df['date-from'] = df['date-from'].apply(lambda x: re.sub('(.+)(T.*)', r'\1', x) if pd.notnull(x) else '')
    df['date-to'] = df['date-to'].apply(lambda x: re.sub('(.+)(T.*)', r'\1', x) if pd.notnull(x) else '')
    df['period'] = df[['date-from', 'date-to']].apply(lambda x: '❦'.join(x.astype(str)), axis=1)
    df.drop(columns=['koordynaty', 'date-from', 'date-to'], inplace=True)
    col_order = ['id', 'geonames', 'lat', 'lon', 'period', 'place name', 'place name lang', 'country name', 'country name lang']
    df = df.reindex(columns=col_order)
    df_grouped = df.groupby('id')
    places_list = []
    for name, group in df_grouped:
        places_dict = {}
        periods = []
        for i, row in group.iterrows():
            for index, value in row.items():
                if pd.notnull(value):
                    if index in ['id', 'geonames', 'lat', 'lon']:
                        if index not in places_dict: 
                            places_dict[index] = value
                    elif index == 'period':
                        periods.append(value)
                        if index not in places_dict:
                            places_dict[index] = {value:{}}
                        else:
                            places_dict[index].update({value:{}})
                    else:
                        places_dict['period'][periods[-1]][index] = value  
        places_list.append(places_dict)
        
    for element in places_list:
        geo_dict = {}
        for key, value in element.items():
            if key != 'period':
                geo_dict[key] = str(value)
        place = ET.SubElement(parent, 'place', geo_dict)
        for ke, va in element['period'].items():
            empty_dict2 = {}
            empty_dict2['date-from'] = ke.split('❦')[0]
            empty_dict2['date-to'] = ke.split('❦')[-1]
            period = ET.SubElement(place, 'period', empty_dict2)
            empty_dict3 = {}
            empty_dict4 = {}
            for k, v in va.items():
                if k in ['place name', 'place name lang']:
                    empty_dict3[place_dict[k]] = v
                else:
                    empty_dict4[place_dict[k]] = v
            pl_name = ET.SubElement(period, 'name', {k:v for k,v in empty_dict3.items() if k == 'lang'})
            pl_name.text = empty_dict3['name']
            try:
                pl_country = ET.SubElement(period, 'country', {k:v for k,v in empty_dict4.items() if k == 'lang'})
                pl_country.text = empty_dict4['country']
            except KeyError:
                pass
    return places_list
                

    
# xml_nodes = create_node_structure(['pbl', 'files', 'places'])
# create_place(xml_nodes['places'], test_place)   
# tree = ET.ElementTree(xml_nodes['pbl'])

# [elem.tag for elem in xml_nodes['pbl'].iter()]

# tree.write('test.xml', encoding='UTF-8')


# xml_nodes = create_node_structure(['pbl', 'files', 'places'])  
# for k, v in ttt.items():
#     create_place(xml_nodes['places'], v)   
# tree = ET.ElementTree(xml_nodes['pbl'])
# tree.write('test.xml', encoding='UTF-8')
    
    


# translator = google_translator()
# translator.translate('İstanbul', lang_src='tr', lang_tgt='pl')





































