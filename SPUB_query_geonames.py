from tqdm import tqdm
import requests
import pandas as pd
import time
from urllib.error import HTTPError
from http.client import RemoteDisconnected
import regex as re
from difflib import SequenceMatcher
from my_functions import simplify_string
import numpy as np
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from google_drive_research_folders import PBL_folder
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import datetime
from geonames_accounts import geoname_users
import cx_Oracle
import pickle

#%% date
now = datetime.datetime.now()
year = now.year
month = '{:02}'.format(now.month)
day = '{:02}'.format(now.day)

#%% google authentication & google drive
#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()


#%% geonames harvesting
miasta = gc.open_by_key('1fgJV-05IaH6z4JJZB61T08xW2zZp8IM4KvHcCcpMs-Y')
miasta = get_as_dataframe(miasta.worksheet('miasta_clean'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)['miejscowość'].to_list()

df = pd.DataFrame()
users_index = 0

for miasto in tqdm(miasta):
    while True:
        url = 'http://api.geonames.org/searchJSON?'
        params = {'username': geoname_users[users_index], 'q': miasto, 'country': '', 'featureClass': '', 'continentCode': '', 'fuzzy': '0.6'}
        result = requests.get(url, params=params).json()
        if 'status' in result:
            users_index += 1
            continue
        test = pd.DataFrame.from_dict(result['geonames'])
        if test.shape[0]:
            test['similarity'] = test['toponymName'].apply(lambda x: SequenceMatcher(0,simplify_string(miasto),simplify_string(x)).ratio())
            test['query name'] = miasto
            test = test[test['similarity'] == test['similarity'].max()][['query name', 'geonameId', 'name', 'countryName', 'similarity']]
            df = df.append(test)
        else:
            test = pd.DataFrame({'query name':[miasto], 'geonameId':'', 'name':'', 'countryName':'', 'similarity':''})
            df = df.append(test)
        break

df.to_excel(f'kartoteka_miejsc_PBL_{year}-{month}-{day}.xlsx', index=False)


# miasto = 'Żytomierz'
# miasto = "'t Goy-Houten"
# url = 'http://api.geonames.org/searchJSON?'
# params = {'username': 'patthub', 'q': miasto, 'country': '', 'featureClass': 'P', 'continentCode': '', 'fuzzy': '0.6'}
# result = requests.get(url, params=params).json()

# test = pd.DataFrame.from_dict(result['geonames'])
# test['similarity'] = test['toponymName'].apply(lambda x: SequenceMatcher(0,simplify_string(miasto),simplify_string(x)).ratio())
# test['query name'] = miasto
# test = test[test['similarity'] == test['similarity'].max()][['query name', 'geonameId', 'name', 'countryName', 'fclName', 'similarity']]



# a może olać API i szukać w pliku allCities.txt? jak robić fuzzy search? - difflib.get_close_matches?



#%% SQL

# SQL connection

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

miasta_statusy = []
for miasto in tqdm(miasta):
    miasto = miasto.replace("'", '_')
    query = f"""select z.za_zapis_id, z.za_status_imp
    from pbl_zapisy z
    full outer join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id = z.za_zr_zrodlo_id
    full outer join IBL_OWNER.pbl_teatry te on te.te_teatr_id = z.za_te_teatr_id
    full outer join IBL_OWNER.pbl_zapisy_wytwornie zw on zw.zawf_za_zapis_id=z.za_zapis_id
    full outer join IBL_OWNER.pbl_wytwornie wf on wf.wf_wytwornia_id=zw.zawf_wf_wytwornia_id
    full outer join IBL_OWNER.pbl_zapisy_wydawnictwa zwyd on zwyd.zawy_za_zapis_id=z.za_zapis_id
    full outer join IBL_OWNER.pbl_wydawnictwa wy on wy.wy_wydawnictwo_id=zwyd.zawy_wy_wydawnictwo_id
    where zr.zr_miejsce_wyd like '%{miasto}%' 
    OR te.te_miasto like '%{miasto}%' 
    OR wf.wf_miasto like '%{miasto}%'
    OR wy.wy_miasto like '%{miasto}%'"""
    
    sql_answer = '|'.join(pd.read_sql(query, con=connection).fillna(value = 'IOK')['ZA_STATUS_IMP'].unique())
    
    miasta_statusy.append((miasto, sql_answer))
    
with open('miasta_statusy.pkl', 'wb') as f:
    pickle.dump(miasta_statusy, f)
    
with open('miasta_statusy.pkl', 'rb') as f:
    miasta_statusy = pickle.load(f)    
    
miasta_ino = [e for e in miasta_statusy if e[-1] == 'INO']    

df = pd.DataFrame(miasta_ino, columns=['miasta', 'statusy'])
df.to_excel('miasta_ino.xlsx', index=False)
    

# dla pustych wyników zastosować tę kwerendę:
# query = f"""select z.za_zapis_id, z.za_status_imp, zr.zr_miejsce_wyd, te.te_miasto, wf.wf_miasto, wy.wy_miasto, zrr.zrr_miejsce_wydania, z.za_miejsce_wydania
# from pbl_zapisy z
# full outer join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id = z.za_zr_zrodlo_id
# full outer join IBL_OWNER.pbl_zrodla_roczniki zrr on zrr.zrr_zr_zrodlo_id=zr.zr_zrodlo_id
# full outer join IBL_OWNER.pbl_teatry te on te.te_teatr_id = z.za_te_teatr_id
# full outer join IBL_OWNER.pbl_zapisy_wytwornie zw on zw.zawf_za_zapis_id=z.za_zapis_id
# full outer join IBL_OWNER.pbl_wytwornie wf on wf.wf_wytwornia_id=zw.zawf_wf_wytwornia_id
# full outer join IBL_OWNER.pbl_zapisy_wydawnictwa zwyd on zwyd.zawy_za_zapis_id=z.za_zapis_id
# full outer join IBL_OWNER.pbl_wydawnictwa wy on wy.wy_wydawnictwo_id=zwyd.zawy_wy_wydawnictwo_id
# where zr.zr_miejsce_wyd like '%{miasto}%' 
# OR te.te_miasto like '%{miasto}%' 
# OR wf.wf_miasto like '%{miasto}%'
# OR wy.wy_miasto like '%{miasto}%'
# OR zrr.zrr_miejsce_wydania like '%{miasto}%'
# OR z.za_miejsce_wydania like '%{miasto}%'"""


# miasto = "fifirifitata".replace("'", '_')

# query = f"""select z.za_zapis_id, z.za_status_imp
# from pbl_zapisy z
# full outer join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id = z.za_zr_zrodlo_id
# full outer join IBL_OWNER.pbl_teatry te on te.te_teatr_id = z.za_te_teatr_id
# full outer join IBL_OWNER.pbl_zapisy_wytwornie zw on zw.zawf_za_zapis_id=z.za_zapis_id
# full outer join IBL_OWNER.pbl_wytwornie wf on wf.wf_wytwornia_id=zw.zawf_wf_wytwornia_id
# full outer join IBL_OWNER.pbl_zapisy_wydawnictwa zwyd on zwyd.zawy_za_zapis_id=z.za_zapis_id
# full outer join IBL_OWNER.pbl_wydawnictwa wy on wy.wy_wydawnictwo_id=zwyd.zawy_wy_wydawnictwo_id
# where zr.zr_miejsce_wyd like '%{miasto}%' 
# OR te.te_miasto like '%{miasto}%' 
# OR wf.wf_miasto like '%{miasto}%'
# OR wy.wy_miasto like '%{miasto}%'"""

# sql_answer = '|'.join(pd.read_sql(query, con=connection).fillna(value = 'IOK')['ZA_STATUS_IMP'].unique())


































