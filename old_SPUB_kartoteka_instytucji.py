import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from my_functions import gsheet_to_df
import pandas as pd
from tqdm import tqdm
import json


#%%
instytucje_oryg = gsheet_to_df('1cFlpXHd5NLsiCOw25L-6JhhovcRHxi7QRVSy16exJ2I', 'instytucje')

instytucje = instytucje_oryg.copy()
instytucje['ID_INSTYTUCJI'] = instytucje[['ID_INSTYTUCJI', 'poprawne ID rodzica']].apply(lambda x: x['poprawne ID rodzica'] if not(isinstance(x['poprawne ID rodzica'], float)) else x['ID_INSTYTUCJI'], axis=1)

temp_dict = {}
for i, row in tqdm(instytucje.iterrows(), total=instytucje.shape[0]):
    if row['instytucja_relacja_id'] not in temp_dict and pd.notnull(row['instytucja_relacja_id']):
        temp_dict[row['instytucja_relacja_id']] = [row['ID_INSTYTUCJI']]
    elif pd.notnull(row['instytucja_relacja_id']):
        temp_dict[row['instytucja_relacja_id']].append(row['ID_INSTYTUCJI'])
        
# errors = []
# for k,v in temp_dict.items():
#     try:
#         instytucje[instytucje['ID_INSTYTUCJI'] == k]['NAZWA_INSTYTUCJI_UJEDNOLICONA'].to_list()[0]
#         [instytucje[instytucje['ID_INSTYTUCJI'] == e]['NAZWA_INSTYTUCJI_UJEDNOLICONA'].to_list()[0] for e in v]
#     except IndexError:
#         errors.append((k,v))

temp_dict2 = {instytucje[instytucje['ID_INSTYTUCJI'] == k]['NAZWA_INSTYTUCJI_UJEDNOLICONA'].to_list()[0]:[instytucje[instytucje['ID_INSTYTUCJI'] == e]['NAZWA_INSTYTUCJI_UJEDNOLICONA'].to_list()[0] for e in v] for k,v in temp_dict.items()}

with open('instytucje_do_sprawdzenia.json', 'w', encoding='utf-8') as f:
    json.dump(temp_dict2, f, indent=4, sort_keys=True, ensure_ascii=False)


k = '1'
v = ['408', '540', '738', '739', '740', '1370', '2163', '7536', '14923', '15175', '15185', '15909', '15926', '21065', '21066', '22198']


#w pierwszej kolejności poprawić błędy w tabeli Instytucje – hierarchia

instytucje.columns.values






temp_dict