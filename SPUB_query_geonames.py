from tqdm import tqdm
import requests
import pandas as pd
import time
from urllib.error import HTTPError
from http.client import RemoteDisconnected
import regex as re
from difflib import SequenceMatcher
from my_functions import simplify_string



url = 'http://api.geonames.org/searchJSON?'
params = {'username': 'ksuhiyp', 'q': 'Belostok', 'country': '', 'featureClass': 'P', 'continentCode': '', 'fuzzy': '0.6'}
result = requests.get(url, params=params).json()

test = pd.DataFrame.from_dict(result['geonames'])
test['similarity'] = test['toponymName'].apply(lambda x: SequenceMatcher(0,simplify_string('Belostok'),simplify_string(x)).ratio())

ttt = test[test['fcodeName'] == 'seat of a first-order administrative division']





