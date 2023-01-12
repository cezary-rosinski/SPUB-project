import json

#%% def

def give_fake_id(places):
    fake_id = 0
    for pl in places:
        if pl.id.endswith(('Q', 'QNone')):
            pl.id = f"http://www.wikidata.org/entity/fake{fake_id}"
            fake_id += 1

#%% main

#plik od MG, który ma miejsca jako tematy – MG ma wygenerować lepszej jakości plik
with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-01-12\geographic.json", encoding='utf-8') as f:
    data = json.load(f)

#raczej nie będzie potrzebne
# wydobycie miejsc z rekordów bibliograficznych
# with open(r"F:\Cezary\Documents\IBL\Libri\dane z libri do pbl\2023-01-12\biblio.json", encoding='utf-8') as f:
#     data = json.load(f)

data = [e for e in data if 'Book' in e['format_major']]
    
    
class Place:
    
    def __init__(self, id_, lat, lon, geonames=''):
        self.id = f"http://www.wikidata.org/entity/Q{id_}" #do przemyślenia, co robić, gdy id jest puste
        self.lat = lat
        self.lon = lon
        self.geonames = geonames
        
    @classmethod
    def from_dict(cls, place_dict):
        id_ = place_dict.get('wiki')
        lat, lon = place_dict.get('coordinates').split(',') if place_dict.get('coordinates') else ['', '']
        return cls(id_, lat, lon)
        
# place_1 = Place('abc', '12', '21')    
# print(place_1.__dict__)

# place_2 = Place.from_dict(data[0])
# print(place_2.__dict__)

places = [Place.from_dict(e) for e in data]
give_fake_id(places)

# [e.id for e in places]


# class PlacePeriod:
# class PlacePeriodName:
# class PlacePeriodCountry:
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
