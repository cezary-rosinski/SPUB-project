from ast import arg
from threading import Thread, Lock
import requests
import time
from queue import Queue
from concurrent.futures import ThreadPoolExecutor


def get_viaf(q, lock):
    global output_list
    while True:
        url = q.get()
        response = requests.get(url)
        with lock:
            output_list.append(response.json()['viafID'])
        q.task_done()

#kontenerem powinien być słownik, żeby zrócić odpowiedź do konkretnego klucza

start = time.time()
lista_viaf = [
    'https://viaf.org/viaf/64009368/viaf.json',
    'https://viaf.org/viaf/102376422/viaf.json',
    'https://viaf.org/viaf/73863844/viaf.json',
    'https://viaf.org/viaf/49338782/viaf.json',
    'https://viaf.org/viaf/88346011/viaf.json',
    'https://viaf.org/viaf/110389400/viaf.json',
    'https://viaf.org/viaf/71386455/viaf.json',
    'https://viaf.org/viaf/4936996/viaf.json',
    'https://viaf.org/viaf/7428033/viaf.json'
    ]

q = Queue()
lock = Lock()
output_list = []

for _ in range(9):
    thread = Thread(target=get_viaf, args=(q, lock))
    thread.daemon = True
    thread.start()

for url in lista_viaf:
    q.put(url)

q.join()

end = time.time()
print(end-start)
print(output_list)

start = time.time()
out2 = []
for el in lista_viaf:
    response = requests.get(el)
    out2.append(response.json()['viafID'])
end = time.time()
print(end-start)

def get_viaf(url):
    global output_list
    response = requests.get(url)
    with lock:
        output_list.append(response.json()['viafID'])

start = time.time()
lista_viaf = [
    'https://viaf.org/viaf/64009368/viaf.json',
    'https://viaf.org/viaf/102376422/viaf.json',
    'https://viaf.org/viaf/73863844/viaf.json',
    'https://viaf.org/viaf/49338782/viaf.json',
    'https://viaf.org/viaf/88346011/viaf.json',
    'https://viaf.org/viaf/110389400/viaf.json',
    'https://viaf.org/viaf/71386455/viaf.json',
    'https://viaf.org/viaf/4936996/viaf.json',
    'https://viaf.org/viaf/7428033/viaf.json'
    ]

output_list = []
lock = Lock()
with ThreadPoolExecutor() as excecutor:
    excecutor.map(get_viaf, lista_viaf)

end = time.time()
print(end-start)
print(output_list)


#%%
from concurrent.futures import ThreadPoolExecutor

def get_viaf_name(viaf_url):
    url = viaf_url + '/viaf.json'
    r = requests.get(url)
    try:
        if r.json().get('mainHeadings'):
            if isinstance(r.json()['mainHeadings']['data'], list):
                name = r.json()['mainHeadings']['data'][0]['text']
            else:
                name = r.json()['mainHeadings']['data']['text']
            viaf_names_resp[viaf_url] = name
        elif r.json().get('redirect'):
            new_viaf = r.json()['redirect']['directto']
            new_url = 'https://www.viaf.org/viaf/' + new_viaf
            viaf_names_resp[viaf_url] = new_url
            get_viaf_name(new_url)
    except KeyboardInterrupt as exc:
        raise exc
    except:
        raise print(url)

viaf_names_resp = {}
viaf_url_set = set(df['related_viaf'])

with ThreadPoolExecutor(max_workers=50) as excecutor:
    list(tqdm(excecutor.map(get_viaf_name, viaf_url_set)))


#%%
import time
import threading

start = time.perf_counter()

# def do_something(seconds):
#     print('Sleeping 1 second')
#     time.sleep(1)
#     print('Done Sleeping')
    
def do_something(seconds):
    print(f'Sleeping {seconds} second(s)')
    time.sleep(seconds)
    print('Done Sleeping')

# t1 = threading.Thread(target=do_something)
# t2 = threading.Thread(target=do_something)

# t1.start()
# t2.start()

# t1.join()
# t2.join()

# threads = []
# for _ in range(10):
#     t = threading.Thread(target=do_something)
#     t.start()
#     threads.append(t)
    
threads = []
for _ in range(10):
    t = threading.Thread(target=do_something, args=[1.5])
    t.start()
    threads.append(t)
    
for thread in threads:
    thread.join()


finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')


#%%
import concurrent.futures
import time

start = time.perf_counter()

def do_something(seconds):
    print(f'Sleeping {seconds} second(s)')
    time.sleep(seconds)
    return f'Done Sleeping {seconds}'
    
# with concurrent.futures.ThreadPoolExecutor () as executor:
#     f1 = executor.submit(do_something, 1)
#     f2 = executor.submit(do_something, 1)
#     print(f1.result())
#     print(f2.result())
    
# with concurrent.futures.ThreadPoolExecutor () as executor:
#     secs = [5, 4, 3, 2, 1]
#     # results = [executor.submit(do_something, 1) for _ in range(10)]
#     results = [executor.submit(do_something, sec) for sec in secs]
    
#     for f in concurrent.futures.as_completed(results):
#         print(f.result())
    
with concurrent.futures.ThreadPoolExecutor() as executor:
    secs = [5, 4, 3, 2, 1]
    results = executor.map(do_something, secs)
    
    for result in results:
        print(result)

finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)} second(s)')

#%%

import requests
import time
import concurrent.futures

img_urls = [
    'https://images.unsplash.com/photo-1516117172878-fd2c41f4a759',
    'https://images.unsplash.com/photo-1532009324734-20a7a5813719',
    'https://images.unsplash.com/photo-1524429656589-6633a470097c',
    'https://images.unsplash.com/photo-1530224264768-7ff8c1789d79',
    'https://images.unsplash.com/photo-1564135624576-c5c88640f235',
    'https://images.unsplash.com/photo-1541698444083-023c97d3f4b6',
    'https://images.unsplash.com/photo-1522364723953-452d3431c267',
    'https://images.unsplash.com/photo-1513938709626-033611b8cc03',
    'https://images.unsplash.com/photo-1507143550189-fed454f93097',
    'https://images.unsplash.com/photo-1493976040374-85c8e12f0c0e',
    'https://images.unsplash.com/photo-1504198453319-5ce911bafcde',
    'https://images.unsplash.com/photo-1530122037265-a5f1f91d3b99',
    'https://images.unsplash.com/photo-1516972810927-80185027ca84',
    'https://images.unsplash.com/photo-1550439062-609e1531270e',
    'https://images.unsplash.com/photo-1549692520-acc6669e2f0c'
]

t1 = time.perf_counter()

# for img_url in img_urls:
#     img_bytes = requests.get(img_url).content
#     img_name = img_url.split('/')[3]
#     img_name = f'{img_name}.jpg'
#     with open(img_name, 'wb') as img_file:
#         img_file.write(img_bytes)
#         print(f'{img_name} was downloaded...')



def download_image(img_url):
    img_bytes = requests.get(img_url).content
    img_name = img_url.split('/')[3]
    img_name = f'{img_name}.jpg'
    with open(img_name, 'wb') as img_file:
        img_file.write(img_bytes)
        print(f'{img_name} was downloaded...')


with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(download_image, img_urls)


t2 = time.perf_counter()

print(f'Finished in {t2-t1} seconds')
























