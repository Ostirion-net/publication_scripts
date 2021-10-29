from urllib.request import urlopen
from xml.etree import ElementTree as et
import pandas as pd
import os
import re

lvl = 0

tags_to_capture = ['title',
                   'ContractFolderID',
                   'ContractFolderStatusCode',
                   'TotalAmount',
                   'ItemClassificationCode',
                   'EndDate',
                   'EndTime'
                   ]

source = 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom'

def get_file(url):
    try:
        f = urlopen(url)
    except:
        print('URL Error.')
        return False
    myfile = f.read()
    header = 'http://www.w3.org/2005/Atom'
    string = myfile.decode('UTF-8').replace(header,'')
    return string

def extract_header(file, cut_off=14):
    header = file.split('\n')[:cut_off]
    return header

def get_date(file):
    header = extract_header(file)
    date_pattern = "<updated>(.*?)\</updated>"
    date = None
    for l in header:
        if '<updated>' in l:
            date= re.search(date_pattern, l).group(1)
    return date

def get_next_url(file):
    header = extract_header(file)
    next_pattern = '<link href=(.*?)\ rel="next"/>'
    next_file = None
    for l in header:
        if 'next' in l:
            next_file = re.search(next_pattern, l).group(1)

    return next_file.strip('"')

def get_same_day(file):

    needed_urls = [file]
    file = get_file(file)
    date = get_date(file)[0:10]
    if not file:
        print('File chain prematurely broken.')
        return needed_urls, date

    next_date = date
    print('Start fetching chain...')

    while True:
        next_url = get_next_url(file)
        print(next_url)
        new_file = get_file(next_url)
        if not new_file:
            print('File chain prematurely broken.')
            return needed_urls, date

        next_date = get_date(new_file)[0:10]

        if next_date != date:
            break

        needed_urls.append(next_url)
        print(next_date)
        file = new_file

    return needed_urls, date

needed_files, date = get_same_day(source)
print(date)

def get_entry(node, entry):

    global lvl
    global tags_to_capture

    lvl = lvl + 1

    if '}' in node.tag:
        tag = node.tag.split('}')[1]
    else:
        tag = node.tag

    tag_id = str(lvl)+':'+tag

    #print(tag_id)
    if tag_id.split(':')[1] in tags_to_capture:
        entry[tag_id.split(':')[1]] = node.text

    children = list(node)
    total_children = len(children)
    if total_children == 0: lvl = lvl -1
    for i in range(total_children):
        get_entry(children[i], entry)

    return

def get_df(root):
    global lvl

    all_entries = []
    for node in root:
        if 'entry' in node.tag:
            lvl = 0
            entry = {}
            get_entry(node, entry)
            all_entries.append(entry)

    df=pd.DataFrame.from_records(all_entries)
    df.dropna(axis=0, how='all', inplace=True)

    return df

full_df = pd.DataFrame(columns = tags_to_capture)
full_df.columns = tags_to_capture

for file in needed_files:
    root = et.fromstring(get_file(file))
    df = get_df(root)
    full_df = full_df.append(df)

df = full_df.dropna(axis=0, how='all')

script_directory = os.path.dirname(os.path.abspath(__file__))
records_dir = os.path.join(script_directory, 'saved_records')

try:
    os.mkdir(records_dir)
except:
    print('Saved records folder found, skipping folder creation.')

record_name = date[0:10].replace('-','') + '_record' + '.csv'
df.to_csv(os.path.join(records_dir, record_name))
