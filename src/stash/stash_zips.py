#!/usr/bin/env python3

import csv
from pathlib import Path
import re
import requests
from urllib.parse import urljoin
from zipfile import ZipFile


DEST_DIR = Path('data', 'stashed')
SRC_PATH = Path('data', 'collated', 'hpms-zipurls.csv')


def create_destpath(link):
    ydir = DEST_DIR.joinpath(link['year'])
    ydir.mkdir(parents=True, exist_ok=True)
    destpath = ydir.joinpath(link['name'] + '.zip')
    return destpath


def extract_links(txt):
    return list(csv.DictReader(txt.splitlines()))

def does_zip_exist(zipname):
    try:
        z = ZipFile(zipname)

    except FileNotFoundError:
        return False
    except BadZipFile:
        return False
    else:
        return True

def fetch(url):
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.content
    else:
        raise IOError("Status code {n} for: {u}".format(n=resp.status_code, u=url))

def main():
    txt =  Path(SRC_PATH).read_text()
    for link in extract_links(txt):
        destpath = create_destpath(link)
        if does_zip_exist(destpath):
            print("Skipping (already exists):", destpath)
        else:
            url = link['url']
            print("Fetching:", url)
            content = fetch(url)

            with open(destpath, 'wb') as w:
                print(f"Writing {len(content)} to: {destpath}")
                w.write(content)


if __name__ == '__main__':
    main()
