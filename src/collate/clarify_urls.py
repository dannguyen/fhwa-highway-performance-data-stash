#!/usr/bin/env python3
import csv
from lxml.html import fromstring as hparse
from pathlib import Path
import re
from urllib.parse import urljoin


DEST_PATH = Path('data', 'collated', 'hpms-zipurls.csv')
SRC_PATH = Path('data', 'stashed', 'hpms-original.html')
SRC_URL = 'https://www.fhwa.dot.gov/policyinformation/hpms/shapefiles.cfm'

ERRATA_NAMES = {
    'arkanas': 'arkansas',
    'deleware': 'delaware',
    'massacusetts': 'massachusetts',
    'nebraska20913': 'nebraska2013',
    'district2': 'districtofcolumbia2',
    r'pa_nhs$': 'pa_nhs2011',
    'pa_nhs_': 'pa_nhs',
    'nationalarterial2017': 'pa_nhs2017',
}


def _extract(link):
    href = link.attrib['href']
    stem = Path(href).stem

    # deal with name fixes
    for err, fix in ERRATA_NAMES.items():
        if re.search(err, stem):
            stem = re.sub(err, fix, stem)

    # special case for which missouri2015 and missouri2016 are both spelled
    # as missouri2015t
    if 'missouri2015t' in stem:
        if 'Missouri (8 MB)' in link.text:
            # the 2015t in the URL is fine, but name should be: missouri2015
            stem = 'missouri2015'
        elif 'Missouri (42 MB)' in link.text:
            stem = 'missouri2016'
            link.attrib['href'] = link.attrib['href'].replace('missouri2015t', 'missouri2016')
    url = urljoin(SRC_URL, href)

    try:
        name, year = re.search(r'(\w+)(20\d\d)$', stem).groups()
    except AttributeError oops:
        print(stem)
        raise oops
    else:
        return {'name': name, 'year': year, 'url': url}


def extract_things(html):
    doc = hparse(html)
    links = doc.xpath('//a[contains(@href, "shapefiles/")]')
    return [_extract(link) for link in links]

def main():
    html =  Path(SRC_PATH).read_text()
    things = extract_things(html)

    with open(DEST_PATH, 'w') as w:
        c = csv.DictWriter(w, fieldnames=things[0].keys())
        c.writeheader()
        c.writerows(things)


if __name__ == '__main__':
    DEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    main()
