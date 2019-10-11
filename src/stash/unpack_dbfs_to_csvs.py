#!/usr/bin/env python3
import csv
from dbfread import DBF
from pathlib import Path
from zipfile import ZipFile
from tempfile import NamedTemporaryFile

SRC_DIR = Path('data', 'stashed', 'zips')
SRC_FILES = SRC_DIR.glob('**/*.zip')
DEST_DIR = Path('data', 'stashed', 'csvs')


def extract_dbf_bytes(zipname):
    # returns a temp filename
    z = ZipFile(zipname)
    dbfpath = next(f.filename for f in z.filelist if '.dbf' in f.filename)
    return z.read(dbfpath)
    # f = parse_zipname(zipname)
    # destdir = DEST_DIR.joinpath(f['year'])
    # destdir.mkdir(exist_ok=True, parents=True)

    # z = ZipFile(zipname)
    # for x in z.filelist:
    #     srcpath = x.filename
    #     fileext = srcpath.split('.')[-1]
    #     destname = f"{f['name']}.{fileext}"

    #     destpath = destdir.joinpath(destname)
    #     print("Extracting:", srcpath, "to:", destpath)
    #     with open(destpath, 'wb') as w:
    #         w.write(z.read(srcpath))


def extract_records(dbfname):
    d = DBF(dbfname, lowernames=True, load=False)
    headers = [f.name for f in d.fields]
    return (d.records, headers)

def parse_zipname(zipname):
    d = {}
    d['year'] = zipname.parent.name
    d['name'] = zipname.stem
    return d


if __name__ == '__main__':
    for zn in SRC_FILES:
        print("Opening:", zn)
        tn = NamedTemporaryFile('wb')
        tn.write(extract_dbf_bytes(zn))
        records, fields = extract_records(tn.name)

        p = parse_zipname(zn)
        destpath = DEST_DIR.joinpath(p['year'], p['name'] + '.csv')
        destpath.parent.mkdir(exist_ok=True, parents=True)
        with open(destpath, 'w') as w:
            print("Writing to:", destpath)
            c = csv.DictWriter(w, fieldnames=fields)
            c.writeheader()
            for r in records:
                c.writerow(r)
            tn.close()
