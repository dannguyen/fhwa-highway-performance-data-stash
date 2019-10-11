#!/usr/bin/env python3
from pathlib import Path
from zipfile import ZipFile

SRC_DIR = Path('data', 'stashed', 'zips')
SRC_FILES = SRC_DIR.glob('**/*.zip')
DEST_DIR = Path('data', 'stashed', 'unpacked')

def parse_zipname(zipname):
    d = {}
    d['year'] = zipname.parent.name
    d['name'] = zipname.stem
    return d

def extract_files(zipname):
    f = parse_zipname(zipname)
    destdir = DEST_DIR.joinpath(f['year'], f['name'])
    destdir.mkdir(exist_ok=True, parents=True)

    z = ZipFile(zipname)
    for x in z.filelist:
        srcpath = x.filename
        fileext = srcpath.split('.')[-1]
        destname = f"{f['name']}.{fileext}"

        destpath = destdir.joinpath(destname)
        print("Extracting:", srcpath, "to:", destpath)
        with open(destpath, 'wb') as w:
            w.write(z.read(srcpath))



if __name__ == '__main__':
    for fname in SRC_FILES:
        print("Opening:", fname)
        extract_files(fname)
