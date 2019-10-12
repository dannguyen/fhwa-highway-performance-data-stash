#!/usr/bin/env python3
import csv
from dbfread import DBF, FieldParser
from sys import stderr
from pathlib import Path
from tempfile import NamedTemporaryFile
from zipfile import ZipFile

SRC_DIR = Path('data', 'stashed', 'zips')
SRC_FILES = SRC_DIR.glob('**/*.zip')
DEST_DIR = Path('data', 'stashed', 'csvs')
global _ERRS
_ERRS = []

"""these two files have errors
data/stashed/csvs/2016/california.csv:478174
data/stashed/csvs/2016/virginia.csv:19306
"""


class HpmsParser(FieldParser):
    # because on 2016/california.dbf, we got this error:
    # ~/.pyenv/versions/anaconda3-5.3.1/lib/python3.7/site-packages/dbfread/field_parser.py in parseN(self, field, data)
    #     167         try:
    # --> 168             return int(data)
    #     169         except ValueError:

    # ValueError: invalid literal for int() with base 10: b'_LA_SHERMA'
    def parseN(self, field, value):
        try:
            x = FieldParser.parseN(self, field, value)
        except ValueError as err:
            x = value.decode('utf8')
            global _ERRS
            _ERRS.append([field.name, x])
        finally:
            return x
    def parseF(self, field, value):
        try:
            x = FieldParser.parseF(self, field, value)
        except ValueError as err:
            x = value.decode('utf8')
        finally:
            return x


def extract_dbf_bytes(zipname):
    # returns a temp filename
    z = ZipFile(zipname)
    dbfpath = next((f.filename for f in z.filelist if '.dbf' in f.filename), None)
    return z.read(dbfpath) if dbfpath else None

def extract_records(dbfname):
    d = DBF(dbfname, lowernames=True, load=False, parserclass=HpmsParser)
    headers = [f.name for f in d.fields]
    return (iter(d.records), headers, len(d.records))

def parse_zipname(zipname):
    d = {}
    d['year'] = zipname.parent.name
    d['name'] = zipname.stem
    return d


def write_records_to_csv(destpath, records, fieldnames, rowcount):
    global _ERRS
    _ERRS = []
    errors = []
    destpath.parent.mkdir(exist_ok=True, parents=True)

    with open(destpath, 'w') as w:
        c = csv.DictWriter(w, fieldnames=fieldnames)
        c.writeheader()
        for i, r in enumerate(range(rowcount)):
            c.writerow(next(records))

            if _ERRS:
                for e in _ERRS:
                   msg = '|'.join([str(destpath), str(i)] + e)
                   errors.append(msg)
                _ERRS = []

    tn.close()
    return errors

if __name__ == '__main__':
    for zn in SRC_FILES:
        p = parse_zipname(zn)
        destpath = DEST_DIR.joinpath(p['year'], p['name'] + '.csv')

        if destpath.exists():
            print("Skipping: {}; this already exists: {}".format(zn, destpath))
            continue

        print("Opening:", zn)
        dbytes = extract_dbf_bytes(zn)
        if not dbytes:
            # some zip files, such as pa_nhs national files, do not have a DBF
            continue

        tn = NamedTemporaryFile('wb')
        tn.write(dbytes)
        records, fields, rowcount = extract_records(tn.name)

        print("Writing", rowcount, "records to:", destpath)

        errors = write_records_to_csv(destpath, records, fields, rowcount)
        if errors:
            print("\tErrors found:", len(errors))
            for e in errors:
                stderr.write(e+"\n")
