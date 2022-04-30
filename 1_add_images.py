#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 14:18:54 2022

@author: fred
"""
from pathlib import Path
from astropy.io import fits

from database import ImageBase, minimaldbfields
from config import datadir, dbname

datadir = Path(datadir)
db = ImageBase(dbname)
db.create(minimaldbfields)

def addImage(imagepath):
    hdr = fits.getheader(imagepath)
    db.insert({'path':imagepath,
               'imagetyp':hdr['imagetyp'],
               'exptime':hdr['exptime'],
               'binning':hdr['xbinning'],
               'airmass':hdr['airmass'],
               'object':hdr['object'],
               'focpos':hdr['focpos'],
               'filter':hdr['filter'],
               'dateobs':hdr['date-obs'],
               'ccdtemp':hdr['ccd-temp']})

files = datadir.rglob('*.fits')

for file in files:
    try:
        addImage(file)
    except Exception as e:
        print(f"Problem with image {file}: {e}")


select = db.execute('select count(object) as c, sum(exptime)/3600 as s, object'
                    ' from images group by object order by s desc')

print("I have the following objects:\n")
print(72*'-')
print(f"{'object':>30} {'total exptime':>20} {'# of exposures':>20}")
print(72*'-')
for c, t, obj in select:
    print(f"{obj:>30} {t:>19.02f}h {c:>20}")
print(72*'-')