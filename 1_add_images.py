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
