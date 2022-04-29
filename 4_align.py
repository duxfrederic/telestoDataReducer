#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 18:23:34 2022

@author: fred
"""

import multiprocessing
import astroalign as aa
from pathlib import Path
import numpy as np
from astropy.io import fits


from database import ImageBase
from config import  dbname, workdir, target, maxcores, crop

workdir = Path(workdir)

db = ImageBase(dbname)



allimages  = db.select(['object'], 
                       [target],
                       returnType='dict')

refimg = allimages[len(allimages)//2]
refimgarray = fits.getdata(refimg['reducedpath'])[crop:-crop, crop:-crop]

def alignOneImage(image):
    path = image['reducedpath']
    array = fits.getdata(path)[crop:-crop, crop:-crop]
    
    aligned = aa.register(array.astype(np.float64), 
                          refimgarray.astype(np.float64))
    
    
    outname = Path(path).name
    outname = workdir / outname.replace('.fits', '_aligned.fits')
    fits.writeto(outname, aligned[0].astype(np.float32), overwrite=1)
    db.update(['recno'], [image['recno']], {'alignedpath':outname})

# for image in allimages:
    # alignOneImage(image)
pool = multiprocessing.Pool(processes=maxcores)
pool.map(alignOneImage, allimages)