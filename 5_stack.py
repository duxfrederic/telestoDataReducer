#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 19:00:16 2022

@author: fred
"""


from pathlib import Path
import numpy as np
from astropy.io import fits
from ccdproc import ImageFileCollection, Combiner

from database import ImageBase
from config import  dbname, workdir, target, outdir

workdir = Path(workdir)
outdir = Path(outdir)

db = ImageBase(dbname)
allimages = db.select(['object'], 
                      [target],
                      returnType='dict')

filters = list(set([e['filter'] for e in allimages]))
scaling_func = lambda arr: 1/np.ma.average(arr)
    
for filter in filters:
    relevantfiles = [e['alignedpath'] for e in allimages if e['filter'] == filter]
    cmbcol = ImageFileCollection(filenames=relevantfiles)
    cmb = Combiner(cmbcol.ccds(ccd_kwargs={'unit':'adu'}))
    cmb.scaling = scaling_func
    cmb.sigma_clipping(low_thresh=2, high_thresh=4, func=np.ma.median)
    av = cmb.average_combine()
    safefilter = filter.replace(' ', '').replace('/', '')
    path = outdir / f"{target}_filter{safefilter}.fits"
    data = av.data.astype(np.float32) 
    data -= np.min(data)
    data /= np.max(data)
    fits.writeto(path, data, overwrite=True)
