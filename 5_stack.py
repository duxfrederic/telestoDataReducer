#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 19:00:16 2022

@author: fred
"""


from pathlib import Path
import numpy as np
from astropy.io import fits
from ccdproc import ImageFileCollection, Combiner, combine

from database import ImageBase
from config import  dbname, workdir, target, outdir,\
                    lowstd, highstd

workdir = Path(workdir)
outdir = Path(outdir)

db = ImageBase(dbname)
allimages = db.select(['object'], 
                      [target],
                      returnType='dict')

filters = list(set([e['filter'] for e in allimages]))
# scaling_func = lambda arr: 1/np.ma.average(arr)
    
for filter in filters:
    relevantfiles = [e['alignedpath'] for e in allimages if e['filter'] == filter]
    relevantfiles = [p for p in relevantfiles if p]
    if len(relevantfiles) == 0:
        break
    cmbcol = ImageFileCollection(filenames=relevantfiles)
    cmb = Combiner(cmbcol.ccds(ccd_kwargs={'unit':'adu'}))
    # cmb.scaling = scaling_func
    cmb.sigma_clipping(low_thresh=lowstd, high_thresh=highstd, 
                        func=np.ma.median, use_astropy=True)
    # av = combine(relevantfiles, 
    #              unit='adu',
    #              method='average',
    #              sigma_clip=True,
    #              sigma_clip_low_thresh=1,
    #              sigma_clip_high_thresh=1)
    av = cmb.average_combine()
    safefilter = filter.replace(' ', '').replace('/', '')
    path = outdir / f"{target}_filter{safefilter}.fits"
    data = av.data.astype(np.float32) 
    data -= np.min(data)
    data /= np.max(data)
    fits.writeto(path, data, overwrite=True)
#%%
import matplotlib.pyplot as plt
# av = np.median([j for j in cmb.ccd_list], axis=0)
def see(im):
    v, V = np.nanpercentile(im, (1, 99))
    plt.figure()
    plt.imshow(im, vmin=v, vmax=V)
    plt.waitforbuttonpress()
see(av)
#%%
# for j in cmb.ccd_list:
    # see(j.data)
