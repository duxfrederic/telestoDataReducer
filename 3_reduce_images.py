#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 16:51:35 2022

@author: fred
"""


import multiprocessing
from pathlib import Path
import numpy as np
from ccdproc import CCDData, subtract_bias,\
                    subtract_dark, flat_correct
from astropy.time import Time
from astropy.units import s
from astropy.io import fits


from database import ImageBase
from config import  dbname, workdir, target, maxcores

workdir = Path(workdir)

db = ImageBase(dbname)


allimages  = db.select(['object'], 
                       [target],
                       returnType='dict')




def reduce(image):
    mainbiases =  db.select(['binning'], 
                            [image['binning']], 
                            tablename='mainbias',
                            returnType='dict')
    mainbiasdates = np.array([Time(e['date']).to_value('mjd') 
                                    for e in mainbiases])
    maindarks     =  db.select(['binning'], 
                               [image['binning']],
                               tablename='maindarks',
                               returnType='dict')
    maindarksdates = np.array([Time(e['date']).to_value('mjd') 
                                    for e in maindarks])
    mainflats      =  db.select(['binning', 'filter'], 
                                [image['binning'], image['filter']], 
                                tablename='mainflats',
                                returnType='dict')
    mainflatsdates = np.array([Time(e['date']).to_value('mjd') 
                                    for e in mainflats])
    imagedate = Time(image['dateobs']).to_value('mjd')
    
    closesti = np.argmin(np.abs(mainbiasdates-imagedate))
    biasccd = CCDData.read(mainbiases[closesti]['path'], unit='adu')

    
    closestdarki = np.argmin(np.abs(maindarksdates-imagedate))
    darkccd = CCDData.read(maindarks[closestdarki]['path'], unit='adu')
    
    closestflati = np.argmin(np.abs(mainflatsdates-imagedate))
    flatccd = CCDData.read(mainflats[closestflati]['path'], unit='adu')
    
    imgccd = CCDData.read(image['path'], unit='adu')
    redimg1 = subtract_bias(imgccd, biasccd)
    redimg2 = subtract_dark(redimg1, darkccd, 
                            dark_exposure=maindarks[closestdarki]['exptime']*s,
                            data_exposure=image['exptime']*s)
    redimg = flat_correct(redimg2, flatccd)
    redimg.data = redimg.data.astype(np.float32)
    redimg.uncertainty.array = redimg.uncertainty.array.astype(np.float32)
    
    filename = Path(image['path']).name 
    writepath = workdir / filename.replace('.fits', '_red.fits')
    fits.writeto(writepath, redimg.data, overwrite=True)
    db.update(['recno'], [image['recno']], {'reducedpath':writepath})
    # ah, and update it in our entry as well so that we do not
    # need to query the database again:
    image['reducedpath'] = writepath


pool = multiprocessing.Pool(processes=maxcores)
pool.map(reduce, allimages)




















