#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 18:05:13 2022

@author: fred
"""


import  warnings
import  numpy                    as     np
from    astropy.utils.exceptions import AstropyUserWarning
from    astropy.io               import fits
from    astropy.stats            import sigma_clipped_stats
from    astropy.modeling         import models, fitting 

from    photutils.detection      import DAOStarFinder







def checkBoundaries(array, x, y, N):

    xlow, xhigh = x-N, x+N 
    ylow, yhigh = y-N, y+N 
    shapey, shapex = array.shape 
    xlow = max(0, xlow)
    ylow = max(0, ylow)
    xhigh = min(shapex, xhigh)
    yhigh = min(shapey, yhigh)
    return xlow, xhigh, ylow, yhigh

def extractStampAndMedian(array, x, y, N=10):

    x,y = int(x), int(y)
    xlow, xhigh, ylow, yhigh = checkBoundaries(array, x, y, N)
    stamp = array[ylow:yhigh, xlow:xhigh]
    mean, median, std = sigma_clipped_stats(stamp)
    stamp -= median
    return stamp, median

def addBackStampAndMedian(array, stamp, median, x, y, N=10):

    x, y = int(x), int(y)
    xlow, xhigh, ylow, yhigh = checkBoundaries(array, x, y, N)
    array[ylow:yhigh, xlow:xhigh] = stamp + median

def fitMoffatProfileAndReplace(fullimage, x0, y0, N=10, debug=0):

    # la petite région contenant l'étoile:
    stamp, median = extractStampAndMedian(fullimage, x0, y0, N=N)
    
    # le modèle Moffat:
    moffat        = models.Moffat2D(x_0=x0-int(x0)+N+1, y_0=y0-int(y0)+N, 
                             amplitude=np.max(stamp), gamma=0.3, alpha=0.3)
    fit_p         = fitting.LevMarLSQFitter()
    
    # les coordonnées:
    leny, lenx    = stamp.shape
    x, y          = np.meshgrid(np.arange(lenx), np.arange(leny))
    
    # on fit:
    warnings.filterwarnings("error")
    try:
        moffat        = fit_p(moffat, x, y, stamp)
        model         = moffat(x,y)
        # on soustrait le modèle:
        residuals     = stamp - model
    except AstropyUserWarning:
        # ok, la psf de cette étoile est bizarre. Ou c'est une étoile double ...
        # mettons 0. Un peu plus moche, mais au moins pas un truc divergent
        residuals     = np.zeros_like(stamp) 
    
    # on remet le résultat dans l'image:
    addBackStampAndMedian(fullimage, residuals, median, x0, y0, N=N)
    
    
def removeStarsFromArray(array):
    mean, median, std  = sigma_clipped_stats(array)
    daofind            = DAOStarFinder(threshold=2*std, fwhm=3.5)
    try:
        sources            = daofind(array-median)
    except:
        return array
    print(f"found {len(sources)} stars.")
    
    positions = np.transpose((sources['xcentroid'], sources['ycentroid']))
    for x0, y0 in positions:
        fitMoffatProfileAndReplace(array, x0, y0, N=10, debug=0)
    return array 


if __name__ == "__main__":
    from database import ImageBase
    from config import  dbname
    
    db = ImageBase(dbname)

    mainflats = db.select(['recno'], ['*'], filter=['path'], tablename='mainflats',
                          returnType='dic')
    
    for fitspath in mainflats:
        outpath = fitspath.replace('.fits', 'destar-ed.fits')
        removeStarsFromBayeredArray(fitspath, outpath)
        db.update(['path'], [fitspath], {'path':outpath},
                   tablename='mainflats')


