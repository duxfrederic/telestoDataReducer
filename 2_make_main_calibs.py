#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 28 20:18:54 2022

@author: fred
"""

from pathlib import Path
import numpy as np
from ccdproc import CCDData, ImageFileCollection, Combiner, subtract_bias,\
                    subtract_dark
from astropy.time import Time
from astropy.units import s


from database import ImageBase
from config import  dbname, calibdir, flat, bias, dark, workdir,\
                    lowstd, highstd
from module_remove_stars_flats import removeStarsFromArray

calibdir = Path(calibdir)
workdir = Path(workdir)

db = ImageBase(dbname)

db.create(['date:str', 'filter:str', 'binning:int', 'path:str'], 
          tablename='mainflats')
db.create(['date:str', 'binning:int', 'path:str'], 
          tablename='mainbias')
db.create(['date:str', 'binning:int', 'exptime:float', 'path:str'], 
          tablename='maindarks')

#################################### biases ###################################
allbinnings = db.select(['imagetyp'], 
                        [bias], 
                        filter=['binning'])
allbinnings = list(set(allbinnings))
for binning in allbinnings:
    relevantentries = db.select(['binning', 'imagetyp'], 
                                [binning, bias], 
                                returnType='dict')
    alldates = [e['dateobs'] for e in relevantentries]
    alldates = list(Time(alldates, format='isot', scale='utc').to_value('mjd'))
    alldatesrounded = [round(e) for e in alldates]
    alldatesrounded = list(set(alldatesrounded))
    alldates = np.array(alldates)
    for date in alldatesrounded:
        relevant = list(np.where((alldates-date <1)*
                                 (date-alldates < 0.5)))[0]
        
        relevantfiles = [relevantentries[i]['path'] for i in relevant]
        if len(relevantfiles) == 0:
            break
        mainbiascmb = ImageFileCollection(filenames=relevantfiles)
        cmb = Combiner(mainbiascmb.ccds(ccd_kwargs={'unit':'adu'}))
        cmb.sigma_clipping(low_thresh=lowstd, high_thresh=highstd, func=np.ma.median)
        av = cmb.average_combine()
        date = Time(date, format='mjd').to_value('isot')
        path = calibdir / f"mainbias_mjd{date}_binning{binning}.fits"
        av.write(path, overwrite=True)
        mainbiases = db.select(['recno'], ['*'], filter=['path'], 
                               tablename='mainbias')
        if not path in mainbiases:
            db.insert({'date':date, 'binning':binning, 'path':path},
                       tablename='mainbias')


#################################### darks ####################################
allbinnings = db.select(['imagetyp'], 
                        [dark], 
                        filter=['binning'])
allbinnings = list(set(allbinnings))
for binning in allbinnings:
    allexptimes = db.select(['binning', 'imagetyp'], 
                            [binning, dark], 
                            filter=['exptime'])
    allexptimes = list(set(allexptimes))
    for exptime in allexptimes:
        relevantentries = db.select(['binning', 'imagetyp', 'exptime'], 
                                    [binning, dark, exptime], 
                                    returnType='dict')
        if len(relevantentries) == 0:
            break
        # for each date, select the closest main bias and subtract it:
        mainbiases =  db.select(['binning'], 
                                [3], 
                                tablename='mainbias',
                                returnType='dict')
        mainbiasdates = np.array([Time(e['date']).to_value('mjd') 
                                        for e in mainbiases])
        
        for entry in relevantentries:
            entrydate = Time(entry['dateobs']).to_value('mjd')
            closesti = np.argmin(np.abs(mainbiasdates-entrydate))
            biasccd = CCDData.read(mainbiases[closesti]['path'], unit='adu')
            darkccd = CCDData.read(entry['path'], unit='adu')
            reddark = subtract_bias(darkccd, biasccd)
            filename = Path(entry['path']).name 
            
            writepath = workdir / filename.replace('.fits', '_red.fits')
            reddark.write(writepath, overwrite=True)
            db.update(['recno'], [entry['recno']], {'reducedpath':writepath})
            # ah, and update it in our entry as well so that we do not
            # need to query the database again:
            entry['reducedpath'] = writepath

        alldates = [e['dateobs'] for e in relevantentries]
        alldates = list(Time(alldates, format='isot', scale='utc').to_value('mjd'))
        

            
        alldatesrounded = [round(e) for e in alldates]
        alldatesrounded = list(set(alldatesrounded))
        
        alldates = np.array(alldates)
        for date in alldatesrounded:
            relevant = list(np.where((alldates-date <1)*
                                     (date-alldates < 0.5)))[0]
            if len(relevant) == 0:
                break
            # now we have all the dates with darks with this exptime and binning.
            
            relevantfiles = [relevantentries[i]['reducedpath'] for i in relevant]
            maindarkcmb = ImageFileCollection(filenames=relevantfiles)
            cmb = Combiner(maindarkcmb.ccds(ccd_kwargs={'unit':'adu'}))
            cmb.sigma_clipping(low_thresh=lowstd, high_thresh=highstd, func=np.ma.median)
            av = cmb.average_combine()
            date = Time(date, format='mjd').to_value('isot')
            path = calibdir / f"maindark_date{date}_binning{binning}_exptime{exptime}.fits"
            av.write(path, overwrite=True)
            maindarks = db.select(['recno'], ['*'], filter=['path'], tablename='maindarks')
            if not path in maindarks:
                db.insert({'date':date, 'binning':binning, 'path':path,
                           'exptime':exptime},
                           tablename='maindarks')

#################################### flats ####################################
allbinnings = db.select(['imagetyp'], 
                        [flat], 
                        filter=['binning'])
allbinnings = list(set(allbinnings))
for binning in allbinnings:
    allfilters = db.select(['binning', 'imagetyp'], 
                            [binning, flat], 
                            filter=['filter'])
    allfilters = list(set(allfilters))
    for filter in allfilters:
        relevantentries = db.select(['binning', 'imagetyp', 'filter'], 
                                    [binning, flat, filter], 
                                    returnType='dict')
        if len(relevantentries) == 0:
            break
        # for each date, select the closest main bias and dark and subtract them:
        mainbiases =  db.select(['binning'], 
                                [3], 
                                tablename='mainbias',
                                returnType='dict')
        mainbiasdates = np.array([Time(e['date']).to_value('mjd') 
                                        for e in mainbiases])
        maindarks     =  db.select(['binning'], 
                                   [3], 
                                   tablename='maindarks',
                                   returnType='dict')
        maindarksdates = np.array([Time(e['date']).to_value('mjd') 
                                        for e in maindarks])
        
        for entry in relevantentries:
            entrydate = Time(entry['dateobs']).to_value('mjd')
            
            closesti = np.argmin(np.abs(mainbiasdates-entrydate))
            biasccd = CCDData.read(mainbiases[closesti]['path'], unit='adu')
            
            closestdarki = np.argmin(np.abs(maindarksdates-entrydate))
            darkccd = CCDData.read(maindarks[closestdarki]['path'], unit='adu')
            
            flatccd = CCDData.read(entry['path'], unit='adu')
            redflat1 = subtract_bias(flatccd, biasccd)
            redflat = subtract_dark(redflat1, darkccd, 
                                    dark_exposure=maindarks[closestdarki]['exptime']*s,
                                    data_exposure=entry['exptime']*s)
        
            # remove potential stars from the flat:
            redflat.data = removeStarsFromArray(redflat.data)
            filename = Path(entry['path']).name 
            writepath = workdir / filename.replace('.fits', '_red.fits')
            
            redflat.write(writepath, overwrite=True)
            db.update(['recno'], [entry['recno']], {'reducedpath':writepath})
            # ah, and update it in our entry as well so that we do not
            # need to query the database again:
            entry['reducedpath'] = writepath
            
        # now we combine the flats
        alldates = [e['dateobs'] for e in relevantentries]
        alldates = list(Time(alldates, format='isot', scale='utc').to_value('mjd'))
        

            
        alldatesrounded = [round(e) for e in alldates]
        alldatesrounded = list(set(alldatesrounded))
        
        alldates = np.array(alldates)
        for date in alldatesrounded:
            relevant = list(np.where((alldates-date <1)*
                                     (date-alldates < 0.5)))[0]
            if len(relevant) == 0:
                break
            # now we have all the dates with darks with this exptime and binning.
            relevantfiles = [relevantentries[i]['reducedpath'] for i in relevant]
            mainflatcmb = ImageFileCollection(filenames=relevantfiles)
            cmb = Combiner(mainflatcmb.ccds(ccd_kwargs={'unit':'adu'}))
            cmb.sigma_clipping(low_thresh=lowstd, high_thresh=highstd, func=np.ma.median)
            av = cmb.average_combine()
            date = Time(date, format='mjd').to_value('isot')
            safefilter = filter.replace(' ', '').replace('/', '')
            path = calibdir / f"mainflat_date{date}_binning{binning}_filter{safefilter}.fits"
            av.write(path, overwrite=True)
            mainflats = db.select(['recno'], ['*'], filter=['path'], tablename='mainflats')
            if not path in mainflats:
                db.insert({'date':date, 'binning':binning, 'path':path,
                           'filter':filter},
                           tablename='mainflats')

print("Done with preparing main calibrations")
