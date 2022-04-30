#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 14:17:23 2022

@author: fred
"""

from os.path import join
from os import makedirs

workdir = '/tmp'
datadir = '/scratch/telesto'
calibdir = join(datadir, 'calibdir')
outdir = join(datadir, 'outdir')
makedirs(calibdir, exist_ok=1)
makedirs(outdir, exist_ok=1)

# interpreter to use when launching multiple scripts:
python = "/home/fred/anaconda3/bin/python"

# were do we want our database?
dbname = join(workdir, 'db.db')


# for alignment, we crop the borders before aligning as the edges of the
# image seem to cause problems on telesto data.
crop = 100

# for alignment, how many cores?
maxcores = 4
# redo align?
redoalign = False
###############################################################################
# ok, now we specify the oject (or list of objects) we want to reduce, 
# as well as a date:
target = 'M 101'
date = '2022-04-27'


# name of the differnt fields in the headers, and the database:
flat = 'Flat Field'
light = 'Light Frame'
dark = 'Dark Frame'
bias = 'Bias Frame'


######### stacking
# in units of standard deviations
highstd = 1.7
lowstd  = 1.6
