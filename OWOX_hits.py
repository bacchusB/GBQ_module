#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module only for OWOX hits tables
"""

import pandas as pd
import math

def peaces_of_hits(hitIds,step=3000):
    """ Requires list or list like object
    Returns list of peaces"""
    hits_arr=[]
    step=step
    start=0
    end=step
    count_steps=math.ceil(len(hitIds)/step)
    for i in range(count_steps):
        hits_arr.append(hitIds[start:end])
        start+=step
        end+=step
    return hits_arr
