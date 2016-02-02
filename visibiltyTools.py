import arrayConfigurationTools as ac
from scipy.stats import rayleigh

import numpy as np
import math


def compute_radialuv(file_casa):
    """
    Compute the radial distance in the uv plane
    """

    f = open(file_casa, 'r')

    xx = []
    yy = []

    for line in f:
        if line[0] != "#":
            dat = line.split()

            xx.append(float(dat[0]))
            yy.append(float(dat[1]))

    f.close()

    nant = len(xx)
    nbl = nant * (nant - 1) / 2
    rad_uv = np.zeros(nbl)

    index = 0
    for i in range(nant):
        for j in range(0, i):
            r2 = (xx[i] - xx[j]) * (xx[i] - xx[j]) + (yy[i] - yy[j]) * (
                yy[i] - yy[j])
            rad_uv[index] = math.sqrt(r2)
            index += 1

    return rad_uv


def compute_bl(ar, freq, las=False):
    """
    compute the BL in meter for a resolution ar (applying a Briggs correction
    """

    try:
        bl_length = 61800 / (freq * ar)
    except ZeroDivisionError:
        bl_length = 0.

    if bl_length < 165.6 and not las:
        bl_length = 165.6

    if las:
        if bl_length > 248.3:
            bl_length = 248.3

    return bl_length


def compute_array_ar(ruv):
    x = np.linspace(0, ruv.max() + 100., 1000)
    param = rayleigh.fit(ruv)
    pdf_fitted = rayleigh.pdf(x, loc=param[0], scale=param[1])
    interval = rayleigh.interval(0.992, loc=param[0], scale=param[1])
    linea = min(interval[1], ruv.max())
    return 61800 / (100. * linea)


def compute_array_ar_check(ruv):
    x = np.linspace(0, ruv.max() + 100., 100)
    param = rayleigh.fit(ruv)
    pdf_fitted = rayleigh.pdf(x, loc=param[0], scale=param[1])
    interval = rayleigh.interval(0.992, loc=param[0], scale=param[1])
    linea = min(interval[1], ruv.max())
    return 61800 / (100. * linea), pdf_fitted, param, ruv, interval, x

