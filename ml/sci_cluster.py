# -*- coding: utf-8 -*-
"""
Created on Tue Aug 04 15:31:09 2015

@author: sijia.lai
"""

from sklearn import datasets

iris = datasets.load_iris()
print iris.data.shape
print iris.target.shape

import numpy as np

print np.unique(iris.target)

digits = datasets.load_digits()
print digits.images.shape

import pylab as pl

pl.imshow(digits.images[0], cmap=pl.cm.gray_r)
pl.show()
