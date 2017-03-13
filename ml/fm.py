import numpy as np

def stocGradAscent(dataMatrix, classLabels, k, iter):
    # dataMatrix mat, classLabels list
    m,n = np.shape(dataMatrix)
    alpha = 0.01

    w = np.zeros((n, 1))
    w_0 = 0.
    v = np.normalvariate(0, 0.2) * np.ones((n, k))
