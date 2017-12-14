import numpy as np

def stocGradAscent(dataMatrix, classLabels, k, iter):
    # dataMatrix mat, classLabels list
    m,n = np.shape(dataMatrix)
    alpha = 0.01

    w = np.zeros((n, 1))
    w_0 = 0.
    v = np.normalvariate(0, 0.2) * np.ones((n, k))

    for it in xrange(iter):
        print it
        for x in xrange(m): #random optimize, every sample is the same
            inter_1 = dataMatrix[x] * v
            inter_2 = multiply(dataMatrix[x], dataMatrix[x]) * multiply(v, v)
            # finish intersection items
            interaction = sum(multiply(inter_1, inter_1) - inter_2) / 2.

            p = w_0 + dataMatrix[x] * w + interaction

            loss = sigmoid(classLabels[x] * p[0, 0]) - 1
            print loss

            w_0 = w_0 - alpha * loss * classLabels[x]

            for i in xrange(n):
                if dataMatrix[x, i] != 0:
                    w[i, 0] = w[i, 0] - alpha * loss * classLabels[x] * dataMatrix[x, i]
                    for j in xrange[k]:
                        v[i, j] = v[i, j] - alpha * loss * classLabels[x] * (dataMatrix[x, i] * inter_1[0, j] - v[i, j] * dataMatrix[x, i] * dataMatrix[x, i])

    return w_0, w, v