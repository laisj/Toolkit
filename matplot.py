import pylab
import numpy as np
from pandas.tools.plotting import scatter_matrix
import pandas as pd

df = pd.DataFrame(np.random.randn(1000, 4), columns=['a', 'b', 'c', 'd'])
print np.random.randn(10, 4)
scatter_matrix(df, alpha=0.5, figsize=(8, 8), diagonal='kde')

def make_scatter_plot(X, name):    
    """
    Make scatterplot.

    Parameters:
    -----------
    X:a design matrix where each column is a feature and each row is an observation.
    name: the name of the plot.
    """
    pylab.clf()
    df = pd.DataFrame(X)
    axs = scatter_matrix(df, alpha=0.2, diagonal='kde')

    for ax in axs[:,0]: # the left boundary
        ax.grid('off', axis='both')
        ax.set_yticks([0, .5])

    for ax in axs[-1,:]: # the lower boundary
        ax.grid('off', axis='both')
        ax.set_xticks([0, .5])

    pylab.savefig(name + ".png")
