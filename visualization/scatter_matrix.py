import numpy as np
x = np.linspace(-1, 1, 100)
y = x**2
plt.plot(x,y)
plt.show()

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
