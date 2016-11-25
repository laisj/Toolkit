import numpy as np
import matplotlib.pyplot as plt
import sklearn
from sklearn import *

# Generate a dataset and plot it
np.random.seed(0)
X, y = datasets.make_moons(200, noise=0.20)
plt.scatter(X[:,0], X[:,1], s=40, c=y, cmap=plt.cm.Spectral)
plt.savefig('./foo.png')
# Train the logistic rgeression classifier
clf = linear_model.LogisticRegressionCV()
clf.fit(X, y)
z = clf.predict(X)

for xx,yy,zz in zip(X,y,z):
    print xx, yy, zz
# Plot the decision boundary
#plot_decision_boundary(lambda x: clf.predict(x))
#plt.title("Logistic Regression")
