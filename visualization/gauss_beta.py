import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
#from matplotlib import cm

from mpl_toolkits.mplot3d import Axes3D

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')

n = np.arange(100, 2000, 100)
#n = np.arange(1000, 20000, 1000)
p = np.arange(0.001, 0.05, 0.001)
n, p = np.meshgrid(n, p)
#aa = 1.0 * a / (a + b)
#bb = 1.0 * a * b / (1.0 * (a + b) * (a + b) * (a + b + 1))
#print bb

#x = np.arange(0.0001, 1, 0.0001)
#y = stats.beta.cdf(x, a, b)

#a = n
#b = n * p
a = n / p
b = n
zz = stats.beta.ppf(0.025, b, a-b)/p
#ax.plot_surface(n,p,zz)
#ax.plot_wireframe(n,p,zz)
ax.plot_surface(n,p,zz, rstride=1, cstride=1, linewidth=0, antialiased=False)
#print stats.beta.ppf(0.975, a, b)
plt.show()
'''
#z = stats.norm.cdf(x, aa, bb)
plt.plot3(a,b,z,color='b')
#plt.plot(x,z,color='r')
plt.title('Gauss and Beta: a=%.1f, b=%.1f' % (a,b))
plt.xlabel('a')
plt.ylabel('b')
plt.zlabel('b')
plt.show()
'''