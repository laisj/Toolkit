#import os
#import sys

import numpy as np

fun1 = lambda x:100*(x[0]**2-x[1])**2 + (x[0]-1)**2
#print fun([1,0])

gfun1 = lambda x:np.array([400*x[0]*(x[0]**2-x[1])+2*(x[0]-1), -200*(x[0]**2-x[1])])
#print gfun([1,0])
hess1 = lambda x:np.array([[1200*x[0]**2-400*x[1]+2, -400*x[0]],[-400*x[0],200]])
#print hess([1,0])

def gradient(fun,gfun,x0):
    maxk = 5000
    rho = 0.5
    sigma = 0.4
    k = 0
    epsilon = 1e-5

    while k<maxk:
        gk = gfun(x0)
        dk = -gk
        if np.linalg.norm(dk) < epsilon:
            break
        m = 0
        mk = 0
        while m<20:
            if fun(x0+rho**m*dk) < fun(x0) + sigma*rho**m*np.dot(gk,dk):
                mk = m
                break
            m += 1
        x0 += rho**mk*dk
        k += 1

    return x0,fun(x0),k

print gradient(fun1,gfun1,[0,0])