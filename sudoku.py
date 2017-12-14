import itertools
a = [[0]*5]*5

def s(i,j):
    return a[i][j]+a[i+1][j]+a[i][j+1]+a[i+1][j+1]


import types

def tramp(gen, *args, **kwargs):
    g = gen(*args, **kwargs)
    while isinstance(g, types.GeneratorType):
        g=g.next()
    return g

# constraint s(0,0) == 15, s(2,1) = 9, s(0,3) = 10, s(1,1)=11,s(1,2)=12,s(2,2)=14,s(3,0)=11,s(3,3)=13

def allpailie(data, pre):
    if len(data) == 0:
        return [[pre]]
    else:
        result = []
        for i in data:
            data2 = [j for j in data if j != i]
            result.extend(allpailie(data2,pre + i))
        return result

print allpailie("12345", "")
