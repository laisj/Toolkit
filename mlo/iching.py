import random
import sys
import os

a1 = raw_input()
a2 = raw_input()
random.seed(int(a1) * int(a2))
res = random.randint(0, 383)
gua = res / 6 + 1
yao = res % 6 + 1
print "%d gua %d yao" % (gua, yao)
