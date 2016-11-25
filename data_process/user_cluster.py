# -*- coding: utf-8 -*-
"""
Created on Wed Jul 22 11:31:21 2015

@author: sijia.lai
"""

import seaborn as sns
import pandas as pd

data_column = ["total_count", "order_count", "self_rate", "xcd_rate","ticket_self_rate", "ticket_xcd_rate", "avg_price", "day_0_rate", "day_1_rate", "day_2_rate", "day_gt2_rate", "average_person_per_order", "dep_city_count", "arr_city_count", "line_count"]

df = pd.DataFrame([map(lambda x: float(x), line.strip().split("\t")[1:12]) for line in open(r"C:\Users\sijia.lai\PycharmProjects\untitled\f_sample.tsv", "r") if float(line.strip().split("\t")[1]) > 16][0:200], columns=data_column[0:11])

print df

g = sns.PairGrid(df)
g.map_diag(sns.kdeplot)
g.map_offdiag(sns.kdeplot, cmap="Blues_d", n_levels=20);

#sns.set(style="ticks", color_codes=True)
f = sns.pairplot(df)
#f.savefig("1.png", size=5)