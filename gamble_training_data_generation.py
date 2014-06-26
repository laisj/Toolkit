# -*- coding: utf-8 -*-
"""
Created on Tue Jun 24 09:42:44 2014

@author: sijla
"""

import urllib2
import sys
import re
import time
import json

'''[0]比赛id [1]主队名字 [2]客队名字 [3]主队号 [4]客队号 [5]联赛名字'''
def GenGameName(rate_reg, num_reg, split_reg, result_reg, blank_reg, html_str):
    match_details = []
    
    teams =  rate_reg.findall(html_str)
    for i in xrange(0, len(teams) / 2):
        teams[2*i] = num_reg.sub("", teams[2*i])
        temp1 = split_reg.split(teams[2*i])
    
        teams[2*i+1] = num_reg.sub("", teams[2*i+1])
        temp2 = split_reg.split(teams[2*i+1])
        
        match_details.append([temp1[5],temp1[14],temp2[14],temp1[1],temp2[1],""])
    
    results =result_reg.findall(html_str)
    for i in xrange(0, len(results)):
        results[i] = num_reg.sub("", results[i])
        temp = split_reg.split(results[i])
        match_details[i][5] = blank_reg.sub("", temp[25])
        
    return match_details
    
'''[0]主队全场进球 [1]客队全场进球 [2]主队半场进球 [3]客队半场进球'''
def GenGameScore(score_reg, team_score_reg):
    

url_template = "http://odds.500.com/history.php?date={1}"
url = url_template.replace("{1}", sys.argv[1])
html_str = urllib2.urlopen(url).read()
#html_unicode_gb = html_str.decode('gbk', 'ignore')
rate_reg = re.compile("teamid=[0-9]{0,5}\"\s*id=\"[a-z]_[0-9]{6}\"\s*class=\"awhite\"\s*target=\"_blank\">.{1,32}<\/a>")
num_reg = re.compile("\[[0-9]{1,2}\]")
split_reg = re.compile("[=\"_<>]")
result_reg = re.compile("teamid=[0-9]{0,5}\"\s*id=\"[a-z]_[0-9]{6}\"\s*class=\"awhite\"\s*target=\"_blank\">.{1,32}<\/a>.{0,4}<span\s*id=\"[a-z]_[0-9]{6,7}\"\s*style=\"display:none;\">.*<\/span>")
blank_reg = re.compile("\s*")
score_reg = re.compile("font\s*color=\"[a-z]{3,5}\">[0-9]")
team_score_reg = re.compile("[<>]")

match_details = GenGameName(rate_reg, num_reg, split_reg, result_reg, blank_reg, html_str)

score_details = GenGameScore(score_reg, team_score_reg)

#step2: get game score
'''[0]主队全场进球 [1]客队全场进球 [2]主队半场进球 [3]客队半场进球'''

score_reg = re.compile("font\s*color=\"[a-z]{3,5}\">[0-9]")
team_score_reg = re.compile("[<>]")
scores = score_reg.findall(html_str)

score_details = []
aa = []

i=0
while 2 * i < len(scores):
    score1 = team_score_reg.split(scores[2*i])[1]
    score2 = team_score_reg.split(scores[2*i+1])[1]
    aa.append([score1,score2])
    i+=1
    
i=0
while 2 * i < len(aa):
    score_details.append([aa[2*i][0], aa[2*i][1], aa[2*i+1][0], aa[2*i+1][1]])
    i += 1

#step 3
'''[0]胜赔 [1]平赔 [2]负赔 [3]返还率0<x<100 [4]变动时间'''

#win, par, loss rate, money back rate, change time
if len(match_details) == len(score_details):
    i = 0
    while i < len(match_details):
        time.sleep(3)
        url_temp = 'http://odds.500.com/fenxi/json/ouzhi.php?fid=' + match_details[i][0] + '&cid=293&r=1&type=europe'
        html_temp_str = urllib2.urlopen(url_temp).read()
        temp_var = json.loads(html_temp_str)

        for temp_var_iter in temp_var:
            temp_var_iter.extend(match_details[i])
            temp_var_iter.extend(score_details[i])
        for v1 in temp_var:
            for v2 in v1:
                print str(v2).decode("gbk", "ignore")+"\t",
            print ""
        
        i += 1