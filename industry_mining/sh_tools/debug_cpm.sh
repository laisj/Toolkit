顺序：
cpm，1-3与2-4cpm，cpm，cpc，gd的cpm,自定义报表，业务端mysql。

［cpm，曝光，总消耗］＊［昨日，今日］
 [1-3,2-4,2-12]＊[cpm,曝光，竞价队列长度] ＊［昨日，今日］
 ［4-3］＊［cpm，曝光，消耗］＊［昨日，今日］
 ［昨日top20广告主,消耗占比80%左右］＊［cpm，曝光，消耗，campaign预算调整，开关campaign，上下线广告，bid调整］
 ［今日top20广告主,消耗占比80%左右］＊［cpm，曝光，消耗，campaign预算调整，开关campaign，上下线广告，bid调整］
 (cpm_x - cpm_total) * display_x/display_total

