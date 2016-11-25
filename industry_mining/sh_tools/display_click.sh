# 计算cache命中率
zcat display*2016-03-01*|grep default|wc -l
zcat display*2016-03-01*|grep default|egrep "\|0.015\|"|wc -l

# 计算点击率
zcat display*2016-03-01*|grep default|grep 2-4|wc -l;zcat click*2016-03-01*|grep default|grep 2-4|wc -l
echo $(zcat display*2016-03-01*|grep default|grep 2-4|wc -l) $(zcat click*2016-03-01*|grep default|grep 2-4|wc -l)|awk '{print $2*1./$1}'

# 曝光和点击时的竞价情况
zcat click-*2016-02-18-20*|grep default|grep 1-3|awk -F"|" 'BEGIN{OFS="\t"}{if($13==1){print substr($1, 12, 2), $7, $13, $21, $7*(1-$13) + $7*$21*$13}}'|less
zcat display-*2016-02-18-20*|grep default|grep 1-3|awk -F"|" 'BEGIN{OFS="\t"}{if($13==0){print substr($1, 12, 2), $7, $13, $21, $7*(1-$13) + $7*$21*$13}}'|less

# 大广告主曝光点击
zcat display-*2016-02-02-*|awk -F"|" '{if($2 ~ /335118576/){print $0}}'|wc -l
2892929
zcat click-*2016-02-02-*|awk -F"|" '{if($2 ~ /335118576/){print $0}}'|wc -l
85226
zcat click-*2016-02-02-*|awk -F"|" '{if($2 ~ /318345052/){print $0}}'|awk -F":" '{print $1}' |uniq -c

# 分小时处理文件
find /home/adengine/ -name "click*2016-03-03*"|xargs -I {} sh -c "zcat {}|python echodir.py"
find . -name "clickuser01*"|xargs -I {} sh -c "echo {};cat {}|python echodir.py"

# 每小时default上的不同位置的点击率
ls /home/adengine/click*2016-03-03-1*|sort|xargs -I {} sh -c "zcat {}|awk -F'|' '{print substr(\$1,0,13),\$11,\$13}'|sort|uniq -c|awk 'BEGIN{OFS=\"\t\"}{print \$1, \$2, \$3, \$4, \$5}'"

ls /home/adengine/click*2016-03-0*|sort|xargs -I {} sh -c "zcat {}|grep default|awk -F'|' '{print substr(\$1,0,13),\$11,\$13}'|sort|uniq -c|awk 'BEGIN{OFS=\"\t\"}{print \$1, \$2, \$3, \$4, \$5}'" > default_slot_hour_click.tsv

cat default_slot_hour_display.tsv|awk 'BEGIN{OFS="\t"}{a[$2" "$3" "$4" "$5]+=1; b[$2" "$3" "$4" "$5]+=$1}END{for(i in a){print i, a[i], b[i]}}'|sort > slot_hour_display.tsv

awk 'NR==FNR{a[$1" "$2" "$3" "$4]=$0;b[$1" "$2" "$3" "$4]=$6}NR>FNR{print a[$1" "$2" "$3" "$4]"\t"$6"\t"1.0*$6/b[$1" "$2" "$3" "$4]}' slot_hour_display.tsv slot_hour_click.tsv > slot_hour_ctr.tsv

# 小时，cpmcpc，slot的display，click，pctr
zcat /home/adengine/display-*2016-03-0*|grep 2-4|grep default|awk -F"|" '{a[substr($1,0,13)" "$11" "$13]+=1}END{for(i in a){print i,a[i]}}'|sort>default2-4display.tsv
zcat /home/adengine/click-*2016-03-0*|grep 2-4|grep default|awk -F"|" '{a[substr($1,0,13)" "$11" "$13]+=1}END{for(i in a){print i,a[i]}}'|sort>default2-4click.tsv
zcat /home/adengine/display-*2016-03-0*|grep 2-4|grep default|awk -F"|" '{a[substr($1,0,13)" "$11" "$13]+=1;b[substr($1,0,13)" "$11" "$13]+=$21}END{for(i in a){print i,a[i],b[i]/a[i]}}'|sort>default2-4pctr.tsv

awk 'NR==FNR{a[$1" "$2" "$3" "$4]+=$5}NR>FNR{print $0,a[$1" "$2" "$3" "$4],$5/a[$1" "$2" "$3" "$4]}' default1-3displaysort.tsv default1-3clicksort.tsv |sed 's/ /\t/g'

# 分位置头广
zcat /home/adengine/display-*2016-05-17-13*|grep 1-3|awk -F"|" '{print $2,$6,$14,$15}'|sort|uniq -c|sort -nr|head

ls /home/adengine/click-*2016-05-17-2*| xargs -I {} sh -c "echo {}; zcat {} |grep 1-3|awk -F'|' '{print \$2,\$6,\$14,\$15}'|sort|uniq -c|sort -nr|head"

echo "2-4"; ls /home/adengine/display-*2016-05-18*| xargs -I {} sh -c "echo {}; zcat {} |grep 2-4|awk -F'|' '{print \$2,\$6,\$14,\$15}'|sort|uniq -c|sort -nr|head";echo "1-3"; ls /home/adengine/display-*2016-05-18*| xargs -I {} sh -c "echo {}; zcat {} |grep 1-3|awk -F'|' '{print \$2,\$6,\$14,\$15}'|sort|uniq -c|sort -nr|head";echo "2-12"; ls /home/adengine/display-*2016-05-18*| xargs -I {} sh -c "echo {}; zcat {} |grep 2-12|awk -F'|' '{print \$2,\$6,\$14,\$15}'|sort|uniq -c|sort -nr|head";
echo "2-4"; ls /home/adengine/display-*2016-05-29*| xargs -I {} sh -c "echo {}; zcat {} |grep 2-4|grep exp4|awk -F'|' '{print \$2,\$6,\$14,\$15}'|sort|uniq -c|sort -nr|head";
