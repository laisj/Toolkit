# 暂时没有定向
# 时间，slot，default，长度，cpm长度，cpc长度，一价cpm比例，二价cpm比例，一价ecpm，二价ecpm，一价cpm的时候的ecpm，一价cpc的时候的ecpm。

zcat /home/adengine/bids-*2016-03-02-23*|awk -F"[|]|\"feeType\":\"|\"qScore\":|,\"adId\"|\",\"eCPM\":|},{|}]" '{if(substr($2,length($2)-1,2)>"50"){print $(NF-1),substr($1,0,13),$5,$7,$9,$10,$12,$14,$15,$17,$19,$20,$22,$24,$25,$27,$29,$30}}'|grep -v 4-3|

zcat /home/adengine/bids-*2016-03*|awk -F"[|]|\"feeType\":\"|\"qScore\":|,\"adId\"|\",\"eCPM\":|},{|}]" '{if(substr($2,length($2)-1,2)>"50"){print $(NF-1),substr($1,0,13),$5,$7,$9,$10,$12,$14,$15,$17,$19,$20,$22,$24,$25,$27,$29,$30}}'|grep -v 4-3|awk '{a[$2" "$3" "$4]+=$1;b[$2" "$3" "$4]+=1}END{for(i in a){print i,a[i]/b[i]}}'|sort > jingjialenm.tsv &

zcat /home/adengine/bids-*2016-03*|awk -F"[|]|\"feeType\":\"|\"qScore\":|,\"adId\"|\",\"eCPM\":|},{|}]" '{if(substr($2,length($2)-1,2)>"50"){print $(NF-1),substr($1,0,13),$5,$7,$9,$10,$12,$14,$15,$17,$19,$20,$22,$24,$25,$27,$29,$30}}'|grep -v 4-3|awk '{a[$2" "$3" "$4]+=$7;b[$2" "$3" "$4]+=1}END{for(i in a){print i,a[i]/b[i]}}'|sort > jingjia1m.tsv &

zcat /home/adengine/bids-*2016-03*|awk -F"[|]|\"feeType\":\"|\"qScore\":|,\"adId\"|\",\"eCPM\":|},{|}]" '{if(substr($2,length($2)-1,2)>"50"){print $(NF-1),substr($1,0,13),$5,$7,$9,$10,$12,$14,$15,$17,$19,$20,$22,$24,$25,$27,$29,$30}}'|grep -v 4-3|awk '{a[$2" "$3" "$4]+=$10;b[$2" "$3" "$4]+=1}END{for(i in a){print i,a[i]/b[i]}}'|sort > jingjia2m.tsv &

zcat /home/adengine/bids-*2016-03*|awk -F"[|]|\"feeType\":\"|\"qScore\":|,\"adId\"|\",\"eCPM\":|},{|}]" '{if(substr($2,length($2)-1,2)>"50"){print $(NF-1),substr($1,0,13),$5,$7,$9,$10,$12,$14,$15,$17,$19,$20,$22,$24,$25,$27,$29,$30}}'|grep -v 4-3|awk '{if($6=="CPC"){a[$2" "$3" "$4]+=1};b[$2" "$3" "$4]+=1}END{for(i in a){print i,a[i],b[i]}}'|sort > jingjia1cpcm.tsv &

zcat /home/adengine/bids-*2016-03*|awk -F"[|]|\"feeType\":\"|\"qScore\":|,\"adId\"|\",\"eCPM\":|},{|}]" '{if(substr($2,length($2)-1,2)>"50"){print $(NF-1),substr($1,0,13),$5,$7,$9,$10,$12,$14,$15,$17,$19,$20,$22,$24,$25,$27,$29,$30}}'|grep -v 4-3|awk '{if($9=="CPC"){a[$2" "$3" "$4]+=1};b[$2" "$3" "$4]+=1}END{for(i in a){print i,a[i],b[i]}}'|sort > jingjia2cpcm.tsv &
