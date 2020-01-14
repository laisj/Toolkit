#!/bin/sh

flight=opt1

check_exist()
{
    while [ 1 == 1 ]
    do
        hadoop --cluster c3prc-hadoop fs -test -e $1
        if [ $? == 0 ]
        then
            echo "File "$1" exist, continue"
            break
        fi
        echo "File "$1" does not exist, wait"
        sleep 60
    done
}

collectFeaPath=/user/h_miui_ad/dev/wwxu/exp/tf/collectedFeas/${flight}_dw
check_exist $collectFeaPath

rm -rf ${flight}_dw

hadoop --cluster c3prc-hadoop fs -get $collectFeaPath

./idsDW.py $flight

hadoop --cluster c3prc-hadoop fs -put -f fea2id_${flight}_d /user/h_miui_ad/dev/wwxu/exp/tf/
hadoop --cluster c3prc-hadoop fs -put -f fea2id_${flight}_w /user/h_miui_ad/dev/wwxu/exp/tf/
hadoop --cluster c3prc-hadoop fs -put -f conf_${flight}_dw /user/h_miui_ad/dev/wwxu/exp/tf/
