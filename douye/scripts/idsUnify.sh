#!/bin/sh

flight=opt3
fromModel=120
startIdx=1

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

if [ $fromModel == 0 ]
then
    collectFeaPath=/user/h_miui_ad/dev/wwxu/exp/cts/filterUselessFeas/$flight
    check_exist $collectFeaPath

    rm -rf $flight

    hadoop --cluster c3prc-hadoop fs -get $collectFeaPath
fi

./idsUnify.py $fromModel $flight $startIdx

hadoop --cluster c3prc-hadoop fs -put -f fea2id_${flight}_$fromModel /user/h_miui_ad/dev/wwxu/exp/tf/2/
hadoop --cluster c3prc-hadoop fs -put -f conf_${flight}_$fromModel /user/h_miui_ad/dev/wwxu/exp/tf/2/
