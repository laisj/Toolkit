#!/bin/sh

data=$1
output=$2

python split.py ${data} ./feature.txt ./instance_id.txt

lightgbm task=predict data=feature.txt input_model=model.txt predict_leaf_index=true num_iteration_predict=1000 \
    output_result=result.txt

python merge.py instance_id.txt result.txt > ${output}

