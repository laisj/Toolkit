#!/bin/sh

data=../../data_with_id/test.txt
python split.py ${data} ./feature.txt ./instance_id.txt

lightgbm task=predict data=./feature.txt input_model=model.txt output_result=result.txt

python calc.py instance_id.txt result.txt > ans_test.txt
