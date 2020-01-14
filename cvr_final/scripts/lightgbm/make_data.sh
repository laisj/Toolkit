#!/bin/sh

dir=lib_svm_samples_with_id

cat ${dir}/train/part-* | awk -F "#" '{print $1> "./data/train.txt"; print $2> "./data/train_id.txt";}'
cat ${dir}/validation/part-* | awk -F "#" '{print $1> "./data/validation.txt"; print $2> "./data/validation_id.txt";}'
cat ${dir}/test/part-* | awk -F "#" '{print $1> "./data/test.txt"; print $2> "./data/test_id.txt";}'
