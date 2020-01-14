#!/bin/sh

sh make_tree_feature.sh ../../data_with_id/test.txt leaf_test.txt
sh make_tree_feature.sh ../../data_with_id/validation.txt leaf_validation.txt
sh make_tree_feature.sh ../../data_with_id/train.txt leaf_train.txt

