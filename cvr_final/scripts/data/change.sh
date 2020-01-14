#!/usr/bin/env bash

hadoop fs -rmr lizhixu/recommend/training/conf/l1lr_torque_test.conf
hadoop fs -put ./conf/l1lr_torque_test.conf lizhixu/recommend/training/conf/
hadoop fs -cat lizhixu/recommend/training/conf/l1lr_torque_test.conf
