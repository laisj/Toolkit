# coding: utf-8
import lightgbm as lgb
import os.path
import sys


train_file = "../../data/train.txt"
validation_file = "../../data/validation.txt"
test_file = "../../data/test.txt"


model_path = "model.txt"

gbm = lgb.Booster(model_file=model_path)

predict = gbm.predict(validation_file, pred_leaf=True, num_iteration=gbm.best_iteration)

print(predict.shape)

for line in predict:
  print(line)

