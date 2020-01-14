# coding: utf-8
import lightgbm as lgb
import os.path
import sys


def load_data(data_file, data_bin_file):
  if os.path.isfile(data_bin_file):
    lgb_data = lgb.Dataset(data_bin_file)
  else:
    lgb_data = lgb.Dataset(data_file)
    lgb_data.save_binary(data_bin_file)
  return lgb_data


# load or create your dataset
print('Start to load data')
sys.stdout.flush()

train_file = "../../data/train.txt"
validation_file = "../../data/validation.txt"
test_file = "../../data/test.txt"

train_bin_file = "../../data/train.txt.bin"
validation_bin_file = "../../data/validation.txt.bin"
test_bin_file = "../../data/test.txt.bin"

# lgb_train = load_data(train_file, train_bin_file)
lgb_train = lgb.Dataset(train_file)
lgb_validation = lgb.Dataset(validation_file, reference=lgb_train)


#print('train', lgb_train.num_data(), lgb_train.num_feature())
#print('validation', lgb_validation.num_data(), lgb_validation.num_feature())

# specify your configurations as a dict
params = {
  'task': 'train',
  'boosting_type': 'gbdt',
  'objective': 'binary',

  'metric': 'binary_logloss',
  'metric_freq': 5,
  'is_training_metric': 'true',

  'max_bin': 255,
  'bin_construct_sample_cnt': 20000000,
  'num_threads': 24,

  'learning_rate' : 0.1,
  'num_leaves': 127,

  'max_depth': -1,
  'min_data_in_leaf': 1,
  'min_sum_hessian_in_leaf': 1e-3,
  'feature_fraction': 1.0,
  'bagging_freq': 0,
  'bagging_fraction': 1.0,

  'lambda_l1': 0,
  'lambda_l2': 0,
  'min_gain_to_split': 0,
}

print('Start training')
sys.stdout.flush()
# train
gbm = lgb.train(params, lgb_train, num_boost_round=20, valid_sets=lgb_validation, early_stopping_rounds=100)

print('Save model')
sys.stdout.flush()
# save model to file
gbm.save_model('model.txt')

print('Start predicting')
sys.stdout.flush()
# predict
# predict = gbm.predict(lgb_test, pred_leaf=False, num_iteration=gbm.best_iteration)
predict = gbm.predict(lgb_train, pred_leaf=False, num_iteration=gbm.best_iteration)

print(predict.shape)
print(predict)
