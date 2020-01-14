import numpy as np
import xgboost as xgb

### load data in do training
data_train = xgb.DMatrix('../../data/train.txt')
data_validation = xgb.DMatrix('../../data/validation.txt')
data_test = xgb.DMatrix('../../data/test.txt')

param = {
  'booster':'gbtree',
  'silent':0,
  'nthread':24,
  'eta':0.1,
  'gamma':1e-3,
  'max_depth':8,
  'min_child_weight':1,
  'objective':'binary:logistic',
  'eval_metric':'logloss',
  'eval_train':1,
  'early_stopping_rounds':10,
  'save_period':10
}

num_round = 20

# specify validations set to watch performance
watchlist = [(data_validation, 'eval'), (data_train, 'train')]
bst = xgb.train(param, data_train, num_round, watchlist)
bst.save_model('0001.model')

# make prediction
ans = bst.predict(data_test)

print ('start testing predict the leaf indices')
### predict using first 2 tree
leaf_index = bst.predict(data_train, ntree_limit=2, pred_leaf=True)
print(leaf_index.shape)
print(leaf_index)

### predict all trees
leaf_index = bst.predict(data_test, pred_leaf = True)
print(leaf_index.shape)

