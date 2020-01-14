/*
*如果thrift有嵌套，最外层的struct放在最下面.
*/
namespace java com.xiaomi.contest.cvr.samples

enum FeaType {
  Continuous = 1,
  Categorical = 2
}

struct BaseFea {
  1: optional FeaType type;
  2: optional i32 group_id;
  3: optional string group_name;
  4: optional i64 id;
  5: optional i64 identifier;     // using i64 identifier a features, which is hash value of fea
  6: optional string fea;         // categorical features only fea
  7: optional double value;       // for categorical features, value always 1.0
  8: optional double weight;
}

struct Sample {
  1: optional i32 label;
  2: optional double predict_pro;
  3: optional i32 instance_id;
  4: optional i32 group_number;
  5: optional string time;
  6: optional list<BaseFea> features;
  7: optional list<BaseFea> lr_features;
  8: optional list<BaseFea> tree_features;
}
