import sys
import redis
#hadoop fs -text training_pipeline/gbdt_lr/gbdt_ftrl_20170802114148/model_str/*|python model_load_prod.py gbdt_ftrl_20170802114148
modelNameOut = sys.argv[1]
print modelNameOut
r = redis.Redis(host="test001", port=6303)
# input comes from STDIN (standard input)
for line in sys.stdin:
    line_pair = line.strip().split("|")
    k = line_pair[0]
    v = line_pair[1]
    r.hset("model:" + modelNameOut + ":info", k, v)
r.sadd("ftrlModels", modelNameOut)
r.publish("reloadLrModel", "1")