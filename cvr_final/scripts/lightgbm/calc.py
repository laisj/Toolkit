import sys
import math


def read_ans_data(ans_file):
  with open(ans_file) as file:
    data = []
    for line in file: data.append(line.strip())
    file.close()
    return data


user = read_ans_data(sys.argv[1])
ovd_rate = read_ans_data(sys.argv[2])

assert len(user) == len(ovd_rate)

ans = []
for i in range(0, len(user)):
  ans.append("%s,%.15f" % (user[i], float(ovd_rate[i])))

print("instance_id,proba")
for line in sorted(ans):
  print("%s" % (line))
