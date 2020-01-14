import sys


def read_ans_data(ans_file):
  with open(ans_file) as file:
    data = []
    for line in file: data.append(line.strip())
    file.close()
    return data


instance_id = read_ans_data(sys.argv[1])
result = read_ans_data(sys.argv[2])

assert len(instance_id) == len(result)

for i in range(0, len(instance_id)):
  print("%s,%s" % (instance_id[i], result[i]))

