import sys


input = sys.argv[1]
feature = sys.argv[2]
instance_id = sys.argv[3]

feature_file = open(feature, 'wb')
instance_id_file = open(instance_id, 'wb')

with open(input) as file:
  for line in file:
    item = line.strip().split('#')
    feature_file.write('%s\n' % item[0])
    instance_id_file.write('%s\n' % item[1])
  file.close()
  feature_file.close()
  instance_id_file.close()

