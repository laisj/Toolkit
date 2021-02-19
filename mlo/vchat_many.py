#coding=utf8
import itchat

itchat.auto_login(hotReload=True)
user_name_list = []
send_name_list = []
friends_list = itchat.get_friends(update=True)

all_friends = itchat.get_friends()
print all_friends[0]

for f in all_friends:
    print f["NickName"]
    print f["RemarkName"]

# assert(len(user_name_list) == len(send_name_list))
# for i, namestr in enumerate(user_name_list):
#     name = itchat.search_friends(name=namestr)
#     uid = name[0]["UserName"]
#     print uid
#     itchat.send(msg=u'过年好啊，' + send_name_list[i] + u'，年后看机会吗？' , toUserName=uid)