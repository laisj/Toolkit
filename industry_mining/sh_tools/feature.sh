cat *|grep -E --color "Display_7days\"\:[1-9]{1,3}\.0"|grep Fee|awk -F"|" '{print substr($2,length($2)-1,length($2))}'|sort|uniq -c
cat *|grep [^0-9a-zA-Z[:space:][:punct:]]
