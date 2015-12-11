#encoding=utf8
import web
import happybase
import collections
import operator

machine = 'l-algozoo1.f.dev.cn0.qunar.com'
table_name1 = 'inter_flt_line'
table_name2 = 'inter_flt_line_country'
col_famliy='u'
conn = happybase.Connection(host=machine, port=9090)
user_table1 = conn.table(table_name1)
user_table2 = conn.table(table_name2)
render = web.template.render('templates/')

urls = (
    '/', 'index'
)

class index:
    def GET(self):
        #return user_table.scan(row_prefix=name)
        i = web.input(start=None,end=None,type=None)
        if i.end == None:
            if i.type == "1":
                print [sorted(user_table2.scan(row_prefix=i.start.encode('utf8')), key=(lambda xx: int(xx[1]["u:rank_value"])), reverse=True)]
                return render.list(["\t".join([x.decode('utf8'), y["u:rank_value"]]) for x,y in sorted(user_table2.scan(row_prefix=i.start.encode('utf8')), key=(lambda xx: int(xx[1]["u:rank_value"])), reverse=True)])
            else:
                return render.list(["\t".join([x.decode('utf8'), y["u:rank_value"]]) for x,y in sorted(user_table1.scan(row_prefix=i.start.encode('utf8')), key=(lambda xx: int(xx[1]["u:rank_value"])), reverse=True)])
        else:
            return render.index(user_table1.row((i.start + "|" + i.end).encode('utf8')))

if __name__ == "__main__":

    app = web.application(urls, globals())
    app.run()