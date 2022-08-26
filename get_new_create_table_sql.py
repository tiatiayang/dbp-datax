import sys
from utils import _read_html,_write_html


def run(table_name):

    ipath = "logs/{}.log".format(table_name)
    html_end = '''row format delimited fields terminated by '\\001'
collection items terminated by '\\002'
map keys terminated by '\\003'
lines terminated by '\\n'
stored as orc;'''
    html = _read_html(ipath)
    html = html.replace("+","").replace("-","").replace("|","").replace("createtab_stmt","")
    html = html.split("ROW FORMAT")[0]
    html = html.replace("\t",'').replace("  ",'').replace("`","").replace("\n\n\n","")
    html += html_end
    print(html)
    _write_html("logs/{}.sql".format(table),html)

if __name__ == '__main__':
    #table = sys.argv[1]
    table = 'ownership_agent_team_tmp'
    run(table)
