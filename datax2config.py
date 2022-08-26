import requests
import json
from utils import print_run_time,_write_html,_read_html
import pandas as pd
import os
BASEDIR = os.path.abspath(os.path.dirname(__file__))

TIME_OUT = 40
GD_DF = pd.read_csv("tables/gd_table.csv")
HN_DF = pd.read_csv("tables/hn_table.csv")
HEADERS = _read_html("source_headers.hd")
HEADERS = json.loads(HEADERS)
TZ_DF = pd.read_excel("/Users/mengyang/opt/pycpro/dcl/bi/doc/数据台账_v1.1.xlsx",sheet_name='stg层到ods层字段级别映射文档')
print(HEADERS)

def request_get_count(url, count=3,params=None,headers=None,**kwargs):
    index = 0
    res = None
    while True:
        try:
            res = requests.get(url,headers=headers,**kwargs)
            if res.status_code == 200:break
        except Exception as e:
            print("err",e)
        index += 1
        if index >=count:break
    return res

def request_post_count(url, count=3,data=None,headers=None,**kwargs):
    index = 0
    res = None
    while True:
        try:
            res = requests.post(url,json=data,headers=headers,**kwargs)
            if res.status_code == 200:break
        except Exception as e:
            print("err",e)
        index += 1
        if index >=count:break
    return res


@print_run_time
def get_job_jdbc_datasource(current=1,size=200):
    url = "http://10.106.14.1:1521/api/jobJdbcDatasource?current={}&size={}&ascs=datasource_name".format(current,size)
    headers = HEADERS
    headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"
    headers["keep_alive"] = 'False'

    #res = requests.get(url,headers=headers,timeout=TIME_OUT)
    res = request_get_count(url,headers=headers,timeout=TIME_OUT)
    if not res:return
    print("get_job_jdbc_datasource",res.status_code)
    if res.status_code == 200:
        return res.json()
@print_run_time
def get_tables_by_id(id):
    url = "http://10.106.14.1:1521/api/metadata/getTables?datasourceId={}".format(id)
    headers = HEADERS
    headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"
    headers["keep_alive"] = 'False'


    #res = requests.get(url,headers=headers,timeout=TIME_OUT)
    res = request_get_count(url,headers=headers,timeout=TIME_OUT)
    if not res:return

    print("get_tables_by_id",res.status_code)
    if res.status_code == 200:
        return res.json()
@print_run_time
def is_table_by_id_table_name(id,table_name):
    params = {
        "datasourceId":id,
        "tableName":table_name
    }
    "http://10.106.14.1:1521/api/metadata/getColumns?datasourceId=28&tableName=order_goods_return"
    url = "http://10.106.14.1:1521/api/metadata/getTables"
    headers = HEADERS
    headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"
    headers["keep_alive"] = 'False'


    #res = requests.get(url,params=params,headers=headers,timeout=TIME_OUT)
    res = request_get_count(url,headers=headers,timeout=TIME_OUT)
    if not res:return
    print("is_table_by_id_table_name",res.status_code)
    flag = False
    if res.status_code == 200:
        if res.json().get("code",None) == 0:
            flag = True
    return flag

@print_run_time
def get_table_columns(datasourceId,tableName):
    url = "http://10.106.14.1:1521/api/metadata/getColumns?datasourceId={}&tableName={}".format(datasourceId,tableName)
    headers = HEADERS
    headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"
    headers["keep_alive"] = 'False'


    #res = requests.get(url,headers=headers,timeout=TIME_OUT)
    res = request_get_count(url,headers=headers,timeout=TIME_OUT)
    if not res:return
    print("get_table_columns",res.status_code)
    if res.status_code == 200:
        return res.json()
@print_run_time
def build_json(data):
    #{"readerDatasourceId":28,"readerTables":["hospital_prescription_draft"],"readerColumns":["id","code","name","specification","number","usageId","usageName","frequencyId","frequencyName","frequencyRemark","perNumber","perUnit","antiComments","days"],"writerDatasourceId":18,"writerTables":["ods_hlwyy_hospital_prescription_draft"],"writerColumns":["0:id:bigint","1:code:string","2:name:string","3:specification:string","4:number:int","5:usageid:string","6:usagename:string","7:frequencyid:string","8:frequencyname:string","9:frequencyremark:string","10:pernumber:double","11:perunit:string","12:anticomments:string","13:days:int","14:sjly:string","15:etl_date:string"],"hiveReader":{},"hiveWriter":{"writerDefaultFS":"hdfs://TcBI-Cluster-HA","writerFileType":"orc","writerPath":"/user/hive/warehouse/ods.db/ods_hospital_prescription_draft","writerFileName":"ods_hospital_prescription_draft","writeMode":"append","writeFieldDelimiter":"\\u0001"},"rdbmsReader":{"readerSplitPk":"","whereParams":"","querySql":""},"rdbmsWriter":{},"hbaseReader":{},"hbaseWriter":{},"mongoDBReader":{},"mongoDBWriter":{}}
    url = "http://10.106.14.1:1521/api/dataxJson/buildJson"
    headers = HEADERS
    headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"
    headers["Content-Type"] = "application/json;charset=UTF-8"
    headers["keep_alive"] = 'False'



    #res = requests.post(url,json=data,headers=headers,timeout=TIME_OUT)
    res = request_post_count(url,data=data,headers=headers,timeout=TIME_OUT)
    print("build_json",res.status_code)
    if res.status_code == 200:
        return res.json()

def get_source_data(source_name,sources):
    for source_data in sources:
        datasourceName = source_data.get("datasourceName")
        if source_name == datasourceName:
            return source_data

def check_source(data_config):
    if not data_config:return

    f_source = data_config.get("f_source")
    t_source = data_config.get("t_source")

    source_res = get_job_jdbc_datasource()
    if source_res.get("code",None) != 0:
        print("check_source","获取不到数据源接口数据")
        return
    sources = source_res.get("data", {}).get("records", [])
    f_source_data = get_source_data(f_source, sources)
    t_source_data = get_source_data(t_source,sources)

    if not f_source_data or not t_source_data:
        if not f_source_data:
            print("chack_config", f_source, '错误')
        if not t_source_data:
            print("chack_config", t_source, '错误')
        return
    data_config['f_source_data'] = f_source_data
    data_config['t_source_data'] = t_source_data
    data_config['f_id'] = f_source_data.get("id")
    data_config['t_id'] = t_source_data.get("id")
    return data_config


def check_table_by_id(table_name,source_id):
    table_res = get_tables_by_id(source_id)
    if table_res.get("code",None) != 0:
        print("check_table_by_id","取不到 source{},table {} 对应的表信息".format(source_id,table_name))
        return
    tables = table_res.get("data")
    print("check_table_by_id {}".format(tables))
    if table_name not in tables:
        print("check_table_by_id","source{},table {} 不在数据源表清单中".format(source_id,table_name))
        return
    return table_name

def check_table_by_id_table_name(table_name,source_id):
    flag = is_table_by_id_table_name(source_id,table_name)
    if not flag:
        print("check_table_by_id_table_name","取不到{},{}对应的表信息".format(source_id,table_name))
        return
    return table_name

def check_table(data_config):
    if not data_config:return
    f_table = data_config.get("f_table")
    t_table = data_config.get("t_table")
    f_id = data_config.get("f_id")
    t_id = data_config.get("t_id")

    f_table = check_table_by_id(f_table, f_id)
    t_table = check_table_by_id(t_table, t_id)
    if not f_table or not t_table:
        return
    return data_config
def check_table_columns_by_id(source_id,table_name):
    if not source_id or not table_name:return
    res = get_table_columns(source_id,table_name)
    if res.get("code", None) != 0:
        print("check_table_columns_by_id","无法获得表的字段信息",source_id,table_name)
        return
    data = res.get("data")
    return data

def check_table_columns(data_config):
    if not data_config:return
    f_table = data_config.get("f_table")
    t_table = data_config.get("t_table")
    f_id = data_config.get("f_id")
    t_id = data_config.get("t_id")

    f_columns = check_table_columns_by_id(f_id,f_table)
    t_columns = check_table_columns_by_id(t_id,t_table)
    if not f_columns or not t_columns:
        return
    data_config["f_columns"] = f_columns
    data_config["t_columns"] = t_columns

    return data_config

def get_query_sql(columns,table_name):
    sql_headers = "select "
    sql_body = ','.join(columns)
    sql_end = " from {}".format(table_name)
    return sql_headers + sql_body + sql_end

def get_tz_query_sql(f_columns,table_name):
    stg_table_name = 'stg_' + table_name
    df = TZ_DF[TZ_DF["源表英文名称*"] == stg_table_name]
    if df.empty:return
    datas = df.to_dict(orient='records')
    data_html = ""
    new_f_columns = [columns.lower() for columns in f_columns[:-2]]
    print(new_f_columns)
    for data in datas:
        std_key= data["源字段英文名称*"]
        ods_key= data["目标字段英文名称"]
        if std_key in new_f_columns:
            data_str = "{std_key} as {ods_key}".format(std_key=std_key,ods_key=ods_key)
            data_html += data_str + ','
    source_html = f_columns[-2] + 'as sjly'
    time_html = f_columns[-1] + 'as etl_date'

    sql_html = "select {data_html} {source_html},{time_html} from {table_name};".format(data_html=data_html,source_html=source_html,time_html=time_html,table_name=table_name)
    return sql_html
def get_select_columns(f_columns,t_columns):
    print("f_columns",f_columns)
    print("t_columns",t_columns)
    t_columns = [col.split(':')[1] for col in t_columns]
    select_columns = []
    if len(t_columns) > len(f_columns):
        pass
    return select_columns


def check_add_table_columns(data_config):
    if not data_config:return
    f_columns = data_config.get("f_columns")
    t_columns = data_config.get("t_columns")
    #f_columns =get_select_columns(f_columns,t_columns)

    f_table = data_config.get("f_table")
    f_source = data_config.get("f_source")

    datasourceGroup = data_config.get("f_source_data",{}).get("datasourceGroup")
    if datasourceGroup == '互联网医院' and isinstance(f_columns,list):
        f_columns.extend(["'{source}_{table}'".format(source=f_source.split('_')[0],table=f_table),"replace( DATE_SUB(CURRENT_DATE,INTERVAL 1 day ),'-','')"])
        data_config["f_columns"] = f_columns
    data_config["f_sql"] = get_tz_query_sql(f_columns,f_table)
    return data_config


def check_hive(data_config):
    if not data_config:return
    t_datasource = data_config.get("t_source_data",{}).get("datasource")
    datasourceName = data_config.get("t_source_data",{}).get("datasourceName")
    print("check_hive",t_datasource,datasourceName)

    layer = datasourceName.split('_')[-1].lower()
    t_table = data_config.get("t_table")
    print("check_hive",layer,t_table)

    if t_datasource == 'hive':
        t_hive = {
            "writerDefaultFS":"hdfs://TcBI-Cluster-HA",
            "writerFileType":"orc",
            "writerPath":"/user/hive/warehouse/{}.db/{}".format(layer,t_table),
            "writerFileName":"{}".format(t_table),
            "writeMode":"append",
            "writeFieldDelimiter":"\u0001"
        }
        data_config["t_hive"] = t_hive
        print("t_hive",t_hive)
    return data_config
def check_build_params(data_config):
    if not data_config:return
    build_params = {}
    build_params["readerDatasourceId"] = data_config.get("f_id")
    build_params["readerTables"] = [data_config.get("f_table")]
    build_params["readerColumns"] = data_config.get("f_columns")

    build_params["writerDatasourceId"] = data_config.get("t_id")
    build_params["writerTables"] = [data_config.get("t_table")]
    build_params["writerColumns"] = data_config.get("t_columns")
    build_params["hiveWriter"] = data_config.get("t_hive",{})
    build_params["rdbmsReader"] = {
        "readerSplitPk":"",
        "whereParams":"",
        "querySql": data_config.get("f_sql")}
    data_config["build_params"] = build_params
    return data_config



def check_config(data_config):
    func_list = [check_source,check_table,check_table_columns,check_add_table_columns,
                 check_hive,check_build_params]
    for func in func_list:
        data_config = func(data_config)
    return data_config

def datax_decimal(datax_data):
    if not datax_data:return
    column = datax_data['job']['content'][0]['writer']['parameter']["column"]
    new_column = []
    for col in column:
        data = {}
        data.update(col)
        type = col.get("type")
        if type == 'decimal':
            data['type'] = 'string'
        new_column.append(data)
    datax_data['job']['content'][0]['writer']['parameter']["column"] = new_column
def update_datax_paramete(datax_data):
    datax_decimal(datax_data)
    hadoopConfig = {
              "dfs.nameservices": "TcBI-Cluster-HA",
              "dfs.ha.namenodes.TcBI-Cluster-HA": "namenode1,namenode2",
              "dfs.namenode.rpc-address.TcBI-Cluster-HA.namenode1": "spgz017:8020",
              "dfs.namenode.rpc-address.TcBI-Cluster-HA.namenode2": "spgz018:8020",
              "dfs.client.failover.proxy.provider.TcBI-Cluster-HA": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        }
    datax_data['job']['content'][0]['writer']["parameter"]['hadoopConfig'] = hadoopConfig
    datax_data['job']['content'][0]['reader']["parameter"]['username'] = 'bi_dept'
    datax_data['job']['content'][0]['reader']["parameter"]['password'] = '4aebmuDZbpLjBEEy'
    return datax_data

def get_build_params(data_config=None):
    data_config = check_config(data_config)
    if not data_config:return
    print("get_build_params",json.dumps(data_config,ensure_ascii=False))

    build_params = data_config.get("build_params")
    print("get_build_params",json.dumps(build_params,ensure_ascii=False))
    res = build_json(data=build_params)
    if not res or res.get("code") != 0:
        return
    data = res.get("data")
    #print("data",data)
    data = update_datax_paramete(json.loads(data))
    return data

def get_db_table_by_table_name(table_name,source):
    table_list = None
    if source == '广东互联网医院':
        table_df = GD_DF[GD_DF['TABLE_NAME']==table_name]
        if not table_df.empty:
            table_list = table_df.to_dict(orient='records')
    elif source == '海南互联网医院':
        table_df = GD_DF[GD_DF['TABLE_NAME']==table_name]
        if not table_df.empty:
            table_list = table_df.to_dict(orient='records')
    else:
        pass
    if table_list:
        return table_list[0]

def mk_ipath_dir(ipath_dir):
    if not os.path.exists(ipath_dir):
        os.makedirs(ipath_dir)
def get_table_datax_config(table_name):
    to_table_name = 'ods_hlwyy_' + table_name
    source_list = ["广东互联网医院","海南互联网医院"]
    base_dir = BASEDIR + '/json/datax/{}/'.format(table_name)
    print(base_dir)
    mk_ipath_dir(base_dir)

    for source in source_list:
        table_dict = get_db_table_by_table_name(table_name,source)
        if not table_dict:
            print("get_table_datax_config {} 无对应库".format(table_name))
            continue
        db_name = table_dict.get("TABLE_SCHEMA")
        if db_name and db_name != 'hospitalplus_pro':
            source += '_' + db_name

        data_config = {
            "f_source": source,
            "f_table": table_name,
            "t_source": "BDP_ODS",
            "t_table": to_table_name
        }
        print(data_config)
        res = get_build_params(data_config)
        if res:
            print('table_name',table_name)
            _write_html(base_dir + '{}.json'.format(source),json.dumps(res,ensure_ascii=False,indent=4))

def run_one():
    #hospitalplus_third_channel
    table = "hospitalplus_store"
    get_table_datax_config(table_name=table)

def get_task_datax(task_id):
    ipath = 'json/task/{}.json'.format(str(task_id))
    config_html = _read_html(ipath)
    config_obj = json.loads(config_html)
    for config in config_obj:
        table_name = config.get("table_name")
        print('table',table_name)
        get_table_datax_config(table_name)

if __name__ == '__main__':
    #medicine_db
    #task_id = '3955165257099'
    #get_task_datax(task_id)

    run_one()