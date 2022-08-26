# dbp-datax

为大数据平台生成datax配置文件

## 原理

> 利用 http://10.106.14.1:1521/index.html#/datax/job/jsonBuild 走完构建datax配置文件的流程。
>
> 最后生成标准，正确的datax配置文件。
>
> 目前的没有解决的问题：加密字段的配置，需要人工处理。

## 项目结构

```ini
├── README.md #项目文档
├── dag_login.py #大数据平台登录脚本，目前弃用
├── datax2config.py #生成datax配置文件
├── get_dag.py  #生成工作流的配置信息
├── get_new_create_table_sql.py
├── json #存放生成的配置信息
│   ├── dag
│   ├── datax #存放生成的datax 配置信息
│   └── task #存放生成的工作流配置信息
├── logs
├── source_headers.hd #Dashboard 网站的登录认证信息。
├── source_login.py  #Dashboard 网站登录
├── tables
│   ├── gd_table.csv #gd 互联网医院 所有表与表名
│   └── hn_table.csv #hn 互联网医院 所有表与表名
├── test_get_dag.py
└── utils.py
```

## 核心代码

配置代码

```python
#datax2config.py

TIME_OUT = 40 #请求的超时时间
RETRY_COUNT = 3 #请求重试次数
GD_DF = pd.read_csv("tables/gd_table.csv") #读取gd所有表名
HN_DF = pd.read_csv("tables/hn_table.csv") #读取hn所有表名
HEADERS = _read_html("source_headers.hd") #读取登录信息
HEADERS = json.loads(HEADERS)
TZ_DF = pd.read_excel("/Users/mengyang/opt/pycpro/dcl/bi/doc/数据台账_v1.1.xlsx",sheet_name='stg层到ods层字段级别映射文档') #读取台账信息，这个需要根据自己处理的工作流做调整。
```

执行代码

```python
def run_one(): #单个表执行
    table = "hospitalplus_store" #配置表名
    get_table_datax_config(table_name=table)

def get_task_datax(task_id): #整个工作流执行
    ipath = 'json/task/{}.json'.format(str(task_id)) #读取工作流的配置信息
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

    run_one()  #建议单个表执行，时间短，并获取最新配置信息
```

## 生成的文件

```ini
├── json #存放生成的配置信息
│   ├── dag
│   ├── datax #存放生成的datax 配置信息
│   │   ├── hospitalplus_virtual_commentvb #基于表名生成文件夹，这里广东，海南都有这张表故生成两个文件。
│   │   │   ├── 广东互联网医院.json
│   │   │   └── 海南互联网医院.json
│   │   ├── medicine_content_dsl #这里广东有这张表故生成一个文件。
│   │   │   └── 广东互联网医院_medicine_db.json 
```

# 更改大数据平台表结构

## 脚本位置

```ini
dslyun@10.106.14.2
cd /data/myang/ods
sh create_table.sh hospitalplus_virtual_commentvb
```



