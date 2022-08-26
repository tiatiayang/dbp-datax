import requests
import json
from utils import print_run_time,_write_html


def get_dag_datas(dag_id):
    #url = "http://10.106.14.1:12345/dolphinscheduler/projects/3955165249536/process-definition/3955165257099?_t=0.8969688185943197"
    url = "http://10.106.14.1:12345/dolphinscheduler/projects/3955165249536/process-definition/{}".format(dag_id)


    headers = {
        #"Authorization":"Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxMixiaV9iZHAiLCJpc3MiOiJhZG1pbiIsImV4cCI6MTY2MDgxMzg5NCwiaWF0IjoxNjYwMjA5MDk0LCJyb2wiOiJST0xFX1VTRVIifQ.JaiLGMRx3naUdULYaHCSz20v36IJSyOIA1pTDkazV2nC2N51vjixQuc5shgmN4-u5EAgyF9_yO2FiWb_hNzARQ",
        "Cookie":"sessionId=6fd4ec9b-cc7d-4ed2-a857-2c6628d16119; language=zh_CN; sessionId=6fd4ec9b-cc7d-4ed2-a857-2c6628d16119",
        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"
    }
    res = requests.get(url,headers=headers)
    print("get_dag_datas", res.status_code)
    if res.status_code == 200:
        return res.json()

def get_taskDefinitionList(res_data):
    taskDefinitionList = res_data.get("data",{}).get("taskDefinitionList",[])
    return taskDefinitionList
def get_taskParams(task_data):
    taskParams = task_data.get("taskParams", {})
    return taskParams
def get_processDefinitionCode(task_data):
    taskParams = get_taskParams(task_data)
    processDefinitionCode = taskParams.get("processDefinitionCode")
    return processDefinitionCode
def get_name(task_data):
    name = task_data.get("name", '')
    return name
def get_taskType(task_data):
    taskType = task_data.get("taskType", '')
    return taskType
def get_table_name(tag_name):
    #zh_hlwyy_ownership_agent_team_tmp
    if "_hlwyy_" in tag_name:
        return tag_name.split("_hlwyy_")[-1]
        #return tag_name.replace("zh_hlwyy_",'').rstrip("_dx")
def get_dag_list(dag_id,table_name=None):
    data = get_dag_datas(dag_id)
    taskDefinitionList = get_taskDefinitionList(data)
    print(taskDefinitionList)
    #print(json.dumps(data,ensure_ascii=False))
    dag_list = []
    index = 0
    for task in taskDefinitionList:
        new_table_name = None
        index += 1
        task_dict = {}
        processDefinitionCode = get_processDefinitionCode(task)
        name = get_name(task)
        if not table_name:
            new_table_name = get_table_name(name)
        else:
            new_table_name = table_name
        task_dict["table_name"] = new_table_name
        print(processDefinitionCode)
        task_dict["name"] = name
        task_dict["dag_id"] = processDefinitionCode
        task_dict["taskType"] = get_taskType(task)
        task_dict["father_dag_id"] = dag_id
        if not processDefinitionCode:
            task_dict["taskParams"] = get_taskParams(task)
            task_dict["son_dag_list"] = []
            dag_list.append(task_dict)
            continue

        son_dag_list = get_dag_list(processDefinitionCode,table_name=new_table_name)

        task_dict["son_dag_list"] = son_dag_list
        dag_list.append(task_dict)
    return dag_list


def run_task():
    dag_id = '3955165257099'
    dag_list = get_dag_list(dag_id)
    print(json.dumps(dag_list,ensure_ascii=False))
    #_write_html('json/task/{}.json'.format(dag_id),json.dumps(dag_list,ensure_ascii=False,indent=4))

if __name__ == '__main__':
    run_task()