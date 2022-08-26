#encoing=utf-8
from get_dag import get_dag_datas
import json

data1 = '''select id,goodsName,mom,unitPrice,quantity,totalPrice,storeNo,storeName,storeDistrict,patientName,doctorId,doctorName,isFinished,prescriptionNumber,rxChannel,orderId,orderNumber,agentUserId,agentUserName,status,statusPay,orderType,createRxTime,payTime,createTime,lastUpdateTime,manufacturerId,'''
data2 = '''select id,goodsName,mom,unitPrice,quantity,totalPrice,storeNo,storeName,storeDistrict,patientName,doctorId,doctorName,isFinished,prescriptionNumber,rxChannel,orderId,orderNumber,agentUserId,agentUserName,status,statusPay,orderType,createRxTime,payTime,createTime,lastUpdateTime,manufacturerId,pmId,pmName,'''