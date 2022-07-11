import time
import numpy as np
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import requests
import json
def send_message1(content,webhook):
    url = webhook
    payload_message = {
        "msg_type": "text",
        "content": {
            "text": content #这里加要发送的文字信息
        }
    }
    headers = {
        'Content-Type': 'application/json'
    }
    requests.request("POST", url, headers=headers, data=json.dumps(payload_message))
webhook = 'https://open.feishu.cn/open-apis/bot/v2/hook/54ef71c5-3cf3-4bec-9c4c-65e37621f141'

pt_h = time.strftime("%H%M", time.localtime(time.time()-3600*1*0)) #用于设定运行的日期
send_ok = pt_h=='0800'

db_ip = "10.129.41.147:4918"
user = "dmp_sys_guest"
password = "fTe^3hy*dO_363"
pos1 = "ck"  #取筛选&算分参数的数据库

#获取表格中最新的uploadtime，数据源筛选需大于该时间
def get_last_time():
    engine=create_engine('mysql+pymysql://'+user+':'+password+'@'+db_ip+'/'+pos1+'?charset=utf8') # 可用于to_sql和read_sql
    conn=engine.connect() # 只能用于read_sql,这语句没必要存在，可删除。
    last_time = pd.read_sql("SELECT max(uploadtime) as uploadtime FROM ck.iupr_log" , engine)['uploadtime'].iloc[-0]
    return str(last_time)


def cal(last_time):
    engine=create_engine('mysql+pymysql://'+user+':'+password+'@'+db_ip+'/'+pos1+'?charset=utf8') # 可用于to_sql和read_sql
    conn=engine.connect() # 只能用于read_sql,这语句没必要存在，可删除。
    df = pd.read_sql("SELECT ein,create_time,ecu_iupr,dcu_iupr FROM ck.tboxlog where ((ecu_iupr is not null and ecu_iupr!='') and (dcu_iupr is not null and dcu_iupr!='')) and create_time>'%s'"%last_time, engine)
    df
    if len(df)!=0:
        def ecu_split(x):
            a = ''
            if len(x)==72:
                for i in range(18):
                    a += str(int(x[i*4:i*4+4],16))+','
            return a[:-1]
        df['ecu_value'] =  df['ecu_iupr'].apply(ecu_split)    
        df['dcu_value'] =  df['dcu_iupr'].apply(ecu_split)   
        df
        ecu_list = ['ECU_1', 'ECU_2', 'ECU_3', 'ECU_4', 'ECU_5', 'ECU_6', 'ECU_7', 'ECU_8', 'ECU_9', 'ECU_10', 'ECU_11', 'ECU_12',
         'ECU_13', 'ECU_14', 'ECU_15', 'ECU_16', 'ECU_17', 'ECU_18']
        dcu_list = ['DCU_1', 'DCU_2', 'DCU_3', 'DCU_4', 'DCU_5', 'DCU_6', 'DCU_7', 'DCU_8', 'DCU_9', 'DCU_10', 'DCU_11', 'DCU_12', 
         'DCU_13', 'DCU_14', 'DCU_15', 'DCU_16', 'DCU_17', 'DCU_18']
        df[ecu_list] = df['ecu_value'].str.split(',',expand=True)
        df[dcu_list] = df['dcu_value'].str.split(',',expand=True)
        for i in ecu_list+dcu_list:
            df[i] = df[i].astype('int')
        df['NMHC比值'] = df['ECU_3']/df['ECU_4']
        df.loc[df['ECU_4']==0,'NMHC比值'] = -1  #分母=0赋值-1
        df['NOx/SCR比值'] = df['DCU_5']/df['DCU_6']
        df.loc[df['DCU_6']==0,'NOx/SCR比值'] = -1  #分母=0赋值-1
        df['PM比值'] = df['ECU_9']/df['ECU_10']
        df.loc[df['ECU_10']==0,'PM比值'] = -1  #分母=0赋值-1
        df['废气传感器比值'] = df['ECU_11']/df['ECU_11']
        df.loc[df['DCU_11']/df['DCU_11']<df['ECU_11']/df['ECU_11'],'废气传感器比值'] = df['DCU_11']/df['DCU_11']
        df.loc[(df['ECU_11']==0)&(df['DCU_11']==0),'废气传感器比值'] = -1   #两个分母=0赋值-1
        df['EGR/VVT比值'] = df['ECU_13']/df['ECU_14']
        df.loc[df['ECU_14']==0,'EGR/VVT比值'] = -1  #分母=0赋值-1
        df['增压压力比值'] = df['ECU_15']/df['ECU_16']
        df.loc[df['ECU_16']==0,'增压压力比值'] = -1  #分母=0赋值-1
        df = df[['ein', 'create_time','ECU_1', 'ECU_2', 'ECU_3', 'ECU_4','NMHC比值', 'DCU_5', 'DCU_6', 'NOx/SCR比值','ECU_9',
            'ECU_10','PM比值', 'DCU_11', 'DCU_12', '废气传感器比值', 'ECU_13', 'ECU_14','EGR/VVT比值', 'ECU_15', 'ECU_16',
            '增压压力比值']]
        df.columns = ['ein', 'uploadtime', 'obd_deno', 'ignition_count', 'nmhc_nume',
           'nmhc_deno', 'nmhc_ratio', 'nox_scr_nume', 'nox_scr_deno',
           'nox_scr_ratio', 'pm_nume', 'pm_deno', 'pm_ratio', 'flue_gas_nume',
           'flue_gas_deno', 'flue_gas_ratio', 'egr_vvt_nume', 'egr_vvt_deno',
           'egr_vvt_ratio', 'super_press_nume', 'super_press_deno',
           'super_press_ratio']
    return df
def insert_data(df):
    engine=create_engine('mysql+pymysql://'+user+':'+password+'@'+db_ip+'/'+pos1+'?charset=utf8') # 可用于to_sql和read_sql
    conn=engine.connect() # 只能用于read_sql,这语句没必要存在，可删除。
    df.to_sql('iupr_log', index=False, con=engine, if_exists='append')
if __name__=="__main__":  
    try:
        n = 'start'
        last_time = get_last_time()
        n = str(last_time)
        print(last_time)
        df = cal(last_time)
        n = str(len(df))
        if len(df)!=0:
            insert_data(df)
        n = 'ok'
        if send_ok:
            content = f'ID:7\nIUPR_参数程序运行成功——{last_time}——插入行数{len(df)}' 
            send_message1(content,webhook)
    except Exception as e:  
        content = f'IUPR_参数程序运行失败\n原因：{str(e)}\n{n}'
        send_message1(content,webhook)