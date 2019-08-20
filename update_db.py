from function import *
from config import usecols
import csv, os, sqlite3,time
import pandas as pd
import numpy as np
from dotenv import load_dotenv

def main():
    bg = time.time()
    print('开始更新数据库')
    load_dotenv() #读环境参数
    download_csv() #下载新数据表
    df = df_clean(read_csv(usecols)) #读取清洗后的新表
    backup_csv(df) #备份csv文件
    conn = sqlite3.connect(os.getenv('DB_PATH')) #连接数据库
    print('数据库已连接')
    last_project_id = conn.execute(os.getenv('LAST_ROW')).fetchall()[0][1] #获取之前的数据输入的最后项目号
    start_index = df[df['ID']==last_project_id].index.values[0] + 1
    print(f'从第{start_index}行开始插入新数据 ')
    for i in range(start_index, df.shape[0]): #输入新数据
        vals = df.iloc[i,]
        try:
            conn.execute(insert_query(vals))
            if i == df.shape[0]-1:
                conn.commit()
                conn.close()
                print(f'更新插入{df.shape[0]-start_index}行数据已完成，用时{time.time()-bg}秒')
        except:
            print(f'插入第{i}行数据时出现错误，退出更新')
            break

if __name__ == "__main__":
    main()
