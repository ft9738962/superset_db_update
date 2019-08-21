from function import *
from config import usecols
import csv, os, sqlite3,time
import pandas as pd
import numpy as np
import logging
from dotenv import load_dotenv

def setup_logger(logger_name, log_file, level):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s',
     datefmt='%Y-%m-%d %H:%M:%S')
    fileHandler = logging.FileHandler(log_file)
    fileHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(fileHandler)

def main():
    setup_logger('log_r','update_routine.log',20)
    setup_logger('log_e','update_error.log',40)
    log_r = logging.getLogger('log_r')
    log_e = logging.getLogger('log_e')
    update_bg = time.time()
    
    log_r.info('开始更新数据库')
    load_dotenv() #读环境参数
    if download_csv() <6: #下载新数据表
         log_r.info('下载数据完成')
    else: log_e.error('下载数据失败，请查看网络连接')
    
    try:
        conn = sqlite3.connect(os.getenv('DB_PATH')) #连接数据库
        log_r.info('数据库已连接')
    except:
        log_e.error('数据库连接失败')

    last_row = conn.execute(os.getenv('LAST_ROW')).fetchall()[0]
    last_row_no = last_row[0] #获取保守的最后行数
    last_project_id = last_row[1] #获取之前的数据输入的最后项目号

    clean_bg = time.time()
    log_r.info('数据表清洗开始')
    df = read_csv(usecols).iloc[last_row_no:,]
    if df.shape[0]: #读取清洗后的新表
        df = df_clean(df)
        log_r.info(f'数据清洗完成，用时{time.time()-clean_bg:.2f}秒')
    else: log_e.error('清洗后df无数据')
   
    start_index = df[df['ID']==last_project_id].index.values[0] + 1
    if start_index >= df.shape[0]:
        conn.close()
        log_r.info('无新数据，结束更新数据库\n')
    else:
        log_r.info(f'开始更新插入新数据 ')
        for i in range(start_index, df.shape[0]): #输入新数据
            vals = df.iloc[i,]
            try:
                conn.execute(insert_query(vals))
                if i == df.shape[0]-1:
                    conn.commit()
                    conn.close()
                    log_r.info(f'更新插入{df.shape[0]-start_index}行数据已完成，用时{time.time()-update_bg:.2f}秒\n')
            except:
                log_e.error(f'插入ID为{vals[0]}的项目时出现错误，退出更新\n')
                break

if __name__ == "__main__":
    main()
