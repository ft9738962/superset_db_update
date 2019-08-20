import socket, requests, time
import pandas as pd
from config import leed_ver_convert_dict
from datetime import date

def download_csv():
    ''' 下载最新项目列表csv（带顶部三行） '''
    d_url = 'https://www.usgbc.org/sites/default/files/data/PublicLEEDProjectDirectory.xls'
    socket.setdefaulttimeout(30)
    down_count = 1
    while down_count <=5:
        try:
            file = requests.get(d_url)
            break
        except socket.timeout():
            print(f'下载失败，重试')
            down_count += 1
            if down_count == 6: print('下载失败5次，请查看网络连接是否正常')
    with open('data/PublicLEEDProjectDirectory.csv','wb+') as f: 
        f.write(file.content)
    print('下载完成')

def read_csv(cols):
    ''' 接受有用的列名列表，读取csv文件 '''
    dtype = {'ID': object}
    df = pd.read_csv('data/PublicLEEDProjectDirectory.csv',sep='\t', skiprows=3, dtype=dtype,
    usecols=cols)
    return df

def space_to_null(v):
    ''' 空格单元改成NULL，非空格单元不变 '''
    if v == ' ': return 'NULL'
    else: return v

def leed_version_name_convert(old_name):
    ''' 统一版本名称 '''
    try:
        return leed_ver_convert_dict[old_name]
    except:
        return 'NULL'

def quote_convert(col):
    ''' 将英文名中的引号变成下划线 '''
    return col.replace("'","_").replace('"','_')

def datetime_2_date(v):
    ''' 时间简化到日 '''
    try: return date.fromisoformat(v.split(' ')[0])
    except: return 'NULL'

def df_clean(df):
    ''' 数据表清洗 '''
    print('数据表清洗开始')
    bg = time.time()

    df.PointsAchieved = df.PointsAchieved.apply(pd.to_numeric, errors='ignore') #数字列类型变换
    df.GrossSqFoot = df.GrossSqFoot.apply(pd.to_numeric, errors='ignore') #数字列类型变换
    df.TotalPropArea = df.TotalPropArea.apply(pd.to_numeric, errors='ignore') #数字列类型变换

    #NaN, NaT变成null字符
    for i in range(len(df.columns)):
        missing_idx = df.iloc[:,i].isnull().values
        df.iloc[missing_idx,i] = 'NULL'
    
    df = df.applymap(space_to_null)
    df['LEEDSystemVersionDisplayName'] = \
    df['LEEDSystemVersionDisplayName'].apply(leed_version_name_convert)

    string_cols = ['ProjectName','City','State','Country','OwnerTypes','ProjectTypes']
    for col in string_cols:
        df[col] = df[col].apply(quote_convert)

    #去掉不带项目ID的列
    df = df[~pd.to_numeric(df.ID,errors = 'coerce').isnull()]
    
    df['CertDate'] = df['CertDate'].apply(datetime_2_date)
    df['RegistrationDate'] = df['RegistrationDate'].apply(datetime_2_date)
    
    df.reset_index(drop=True, inplace=True) #重新编号

    print(f'数据清洗完成，用时{time.time()-bg:.2f}秒')
    return df

def backup_csv(df):
    try:
        df.to_csv('data/cleaned_df.csv','\t')
        print('清洗后csv数据已备份在data文件夹内')
    except: 
        print('备份数据失败')

def insert_query(vals):
    ''' 将列转成sql语句插入数据库 '''
    beg = 'INSERT INTO projects Values (null'
    for i in range(len(vals)):
        if vals[i] == 'NULL' or i in [7,12,13]:
            beg += f',{vals[i]}'
        else:
            beg += f",'{vals[i]}'"
    return beg+')'