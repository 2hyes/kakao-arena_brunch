import pandas as pd
import json
import os
from itertools import chain
from datetime import datetime
import datetime

"""
"
데이터 파일 로딩
"""
def training_loading():
    path = './res/'
    ## predict/
    f = open(path+'predict/dev.users')
    dev_users_list = f.read().splitlines()
    f.close()
    f2 = open(path+'predict/test.users')
    test_users_list = f2.read().splitlines()
    f2.close()

    ## 사용자 
    users = pd.read_json(path+'users.json', lines=True)

    ## 글 메타데이터
    metadata = pd.read_json(path+'metadata.json', lines=True)

    ## 매거진 정보
    magazine = pd.read_json(path+'magazine.json', lines=True)

    ## 일부 독자가 읽은 글의 데이터
    read_file_list = os.listdir(path+'read/')
    exclude_file_list = ['read.tar', '.2019010120_2019010121.un~']
    read_df_list = []
    for f in read_file_list:
        file_name = os.path.basename(f) 
        if file_name in exclude_file_list:
            continue
        #read.tar / .un~파일 외에, read_csv
        else: 
            df_temp = pd.read_csv(path+'read/'+f, header=None, names=['raw'])
            df_temp['date'] = file_name[:8]
            df_temp['hour'] = file_name[8:10]
            df_temp['user_id'] = df_temp['raw'].str.split(' ').str[0]
            df_temp['article_id'] = df_temp['raw'].str.split(' ').str[1:].str.join(' ').str.strip()
            read_df_list.append(df_temp)
            
    read = pd.concat(read_df_list)
    """
    read = read[read['article_id']!='']
    read = read.reset_index() # 파일 여러개를 이어붙여서, index가 중복됨.
    read.article_id = read.article_id.str.split(' ') # 리스트 형태로 변경
    del read['raw'], read['index']
    """

    read_cnt_by_user = read['article_id'].str.split(' ').map(len)
    read = pd.DataFrame({'date': np.repeat(read['date'], read_cnt_by_user),
    'hour': np.repeat(read['hour'], read_cnt_by_user),
    'user_id': np.repeat(read['user_id'], read_cnt_by_user),
    'article_id': list(chain.from_iterable(read['article_id'].str.split(' '))) })


    print('data loaded!', '\n')
    
    return dev_users_list, test_users_list, users, metadata, magazine, read


#_,_,users, metadata, magazine, read  = training_loading()
