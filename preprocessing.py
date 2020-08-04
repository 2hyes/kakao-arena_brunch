def column_name_fixing(users, magazine, metadata):
    users.rename(columns={"id":"user_id"}, inplace = True)
    magazine.rename(columns={"id":"magazine_id"}, inplace = True)
    metadata.rename(columns={"keyword_list":"article_tag_list"}, inplace = True)
    return users, magazine, metadata
    
#users, magazine, metadata = column_name_fixing(users, magazine, metadata)

def counting(users, read):
    # ascending = False: 점수가 높을 수록 상위 순위
    # method='dense': 동점자에게 같은 순위 부여 / 그룹 간 순위가 1씩 증가
    """
    ['following_cnt']: 해당 사용자가 구독하는 작가 수 col 생성 
    ['following_cnt_rank']: 랭킹 col 생성 
    """
    users['following_cnt'] = users['following_list'].map(len)
    users['following_cnt_rank'] = users['following_cnt'].rank(ascending=False,method='dense')

    """
    ['article_cnt']: 해당 사용자가 읽은 글의 수 col 생성
    랭킹 col 생성 x (시간 당 읽은 글의 수--> 사용자id 중복)

    """
    read['article_cnt'] = read['article_id'].map(len)

    return users, read

#users, read = counting(users, read)

"""
metadata preprocessing
:metadata.reg_ts가 unix time으로 되어있기때문에,timestamp로 변경.
"""
def unix_to_timestamp(metadata):
    metadata.reg_ts /= 1000.0  # milisecond로 되어있기때문에
    metadata['date'] = metadata.reg_ts.apply(lambda d: datetime.datetime.fromtimestamp(int(d)).strftime('%Y-%m-%d'))
    metadata['hour'] = metadata.reg_ts.apply(lambda d: datetime.datetime.fromtimestamp(int(d)).strftime('%H:%M:%S'))
    metadata['weekday'] = metadata.reg_ts.apply(lambda d: datetime.datetime.fromtimestamp(int(d)).strftime('%a'))
    return metadata

#metadata = unix_to_timestamp(metadata)

"""
metadata preprocessing
: 일정 기간 동안의 조회수 recent_view / 전체 기간 동안의 조회수 view 생성
"""
def metadata_preprocessing(metadata, read_each_article, from_dt, to_dt):
    # 전체 조회수 view
    view = read_each_article.groupby('article_id').count()['user_id']
    view_df = pd.DataFrame({'id':view.index, 'view':view.values})
    metadata = pd.merge(metadata, view_df, how='left', on='id')
    metadata['view'] = metadata['view'].fillna(0)
    
    # 최근 조회수 recent_view
    dt = pd.to_numeric(read_each_article['date'])
    partial_read = read_each_article[(dt >= from_dt) & (dt <= to_dt)]
    recent_view = partial_read.groupby('article_id').count()['user_id']
    recent_view_df = pd.DataFrame({'id':recent_view.index, 'recent_view':recent_view.values})
    metadata = pd.merge(metadata, recent_view_df, how='left', on='id')
    metadata['recent_view'] = metadata['recent_view'].fillna(0)
    print('metadata preprocessing completed!', '\n')

    return metadata


# 최근(2주로 설정) 조회수 와 전체 조회수 생성
#metadata = metadata_preprocessing(metadata, read_each_article, 20190215, 20190228)

"""
read preprocessing
: date colum dtype 변환(str->int)
"""
def read_preprocessing(read):   
    if type(read['date'].values[0]) == type('string'):
        date = read['date'].tolist()
        read['date'] = [int (i) for i in date]
        print('read preprocessing completed!\n')
    else:
        print('already preprocessed!\n')
    return read

#read = read_preprocessing(read)

"""
전체 전처리 함수 실행
"""

def data_preprocessing(mode):
    if mode == 'dev':
       target_users_list, _, users, metadata, magazine, read, read_each_article = training_loading()
    elif mode == 'test':
        _, target_users_list, users, metadata, magazine, read, read_each_article = training_loading()
    
    users, magazine, metadata = column_name_fixing(users, magazine, metadata)
    users, read = counting(users, read)
    metadata = unix_to_timestamp(metadata)
    metadata = metadata_preprocessing(metadata, read_each_article, 20190215, 20190228)
    read = read_preprocessing(read)

    return target_users_list, users, metadata, magazine, read, read_each_article
