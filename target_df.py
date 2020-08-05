""" 
target_df 생성 (users에 없는 user_id 추가) 
: 매개변수에는 dev_users_list / test_users_list 와 users. 
""" 
def target_df_generator(target_users_list, users):
    print('1. target user의 dataframe 생성 중')
    target_df = users[users['user_id'].isin(target_users_list)]
    
    for target_user in target_users_list:
        if (target_user in target_df['user_id'].tolist()) == False:
            new_df = pd.DataFrame({'following_list':[[]], 'user_id':[target_user], 'keyword_list':[[]]})
            target_df = target_df.append(new_df)
    
    print('target user의 dataframe 생성 완료')
    return target_df

"""
전체 기간동안 target user가 읽은 article 저장
"""
def target_read_article(target_df, read):
    print('2. 전체 기간동안 target user가 읽은 article 저장 중\n')

    target_read_article = []
    for idx in target_df['user_id'].values.tolist(): # user_id한개씩 루프
        read_list = read[read['user_id']==idx]
        read_list = read_list[read_list['article_id'].str.startswith('@')]['article_id'].tolist() #해당 user가 읽은 글 목록
        target_read_article.append(read_list)

    #target_df['read'] = read_list
    target_df['read'] = target_read_article # 읽은 글 
            
    print('전체 기간동안 target user가 읽은 article 저장 완료\n')
    return target_df

"""
최근 2주동안 target user가 읽은 article저장
"""
def target_recent_article(target_df, read, from_dt, to_dt):
    print('3. 최근 2주 동안 target user가 읽은 article 저장 중\n')

    target_recent_article = []
    partial_read = read[(read.date >= from_dt) & (read.date <= to_dt)]

    for idx in target_df['user_id'].values.tolist(): # user_id한개씩 루프
        read_list = partial_read[partial_read['user_id']==idx]
        read_list = read_list[read_list['article_id'].str.startswith('@')]['article_id'].tolist()
        target_recent_article.append(read_list)
    target_df['recent'] = target_recent_article

    print('최근 2주 동안 target user가 읽은 article 저장 완료\n')
    return target_df
    
    """
target user가 본 '구독 작가 글의 cnt' 저장
"""
# mode: read / recent
def target_read_following(target_df, mode):
    print('4. target user가 본 following의 빈도수 저장 중\n')

    target_read_following = []
    for idx in target_df['user_id'].values.tolist():
        # following_frequency: read_list 중에서 해당 작가(f_id)의 글의 빈도수
        following_frequency = {}
            
        # read_list: 타겟 사용자가 전체기간/최근 동안 읽은 글 리스트
        read_list = target_df[target_df['user_id']==idx][mode].values[0][:]
            
        if len(read_list) > 0:
            # read_series: 읽은 글 리스트 -> Series
            read_series = pd.Series(read_list)

            # f_list: 타겟 사용자가 구독하는 작가 리스트
            f_list = target_df[target_df['user_id']==idx]['following_list'].values[0][:]
            for i in range(len(f_list)):
                f_list[i] = f_list[i] + '_'

            # read_series[read_series.str.startswith(f_id)].tolist() 
            # 읽은 글 중에 구독하는 작가의 글을 출력 
            for f_id in f_list:
                # frequency: 구독하는 작가 글 중 읽은 글 갯수
                frequency = len(read_series[read_series.str.startswith(f_id)].tolist())
                if frequency > 0:
                    # 그 구독하는 작가명 : frequency 형태로 저장
                    following_frequency[f_id[:-1]]=frequency
                        
        target_read_following.append(following_frequency)

    target_df[mode+'_following'] = target_read_following
    
    print('target user가 본 following의 빈도수 저장 완료\n')
    return target_df
    
    """
target user가 본 '매거진 cnt' 저장
"""
# mode: read / recent
def target_read_magazine(target_df, metadata, mode):
    print('5. target user가 본 magazine의 빈도수 저장 중\n')
    target_read_magazine = []

    for idx in target_df['user_id'].values.tolist():
        # read_list: 타겟 사용자가 전체기간/최근 동안 읽은 글 리스트
        read_list = target_df[target_df['user_id']==idx][mode].values[0][:]
        # magazine_list: metadata에서 타겟 사용자가 읽은 글 레코드(id)의 magazine_id를 뽑아냄
        magazine_list = metadata[metadata['id'].isin(read_list)]['magazine_id'].tolist()
            
        # magazine_id 빈도수 dictionary로 저장
        magazine_frequency = Counter(magazine_list)
        del magazine_frequency[0]
        target_read_magazine.append(magazine_frequency)

       
    target_df[mode+'_magazine'] = target_read_magazine
    
    print('target user가 본 magazine의 빈도수 저장 완료\n')
    return target_df
    
    """
target user가 본 글의 태그 cnt저장
--> 읽은 글의 태그와 같은 태그를 가진 글을 추천해주기 위함
"""
# mode: read / recent
def target_read_tag(target_df, metadata, mode):
    print('6. target user가 본 글의 태그 빈도수 저장 중\n')

    target_read_tag = []
    keyword_list = {}
    for idx in target_df['user_id'].values.tolist():
        # read_list: 타겟 사용자가 전체기간/최근 동안 읽은 글 리스트
        read_list = target_df[target_df['user_id']==idx][mode].values[0][:]
        # metadata에서 타겟 사용자가 읽은 글 레코드(id)의 keyword_list(검색키워드)
        # 각 타겟 사용자가 일정 기간 동안 읽은 글의 태그('article_tag_list') 합쳐서 저장
        keyword_list = metadata[metadata['id'].isin(read_list)]['article_tag_list'].tolist()
        read_tag = sum(keyword_list, [])
            
        # 각 타겟 사용자가 일정 기간 동안 읽은 글의 태그들의 빈도수 dictionary로 저장
        frequency = Counter(read_tag)
        target_read_tag.append(frequency)

            
    target_df[mode+'_tag'] = target_read_tag

    print('target user가 본 글의 태그 빈도수 저장 완료\n')
    return target_df
    
    """
target user의 tag에서 빈도수가 높은 상위 top_N개를 관심 키워드로 저장
--> 타겟 유저의 관심사를 알고 추천해주기 위함.
"""
# mode: read / recent
# top_N: 태그 중 빈도수 높은 top_N개의 interest를 저장.
def target_read_interest(target_df, top_N, mode):
    print('7. target user 관심 태그 상위',top_N,'개 저장 중\n')
    target_read_interest = []
    # read_tag에서 빈도수가 높은 상위 top_N개의 키워드 저장
    for idx in target_df['user_id'].values.tolist():
        interest = []
        # rt: 읽은 글의 태그 정보
        rt = target_df[target_df['user_id']==idx][mode+'_tag'].values[0]
        # sorted_rt: rt를 순서대로 정렬(내림차순)
        sorted_rt = sorted(rt.items(), key=lambda x: x[1], reverse=True)
            
        for i in range(len(sorted_rt[:top_N])):
            interest.append(sorted_rt[:top_N][i][0])
        target_read_interest.append(interest)
            
    target_df[mode+'_interest'] = target_read_interest

    print('target user 관심 태그 상위',top_N,'개 저장 완료\n')
    return target_df
    
    ## target_df 'behavior' 전처리 (target user의 글 소비 성향 저장)
def target_read_behavior(target_df, metadata, mode):
    print('8. target user 글 소비 성향 저장 중\n')

    ratio_list = []

    if mode == 'recent':
    # 최근 기간 -> pop: recent_view가 상위 20%인 글 / reg: 2019.02.15 ~ 2019.03.01 동안 발행된 글
        pop_id = metadata[metadata['recent_view'] > metadata['recent_view'].quantile(0.80)]['id'].tolist()
        reg_id = metadata[(metadata['reg_ts'] >= get_unix_time(20190215)) & (metadata['reg_ts'] < get_unix_time(20190301))]['id'].tolist()
    else:
    # 전체 기간 -> pop: view가 상위 20%인 글 / reg: 2018.09.15 ~ 2019.03.01 동안 발행된 글
        pop_id = metadata[metadata['view'] > metadata['view'].quantile(0.80)]['id'].tolist()
        reg_id = metadata[(metadata['reg_ts'] >= get_unix_time(20180915)) & (metadata['reg_ts'] < get_unix_time(20190301))]['id'].tolist()

       
    for idx in target_df['user_id'].values.tolist():
        read_list = target_df[target_df['user_id']==idx][mode].values[0][:]

        fr_dic = target_df[target_df['user_id']==idx][mode+'_following'].values[0]
        mr_dic = target_df[target_df['user_id']==idx][mode+'_magazine'].values[0]
        pop = pd.Series(read_list)[pd.Series(read_list).isin(pop_id)].tolist()
        reg = pd.Series(read_list)[pd.Series(read_list).isin(reg_id)].tolist()

        # f_ratio -> target user가 본 article 중에서 following의 비율
        # m_ratio -> target user가 본 article 중에서 magazine의 비율
        # p_ratio -> target user가 본 article 중에서 인기가 많은 글('recent_view' or 'view'가 상위 20%)의 비율
        # r_ratio -> target user가 본 article 중에서 최근 발행된 글('reg_ts'가 '190215' or '180915' 이후)의 비율
        f_ratio, m_ratio, p_ratio, r_ratio = 0, 0, 0 ,0

        if len(read_list) >= 1:
            f_ratio = round(sum(fr_dic.values())/len(read_list), 2)
            m_ratio = round(sum(mr_dic.values())/len(read_list), 2)      
            p_ratio = round(len(pop)/len(read_list), 2)
            r_ratio = round(len(reg)/len(read_list), 2)

        ratio_list.append([f_ratio, m_ratio, p_ratio, r_ratio])

    ratio_df = pd.DataFrame(ratio_list, columns=[mode+'_f_ratio', mode+'_m_ratio', mode+'_p_ratio', mode+'_r_ratio'])
    target_df.index = list(range(len(target_df)))
    target_df = pd.concat([target_df, ratio_df], axis=1)
    
    print('8. target user 글 소비 성향 저장 중\n')
    return target_df
    
    """
타겟 사용자 데이터프레임 전처리
"""
def target_users_preprocessing(target_users_list, users, metadata, read, from_dt, to_dt, top_N, mode):
    target_df = target_df_generator(target_users_list, users)
    target_df = target_df.reset_index() # id 리셋
    del target_df['index']
    target_df = target_read_article(target_df, read)
    target_df = target_recent_article(target_df, read, from_dt, to_dt)
    target_read_following(target_df, mode)
    target_df = target_read_magazine(target_df, metadata, mode)
    target_df = target_read_tag(target_df, metadata, mode)
    target_df = target_read_interest(target_df, top_N, mode)
    target_df = target_read_behavior(target_df, metadata, mode)
    return target_df
    
    
