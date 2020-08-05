import preprocessing
import recommender

## dev
target_users_list, users, metadata, magazine, read, read_each_article = preprocessing.data_preprocessing('dev')
recommed = recommender.recommender(target_users_list, './recommend.txt')

## test
#target_users_list, users, metadata, magazine, read, read_each_article = data_preprocessing('test')
#recommed = recommender(target_users_list, './recommend.txt')
