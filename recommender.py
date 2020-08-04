def recommender(target_users_list, output_txt):
    recommend_list = metadata.sort_values(by='view', ascending=False)['id'][0:100].tolist()

    f = open(output_txt, 'w')
    global i
    for i in range(len(target_users_list[i])):
        f.write(target_users_list[i])
        f.write(' ')
        for j in range(len(recommend_list)):
            f.write(recommend_list[j])
            f.write(' ')    
        f.write('\n')
    f.close()

    return recommend_list
