# -*- coding: utf-8 -*
import random

# 该程序是模拟 用keyspace的io数差异
# 这里模拟先不引用布隆过滤器 也不引用block缓存
# 不使用keyspace的话:
#   index_block+1 然后如果在某个range中 +1 找到停止下一个key
# 使用keyspace的话:
#   index_block+1 如果有key在range中 +1 找到则删key
# 先整出写入以及读取的key 然后key_per_block个key组装成一个block
# 再整出读取的key 随机

def make_random_list(data_range, data_size):
    randomlist = []
    for i in range(0, data_size):
        n = random.randint(0, data_range)
        randomlist.append(n)
    return randomlist




if __name__ == "__main__":
    data_range = 10 # key range 按照随机分配
    block_cache_size = 50 # lru缓存大小
    write_size = 100 # 输入的key总数
    key_per_block = 5 # 一个block的key总数
    block_per_sst = 3 # 一个sst的block数
    lookupkeys_size = 10 # lookupkey的大小
    mem_size = 15
    read_size = 50 # 读取的key数
    
    # 应该是先加载block_index(顺序查找 那应该是在他后面就可以减少一次io，但是block index本来就在缓存) 先看看是不是顺序查找
    # block_cache 不仅仅会缓存data_block index_block也是会缓存的
    sst_list = []

    # code begin
    # 随机生成data_size个key
    write_keys_list = make_random_list(data_range, write_size)
    # print(write_keys_list)

    # 将元素放入多个sst中
    block = [] # 后面将block追加到sst的block_list里面
    cur_sst = [] # save bock list
    mem_data = []
    for i in write_keys_list:
        mem_data.append(i)
        if(mem_data.__len__() >= mem_size):
            mem_data.sort()
            print mem_data
            for j in mem_data:
                block.append(j)
                if block.__len__() < key_per_block:
                    pass
                else: # a block full
                    cur_sst.append(block)
                    block = []
                    if cur_sst.__len__() < block_per_sst: # sst not full
                        pass
                    else: # sst full and add to level
                        sst_list.append(cur_sst)
                        cur_sst = []
            if block.__len__() != 0:
                cur_sst.append(block)
                block = []
            if cur_sst.__len__()!=0:
                sst_list.append(cur_sst)
                cur_sst = []
            mem_data = []
    
    # print write list
    for i in sst_list:
        print i
    read_keys_list = make_random_list(data_range, read_size)
    print read_keys_list

    # origin read
    io = 0
    for i in read_keys_list: # loop the read key
        for sst in sst_list:
            find = False
            io = io + 1 # this for open index_block
            for block in sst:
                # read the index_block handler check the block is in this block or not
                if block[-1] >= i:
                    io = io + 1 # this for get the data_block
                    if block.__contains__(i):
                        find = True
                    break
            if find:
                break
            
    print 'origin io:' + str(io)

    # optimization
    io = 0
    lookupkey_list = []
    cur_key = 0 # current read key
    while lookupkey_list.__len__() < lookupkeys_size and cur_key < read_size:
        lookupkey_list.append([read_keys_list[cur_key], -1]) # every element means (key, last_search_sst)
        cur_key = cur_key + 1
    while lookupkey_list.__len__() != 0:
            cur_sst_id = lookupkey_list[0][1] + 1
            cur_sst = sst_list[cur_sst_id] # begin search from first lookupkey_list ele
            io = io + 1 # get the index_block
            remove_lookupkey = []
            for block in cur_sst:
                is_io = False
                for lookupkey in lookupkey_list: # search the sst for seq lookupkeys
                    key = lookupkey[0]
                    if lookupkey[1] + 1 == cur_sst_id and block[-1] >= key: # every block check all lookupkey
                        lookupkey[1] = cur_sst_id
                        if is_io is False:
                            io = io + 1
                            is_io = True
                        if block.__contains__(key):
                            remove_lookupkey.append(lookupkey)
            for r in remove_lookupkey:
                lookupkey_list.remove(r)
            while lookupkey_list.__len__() < lookupkeys_size and cur_key < read_size:
                lookupkey_list.append([read_keys_list[cur_key], -1]) # every element means (key, last_search_sst)
                cur_key = cur_key + 1

    print "optimization:" + str(io)









                
                





                




        











