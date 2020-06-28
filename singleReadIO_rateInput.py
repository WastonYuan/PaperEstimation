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
    data_range = 100 # key range 按照随机分配
    block_cache_size = 50 # lru缓存大小
    write_size = 10000 # 输入的key总数
    key_per_block = 10 # 一个block的key总数
    block_per_sst = 5 # 一个sst的block数 sst 的key数要比mem_size大
    lookupkeys_size = 1   # lookupkey的大小 ** 调节几个图
    mem_size = 15   # memtable 一定要比一个sst大 要么一个sst只会有一个block
    read_size = 3 # 读取的key数
    
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
    for write_key_id in range(write_keys_list.__len__()):
        i = write_keys_list[write_key_id]
        mem_data.append(i)
        if mem_data.__len__() >= mem_size or write_key_id == write_keys_list.__len__() - 1:
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
    data_block_io = 0
    index_block_io = 0
    for i in read_keys_list: # loop the read key
        for sst in sst_list:
            find = False
            index_block_io = index_block_io + 1 # this for open index_block
            for block in sst:
                # read the index_block handler check the block is in this block or not
                if block[-1] >= i:
                    data_block_io = data_block_io + 1 # this for get the data_block
                    if block.__contains__(i):
                        find = True
                    break
            if find:
                break
            

    # optimization
    o_index_block_io = 0
    o_data_block_io = 0
    lookupkey_list = []
    cur_key = 0 # current read key
    
    while cur_key < read_size:
        # add key to lookupkey_size
        add = lookupkeys_size - lookupkey_list.__len__()
        for i in range(add):
            if lookupkey_list.__len__() < lookupkeys_size and cur_key < read_size:
                lookupkey_list.append([read_keys_list[cur_key], -1, False]) # every element means (key, last_search_sst, is_find)
                cur_key = cur_key + 1
        # begin a sst search
        if lookupkey_list.__len__() != 0:
            print 'lookup_list:' + str(lookupkey_list)
            cur_sst_id = lookupkey_list[0][1] + 1
            print 'search sst:' + str(cur_sst_id), 'all sst num:' + str(sst_list.__len__())
            cur_sst = sst_list[cur_sst_id] # begin search from first lookupkey_list ele
            for lookupkey in lookupkey_list: # change all last lookupkey point to next sst
                if lookupkey[1] + 1 == cur_sst_id: # every block check all lookupkey
                    lookupkey[1] = cur_sst_id
            o_index_block_io = o_index_block_io + 1 # get the index_block
            remove_lookupkey = []
            print 'cur_sst:' + str(cur_sst)
            key_find_range = []
            for block in cur_sst:
                is_io = False
                for lookupkey in lookupkey_list: # search the sst for seq lookupkeys
                    key = lookupkey[0]
                    if lookupkey[1] == cur_sst_id:
                        if block[-1] >= key and lookupkey[2] == False and not key_find_range.__contains__(key):
                            key_find_range.append(key)
                            if is_io is False:
                                o_data_block_io = o_data_block_io + 1
                                is_io = True
                            if block.__contains__(key):
                                print '    find ' + str(key) + 'in ' + str(block)
                                lookupkey[2] = True
            lookupkey_list = [s for s in lookupkey_list if s[2] == False]
            if cur_sst_id == sst_list.__len__() - 1: # del the cannot find lookupkey
                remove_lookupkey = []
                for lookupkey in lookupkey_list:
                    if lookupkey[1] == cur_sst_id:
                        remove_lookupkey.append(lookupkey)
                for r in remove_lookupkey:
                    lookupkey_list.remove(r)
    
    origin_all = index_block_io + data_block_io
    optimize_all = o_index_block_io + o_data_block_io
    print "origin_io index:" + str(index_block_io) + " data:" + str(data_block_io) + " all:" + str(origin_all)
    print "optimization_io index:" + str(o_index_block_io) + " data:" + str(o_data_block_io) + " all:" + str(optimize_all)
    print "optimize rate:" + str((float(origin_all - optimize_all) / float(origin_all)))








                
                





                




        











