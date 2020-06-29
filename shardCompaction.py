# -*- coding: utf-8 -*
import time
import random

def make_random_list(data_range, data_size):
    randomlist = []
    for i in range(0, data_size):
        n = random.randint(0, data_range)
        randomlist.append(n)
    return randomlist

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

if __name__ == "__main__":
    data_range = 5000
    write_size = 100000 # 输入的key总数
    sst_list = []
    leveln_sst_list = []
    for i in range(5):
        leveln_sst_list.append([])
    sst_size = 10 # 一个sst多少个key
    mem_size = 30 # 大于sst_size
    compaction_size = [10, 100, 1000, 10000, 100000]
    # code begin
    # 随机生成data_size个key
    write_keys_list = make_random_list(data_range, write_size)

    #统计input sst数 output sst数 我们需要的结论是不仅仅是快3倍 
    # 1 2 3 4 5 6 7 8 9 | 1 2 3 
    input_sst_count = 0
    output_sst_cout = 0


    mem = set()
    sst_list = [] #输入l0的sst合集 原始顺序
    for key in write_keys_list:
        if mem.__len__() <= mem_size:
            mem.add(key)
        else:
            ssts = list(chunks(list(mem), sst_size))
            mem = set()
            for i in ssts:
                i.sort()
                sst_list.append(i)
    if mem.__len__() != 0:
        ssts = list(chunks(list(mem), sst_size))
        mem = set()
        for i in ssts:
            i.sort()
            sst_list.append(i)

    # print sst_list

    # origin
    for sst in sst_list:
        leveln_sst_list[0].append(sst)
        # print leveln_sst_list
        # check all level
        is_compaction = False
        for level in range(leveln_sst_list.__len__()):
            
            new_key_list = []
            if leveln_sst_list[level].__len__() >= compaction_size[level]:
                is_compaction = True

            
                # print 'compaction level: ' + str(level)
                # print 'old version:'
                # for i in leveln_sst_list:
                #     print i

                leveln_sst_list[level]
                input_sst_count = input_sst_count + leveln_sst_list[level].__len__()
                new_key_set = set()
                for sst in leveln_sst_list[level]:
                    for k in sst:
                        new_key_set.add(k)
                
                new_key_list = list(new_key_set)
                leveln_sst_list[level] = []

                next_l = leveln_sst_list[level + 1]
                next_l_input_sst = []
                for sst in next_l:
                    # print new_key_list, sst
                    if new_key_list[0] > sst[-1] or new_key_list[-1] < sst[0]:
                        pass
                    else:
                        next_l_input_sst.append(sst)
                        input_sst_count = input_sst_count + 1
                for r in next_l_input_sst:
                    for k in r:
                        new_key_list.append(k)
                    next_l.remove(r)
                new_key_list = list(set(new_key_list))
                new_key_list.sort()
                new_sst_list = list(chunks(new_key_list, sst_size))
                # print 'nsl: ' + str(new_sst_list)
                for i in new_sst_list:
                    output_sst_cout = output_sst_cout + 1
                    next_l.append(i)
                # if is_compaction:
                #     print 'new_version:'
                #     for i in leveln_sst_list:
                #         print i
    print 'now version:'
    for i in leveln_sst_list:
        print i
    print 'intput/output ' + str(input_sst_count) + ' ' + str(output_sst_cout)


    # optimization
    o_input_sst = []
    o_output_sst = []
    k_leveln_sst_list = []
    shard_n = 20 # 增大这个看效果############
    for i in range(shard_n):
        k_leveln_sst_list.append([])
        o_input_sst.append(0)
        o_output_sst.append(0)
        for j in range(5):
            k_leveln_sst_list[i].append([])
    for sst_id in range(sst_list.__len__()):
        sst = sst_list[sst_id]
        shard = sst_id % shard_n
        leveln_sst_list = k_leveln_sst_list[shard]
        leveln_sst_list[0].append(sst)
        # print leveln_sst_list
        # check all level
        is_compaction = False
        for level in range(leveln_sst_list.__len__()):
            
            new_key_list = []
            if leveln_sst_list[level].__len__() >= compaction_size[level]:
                is_compaction = True

            
                # print 'compaction level: ' + str(level)
                # print 'old version:'
                # for i in leveln_sst_list:
                #     print i

                leveln_sst_list[level]
                o_input_sst[shard] = o_input_sst[shard] + leveln_sst_list[level].__len__()
                new_key_set = set()
                for sst in leveln_sst_list[level]:
                    for k in sst:
                        new_key_set.add(k)
                
                new_key_list = list(new_key_set)
                leveln_sst_list[level] = []

                next_l = leveln_sst_list[level + 1]
                next_l_input_sst = []
                for sst in next_l:
                    # print new_key_list, sst
                    if new_key_list[0] > sst[-1] or new_key_list[-1] < sst[0]:
                        pass
                    else:
                        next_l_input_sst.append(sst)
                        o_input_sst[shard] = o_input_sst[shard] + 1
                for r in next_l_input_sst:
                    for k in r:
                        new_key_list.append(k)
                    next_l.remove(r)
                new_key_list = list(set(new_key_list))
                new_key_list.sort()
                new_sst_list = list(chunks(new_key_list, sst_size))
                # print 'nsl: ' + str(new_sst_list)
                for i in new_sst_list:
                    o_output_sst[shard] = o_output_sst[shard] + 1
                    next_l.append(i)
                # if is_compaction:
                #     print 'new_version:'
                #     for i in leveln_sst_list:
                #         print i
    # print 'now version:'
    # for i in leveln_sst_list:
    #     print i
    print 'intput/output ' + str(input_sst_count) + ' ' + str(output_sst_cout)

    print o_input_sst, o_output_sst
    print sum(o_input_sst), sum(o_output_sst)
        

            


            


    
