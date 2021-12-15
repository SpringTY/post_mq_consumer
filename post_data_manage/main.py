from data.mongo import convert,save_to_post_predict

from process.process import ping,process_main
import os
import sys
import time
cur_path = os.path.abspath(__file__)
sys.path.append(cur_path + "/process")


def do_task(task_tag):
    [region,date] = task_tag.split('-')
    x = convert(task_tag)
    last_x, last_len, global_x, unpick_x, unpick_len, unpick_geo, days_np, order_np, index_np, eta_np, dic_geo2index = process_main(x)
    save_to_post_predict(region,date,unpick_x,unpick_len,order_np)
# print(unpick_x[0])
# print(ping())
# print(eta_np[0])
# import happybase
# # 注意protocol和transport这两个参数，需要和hbase启动命令中的相同，否则会报错
# connection = happybase.Connection('211.71.76.189',port=9090)
# print(connection.tables())
do_task('上海市-20200423')

# info = '20191114,13603,上海市,4398066205389,2019-11-14 07:48:22,2019-11-14 09:00:00,2019-11-14 11:00:00,121.406618504,31.311933287,24566681,3110,居民区,2019-11-14 09:46:57,2019-11-14 09:43:36,121.4055433485243,31.312476942274305,13.69074535369873,,,,'
# infos = info.split(',')
# day = infos[0]
# city = info[2]
# post_man_id = infos[3]

# info_id = infos[0] + 'shanghai' + infos[3]
# print('info_id: %s'%info_id)
# postman_test_table = happybase.Table('postman_test',connection)
# row = {
#     'id:id':'%s' %info_id,
#     'post_arrive_info:raw': '%s' %info,
#     'day:day' :'%s'%day,
#     'region:region':'%s' %city,
#     'post_id:post_id':'%s' %post_man_id
# }
# # 插入
# # postman_test_table.put('%s' %info_id, row)
# # 查询
# x = postman_test_table.cells(info_id,'post_arrive_info:raw')
# # 
# print(type(x))
# print(len(x))
# print(x)
#insert to hbase


# 






# from rocketmq.client import PullConsumer
# consumer = PullConsumer('post_data_manage_consumer')
# consumer.set_namesrv_addr('211.71.76.189:9876')
# consumer.start()
# while True :
#     for msg in consumer.pull('post_data_manage'):
#         print(msg.id, msg.body)
#     time.sleep(5) # 防止CPU空转
#     print("epoch")

# consumer.shutdown()