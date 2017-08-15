# -*- coding: utf-8 -*-
"""c_gd_news_basicinfo_add表有数据时执行"""
import hot_news
import utils
from datetime import datetime
import threading

start=datetime.now()

threads = []
t1 = threading.Thread(target=hot_news.separate_calculate_hots_multi_thread_weibo(),args=())
threads.append(t1)
t2 = threading.Thread(target=hot_news.separate_calculate_hots_multi_thread_weixin(),args=())
threads.append(t2)
t3 = threading.Thread(target=hot_news.separate_calculate_hots_multi_thread_website(),args=())
threads.append(t3)

if __name__ == '__main__':

    '''过时热点删除并不是每一次运行都需要跑的，可以设置在每天的一个固定时间，少一次遍历'''
    update_datetime = datetime.now().replace(hour=23, minute=40, second=0)
    print update_datetime
    if utils.time_to_update(update_datetime):
        print 'beginging delete old hotID'
        obsolete_hot = utils.get_obsolete_hot()
        if obsolete_hot != []:
            utils.delete_bact_hot(obsolete_hot)

    if utils.get_data_count():
        for t in threads:
            t.setDaemon(True)
            t.start()

        t.join()

    print(datetime.now()-start)
    print'processde over'