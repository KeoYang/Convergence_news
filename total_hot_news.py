# -*- coding: utf-8 -*-

import chinese_segment

import model_train
import utils
from six import iteritems
from datetime import datetime

import math


def calculate(raw_data,tag):
    '''tag:原始新闻属于哪一个信息来源'''
    """获取原始新闻数据"""
    if not raw_data=={}:
        entityId_list = []  # 新闻ID
        train_titles = []
        publishDateTime_list = []
        count = 0
        for k, v in raw_data.items():
            if v.get('content') is None or v.get('content') == '':
                continue
            count += 1
            entityId_list.append(k)

            if v.get('title') is None or v.get('title') == '':
                train_titles.append(v.get('content'))
            else:
                train_titles.append(v.get('content') + ' ' + v.get('title'))
            publishDateTime_list.append(v.get('publishDateTime'))

        print "get raw data done!"

        """聚类分析"""
        hotId2newsId = {} #热点对应的新闻ID
        hotId_to_be_write = {}

        train_titles = [chinese_segment.get_segment(title) for title in train_titles]
        train_data = [model_train.get_tf(d) for d in train_titles]
        '''第一次聚类，将原始数据聚类为若干热点'''
        first_cluster_result = model_train.first_cluster(train_data, entityId_list, publishDateTime_list)

        old_hotId_words = utils.get_hotId_words(tag)

        hotId_newsId = []#热点对应的新闻ID
        '''
        当前新闻热点与已有新闻热点比较，如果该热点不在已有热点中则，插入该热点，否则跳过

        '''
        for first_result in first_cluster_result:
            terms = model_train.get_tf_from_list(first_result)#热点下面的关键词
            hotId = utils.get_hot_id(terms, old_hotId_words) #该新闻热点产生一个热点标志
            if hotId not in old_hotId_words:
                hotId_to_be_write[hotId] = terms #新产生一个热点
            for news in first_result:
                # record = [hotId, news[0], news[2], utils.generateTime(),tag]#news[0],[2]分别是新闻ID 以及 产生时间
                record = [hotId, news[0], news[2], utils.generateTime()]#news[0],[2]分别是新闻ID 以及 产生时间
                hotId_newsId.append(record)
                if hotId2newsId.__contains__(hotId):
                    hotId2newsId[hotId].append(news[0])#热点事件以及其对应的新闻事件
                else:
                    hotId2newsId[hotId] = [news[0]]
        utils.write2hotId_newsId(hotId_newsId)
        print "write to newsId done!"

        """写新的新闻热点"""
        for hotId, terms in hotId_to_be_write.items():
            if hotId in hotId2newsId:
                utils.write2hotId_words(hotId, terms, tag)
        print "write to words done!>>>>>>>>>"

        """抽取每个热点下新闻主题词"""
        # 统计采集的这一批次所有新闻的 idf
        stopdict = utils.get_stopwords()
        hotId2title = {}
        df_dict = {}
        n_doc = 0
        for line in train_titles:
            arr = line.split(' ')
            n_doc += 1
            temp_dict = {}
            #去重，因为求的 Document_Frequency，简称DF
            for tk in arr:
                # if token2id.__contains__(tk):
                temp_dict[tk] = 1
            for k, _ in temp_dict.items():
                if df_dict.__contains__(k):
                    df_dict[k] += 1
                else:
                    df_dict[k] = 1
        idf_dict = {}
        for w, df in iteritems(df_dict):
            idf_dict[w] = math.log(n_doc / float(df + 1))

        # 统计每个热点下的TF-IDF，找出每一个热点下的关键词
        for hotId, newsId_list in hotId2newsId.items():
            tf = {}
            title_words = []
            for newsId in newsId_list:
                content = raw_data[newsId].get('content')
                seg = chinese_segment.get_segment(content)
                for word in seg.split(' '):
                    if not stopdict.__contains__(word) and word.strip().__len__() > 1:
                        if tf.__contains__(word):
                            tf[word] += 1
                        else:
                            tf[word] = 1
            tf_idf = {}
            for word, freq in tf.items():
                if not idf_dict.__contains__(word):
                    tf_idf[word] = 0
                else:
                    tf_idf[word] = freq * idf_dict[word]
            rs = sorted(iteritems(tf_idf), key=lambda d: d[1], reverse=True)
            if len(rs) >= 3:
                title_words = [rs[0][0], rs[1][0], rs[2][0]]
            else:
                for r in rs:
                    title_words.append(r[0])
            hotId2title[hotId] = title_words

        """依据每个热点的新闻数据做统计分析"""
        """order, hotScore, category, totalComments, totalLikes, siteCoverage, area, totalNews"""
        hot_information_data = []  # [{"hotId": 1, "order": 1, "title": "apple"},{...}]
        update = []
        insert_count = 0
        for hotId, newsId_list in hotId2newsId.items():
            # firstFindSite, lastDays
            keywords = hotId2title[hotId]
            news_count = len(newsId_list)
            totalComments = 0
            totalLikes = 0

            category_dict = {}
            site_set = set()
            local_area_count = 0
            earliestFindTime = "2049"
            firstFindSite = ""

            max_title = ""  # 得分最高的新闻的标题
            max_score = -1  # 新闻最高得分
            second_title = ""  # 得分第二高的新闻的标题
            second_score = -1  # 新闻第二得分
            for newsId in newsId_list:
                news_category = raw_data[newsId].get('categoryName')
                news_comments = raw_data[newsId].get('commentCount')
                news_likes = raw_data[newsId].get('joinCount')
                news_site = raw_data[newsId].get('siteName')
                news_source_site =raw_data[newsId].get('sourceSiteName')
                news_publishDateTime = raw_data[newsId].get('publishDateTime')
                news_area = raw_data[newsId].get('siteTypeName')
                news_title = raw_data[newsId].get('title')

                if category_dict.__contains__(news_category):
                    category_dict[news_category] += 1
                else:
                    category_dict[news_category] = 1
                if news_area == u"本地":
                    local_area_count += 1
                # 找到最早的新闻时间
                if news_publishDateTime < earliestFindTime:
                    earliestFindTime = news_publishDateTime
                    if news_source_site is None or news_source_site == '':
                        firstFindSite = news_site
                    else:
                        firstFindSite = news_source_site
                temp_score = 0
                if news_comments is not None and news_comments.isdigit():
                    totalComments += int(news_comments)
                    temp_score += int(news_comments) * 0.6
                if news_likes is not None and news_likes.isdigit():
                    totalLikes += int(news_likes)
                    temp_score += int(news_likes) * 0.4
                # 找到得分最高的两篇新闻的标题
                if temp_score > max_score:
                    second_score = max_score
                    second_title = max_title
                    max_score = temp_score
                    max_title = news_title
                else:
                    if temp_score > second_score:
                        second_score = temp_score
                        second_title = news_title
                site_set.add(news_site)
            title = [max_title, second_title]
            rs = sorted(iteritems(category_dict), key=lambda d: d[1], reverse=True)  # 类别统计排序
            category = rs[0][0]  # 取新闻最多的分类
            all_site_number = 7.  # 总的网站数
            siteCoverage = round(len(site_set) / all_site_number, 2)
            if (float(local_area_count)/news_count) > 0.5:
                area = "local"
            else:
                area = "global"
            if tag=='weibo':
                hotScore = 100*(totalComments+totalLikes)
            else:
                hotScore = 0.8 * totalComments + 1.0 * totalLikes + news_count*siteCoverage

            if tag=='weibo':
                firstFindSite = '微博'
            elif tag == 'weixin':
                firstFindSite = '微信'

            hotScore = round(math.log(hotScore+1), 2)

            if hotScore>30:
                hotScore = 20
            elif hotScore<30 and hotScore>20:
                hotScore = 19.8

            #统计已有热点新闻条数
            news_count_old = 0
            if hotScore>5 and hotId in old_hotId_words:
                news_count_old = utils.get_newsId_count(hotId)
                print 'old total news:%s' % news_count_old

            hot_data_wrap = {"hotId": hotId,  "hotScore": hotScore, "title": title,
                             "category": category, "totalComments": totalComments,
                             "totalLikes": totalLikes, "siteCoverage": siteCoverage,
                             "area": area, "keywords": keywords, "totalNews": news_count+news_count_old,
                             "firstFindSite": firstFindSite,"datatagcategory":tag,"lastDays":1}
            #热点发现时间
            if hotId in old_hotId_words:
                update.append(1)
            else:
                hot_data_wrap.setdefault("findTime", utils.generateTime())
                update.append(0)
                insert_count += 1

            hot_data_wrap.setdefault("lastUpdate", utils.generateTime())
            hot_information_data.append(hot_data_wrap)
        print "insert_count/operation_count:", insert_count, '/', hot_information_data.__len__()
        utils.write2hot_info(hot_information_data, update)
        print "write to hot_info done!>>>>>>>>>"

        """计算order、hotScore(满分100分)"""
        hotScore_data = utils.get_hotScore_from_hot_info('datatagcategory',tag)  # 获取hotId、hotScore
        hotScore_data = sorted(hotScore_data, key=lambda d: d["hotScore"], reverse=True)  # 按hotScore降序排序
        # max_hotScore = hotScore_data[0].get('hotScore')  # 取最大值
        # print max_hotScore
        for i in range(len(hotScore_data)):
            hotScore_data[i].setdefault("order", i + 1)
            # hotScore_data[i].__setitem__("hotScore", int(100 * hotScore_data[i].get("hotScore") / float(max_hotScore)))
        utils.update_hot_info(hotScore_data)
        print "update rank to hot_info done!>>>>>>>>>"
    else:print"empty raw data"

def separate_calculate_hots():
    '''分开计算各数据来源'''
    # raw_weibo_data,raw_website_data,raw_weixin_data = utils.get_raw_diff_category_data()
    raw_data = utils.get_raw_diff_category_data_weibo_or_weixin(u'17')
    calculate(raw_data,'weibo')
    # for i in raw_data.keys():
    #     utils.remove_basicinfo_add_f(i)
    print'weibo data processed'
    raw_data=[]
    raw_data = utils.get_raw_diff_category_data_website()
    calculate(raw_data,'website')
    # for j in raw_data.keys():
    #     utils.remove_basicinfo_add_f(j)
    print'website data processed'
    raw_data=[]
    raw_data = utils.get_raw_diff_category_data_weibo_or_weixin(u'46')
    calculate(raw_data,'weixin')
    # for k in raw_data.keys():
    #     utils.remove_basicinfo_add_f(k)
    print'weixin data processed'

if __name__ == '__main__':
    print datetime.now()
    # raw_weibo_data,raw_website_data,raw_weixin_data = utils.get_raw_diff_category_data()
    # calculate(raw_weibo_data,'weibo')
    # calculate(raw_website_data,'website')
    # calculate(raw_weixin_data,'weixin')
    separate_calculate_hots()
    print datetime.now()
