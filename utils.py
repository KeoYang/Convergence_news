# -*- coding: utf-8 -*-
from __future__ import division
import pymongo
import uuid
import math
import time
import codecs
from datetime import datetime

# cli = pymongo.MongoClient('10.9.201.20', 27017)
cli = pymongo.MongoClient('localhost', 9666)
db = cli.guangdian
db.authenticate("gduser_dev", "Password")

def time_to_update(update_datetime):
    if (datetime.now() - update_datetime).seconds<280:
        return True
    else:return False

def get_data_count():
    db = cli.guangdian
    return db.c_gd_news_basicinfo_add.find().count()

def get_newsId_count(hotId):
    db = cli.guangdian
    num = db.newsId.find({'hotId': hotId}).count()
    return num

def get_stopwords():
    stoplist = []
    fnobj = codecs.open("stopwords", 'r', 'utf-8')
    while True:
        line = fnobj.readline()
        if line:
            stoplist.append(line.strip())
        else:
            break
    stopdict = {}
    for word in stoplist:
        stopdict[word] = 1
    return stopdict


def get_raw_data():
    db = cli.guangdian
    """:return [{},{}]"""
    index = 0
    raw_data = {}
    # raw_data_list = []
    for mongo_file in db.c_gd_news_basicinfo.find():
        if index >= 500:
            break
        entityId = mongo_file.get('entityId')
        siteName = mongo_file.get('siteName')
        sourceSiteName = mongo_file.get('sourceSiteName')
        siteTypeName = mongo_file.get('siteTypeName')
        categoryName = mongo_file.get('categoryName')
        title = mongo_file.get('title')
        content = mongo_file.get('content')
        publishDateTime = mongo_file.get('publishDateTime')
        getDateTime = mongo_file.get('getDateTime')
        commentCount = mongo_file.get('commentCount')
        joinCount = mongo_file.get('joinCount')
        data_dict = {"siteName": siteName, "title": title, "sourceSiteName": sourceSiteName,
                     "siteTypeName": siteTypeName, "categoryName": categoryName,
                     "publishDateTime": publishDateTime, "getDateTime": getDateTime,
                     "commentCount": commentCount, "joinCount": joinCount,
                     "content": content}
        raw_data[entityId] = data_dict
        index += 1
    print "raw_data_number:" + str(index)
    return raw_data

def get_raw_diff_category_data():
    db = cli.guangdian
    """:return [{},{}]"""
    index = 0
    raw_weibo_data = {}
    raw_website_data = {}
    raw_weixin_data = {}

    for mongo_file in db.c_gd_news_basicinfo_add.find():
        entityId = mongo_file.get('entityId')
        siteName = mongo_file.get('siteName')
        sourceSiteName = mongo_file.get('sourceSiteName')
        siteTypeName = mongo_file.get('siteTypeName')
        categoryName = mongo_file.get('categoryName')
        title = mongo_file.get('title')
        content = mongo_file.get('content')
        publishDateTime = mongo_file.get('publishDateTime')
        getDateTime = mongo_file.get('getDateTime')
        commentCount = mongo_file.get('commentCount')
        joinCount = mongo_file.get('joinCount')
        siteId = mongo_file.get('siteId')
        data_dict = {"siteName": siteName, "siteId":siteId, "title": title, "sourceSiteName": sourceSiteName,
                     "siteTypeName": siteTypeName, "categoryName": categoryName,
                     "publishDateTime": publishDateTime, "getDateTime": getDateTime,
                     "commentCount": commentCount, "joinCount": joinCount,
                     "content": content}
        if int(siteId) == 17:
            raw_weibo_data[entityId] = data_dict
        elif int(siteId) == 100:
            raw_weixin_data[entityId] = data_dict
        else:
            raw_website_data[entityId] = data_dict

        index += 1
    print "raw_data_number:" + str(index)
    return raw_weibo_data,raw_website_data,raw_weixin_data

def get_raw_diff_category_data_weibo_or_weixin(tag):
    db = cli.guangdian
    """:return [{},{}]"""
    index = 0
    raw_data = {}

    for mongo_file in db.c_gd_news_basicinfo_add.find({'siteId': tag}).batch_size(100):
        entityId = mongo_file.get('entityId')
        siteName = mongo_file.get('siteName')
        sourceSiteName = mongo_file.get('sourceSiteName')
        siteTypeName = mongo_file.get('siteTypeName')
        categoryName = mongo_file.get('categoryName')
        title = mongo_file.get('title')
        content = mongo_file.get('content')
        publishDateTime = mongo_file.get('publishDateTime')
        getDateTime = mongo_file.get('getDateTime')
        commentCount = mongo_file.get('commentCount')
        joinCount = mongo_file.get('joinCount')
        siteId = mongo_file.get('siteId')
        data_dict = {"siteName": siteName, "siteId":siteId, "title": title, "sourceSiteName": sourceSiteName,
                     "siteTypeName": siteTypeName, "categoryName": categoryName,
                     "publishDateTime": publishDateTime, "getDateTime": getDateTime,
                     "commentCount": commentCount, "joinCount": joinCount,
                     "content": content}
        raw_data[entityId] = data_dict

        index += 1
    print "raw_data_number:" + str(index)
    return raw_data

def get_raw_diff_category_data_website():
    db = cli.guangdian
    """:return [{},{}]"""
    index = 0
    raw_data = {}

    for mongo_file in db.c_gd_news_basicinfo_add.find():
        siteId = mongo_file.get('siteId')
        if int(siteId) == 17 or int(siteId) == 46:
            continue
        else:
            entityId = mongo_file.get('entityId')
            siteName = mongo_file.get('siteName')
            sourceSiteName = mongo_file.get('sourceSiteName')
            siteTypeName = mongo_file.get('siteTypeName')
            categoryName = mongo_file.get('categoryName')
            title = mongo_file.get('title')
            content = mongo_file.get('content')
            publishDateTime = mongo_file.get('publishDateTime')
            getDateTime = mongo_file.get('getDateTime')
            commentCount = mongo_file.get('commentCount')
            joinCount = mongo_file.get('joinCount')
            data_dict = {"siteName": siteName, "siteId":siteId, "title": title, "sourceSiteName": sourceSiteName,
                         "siteTypeName": siteTypeName, "categoryName": categoryName,
                         "publishDateTime": publishDateTime, "getDateTime": getDateTime,
                         "commentCount": commentCount, "joinCount": joinCount,
                         "content": content}
            raw_data[entityId] = data_dict

            index += 1
    print "raw_data_number:" + str(index)
    return raw_data


def get_hotId_words(tag):
    db = cli.guangdian
    id_words = {}
    for mongo_file in db.topics.find({'datatagcategory': tag}).batch_size(100):
        hotId = mongo_file.get('hotId')
        words = mongo_file.get("words")
        id_words[hotId] = words
    return id_words


def get_lastDays_from_hot_info(field,fieldvalue):
    db = cli.guangdian
    mongo_file = db.hot_info.find_one({field: fieldvalue})
    lastdays = mongo_file.get('lastDays')
    # find_time = mongo_file.get('lastDays')
    if lastdays:
        return lastdays
    else:return 1

def get_lastDay_from_hot_info():
    db = cli.guangdian
    hot_info = []
    for mongo_file in db.hot_info.find().batch_size(100):
        hotId = mongo_file.get('hotId')
        # lastDays = mongo_file.get('lastDays')
        lastUpdate = mongo_file.get('lastUpdate')
        findTime = mongo_file.get('findTime')
        # hot_info.append({"hotId": hotId, "lastDays": lastDays, "lastUpdate": lastUpdate,"findTime":findTime})
        hot_info.append({"hotId": hotId, "lastUpdate": lastUpdate,"findTime":findTime})
    return hot_info


def write2hotId_words(hotId, words,tags):
    db = cli.guangdian
    db.topics.insert({'hotId': hotId, 'words': words,'datatagcategory':tags})

def write2hotId_newsId(hotId_newsId):
    db = cli.guangdian
    for record in hotId_newsId:
        db.newsId.insert({"hotId": record[0], "newsId": record[1],
                              "publishDateTime": record[2], "generateTime": record[3]})

def write2hotId_newsId_multi(hotId_newsId):
    db = cli.guangdian
    for record in hotId_newsId:
        db.newsId.insert({"hotId": record[0], "newsId": record[1],
                                "publishDateTime": record[2], "generateTime": record[3],"datatagcategory":record[4]})


def write2hot_info(hot_info, update):
    db = cli.guangdian
    for i in range(len(hot_info)):
        if update[i] == 1:
            # print "update"
            db.hot_info.update({"hotId": hot_info[i]["hotId"]}, {"$set": hot_info[i]})
        else:
            # print "insert"
            db.hot_info.insert(hot_info[i])


def write2hot_operation(hot_operation):
    db = cli.guangdian
    for operation in hot_operation:
        db.hot_operation.insert(operation)


def update_hot_info(hotScore_data):
    db = cli.guangdian
    for hotScore_stuff in hotScore_data:
        db.hot_info.update({"hotId": hotScore_stuff["hotId"]}, {"$set": hotScore_stuff})

def get_hotScore_from_hot_info(field,fieldvalue):
    db = cli.guangdian
    hot_info = []
    for mongo_file in db.hot_info.find({field: fieldvalue}).batch_size(100):
        hotId = mongo_file.get('hotId')
        hotScore = mongo_file.get('hotScore')
        hot_info.append({"hotId": hotId, "hotScore": hotScore})
    return hot_info

def get_hotScore_from_hot_info_time_decay(field,fieldvalue):
    db = cli.guangdian
    hot_info = []
    now_date = datetime.now()
    for mongo_file in db.hot_info.find({field: fieldvalue}).batch_size(100):
        hotId = mongo_file.get('hotId')
        hotScore = mongo_file.get('hotScore')
        findTime = mongo_file.get('findTime')
        lastUpdate = mongo_file.get('lastUpdate')
        lastUpdate = datetime.strptime(lastUpdate, "%Y-%m-%d %H-%M")
        lastDays = 1
        diffd_days = (now_date - lastUpdate).days
        if int(diffd_days) == 0 and (now_date - lastUpdate).seconds<900:
            findTime = datetime.strptime(findTime, "%Y-%m-%d %H-%M")
            lastDays  = (now_date - findTime).days+1
        if int(diffd_days) != 0:
            hotScore = score_attenuation(diffd_days,hotScore,lastDays)
            if hotScore>30:
                hotScore = 20
            elif hotScore<30 and hotScore>20:
                hotScore = 19.8
        hot_info.append({"hotId": hotId, "hotScore": hotScore,"lastDays":lastDays})
    return hot_info


def update_lastDays():
    """更新lastDays"""
    lastDays_data = get_lastDay_from_hot_info()
    now_date = datetime.now()
    to_be_update = []
    for lastDays_stuff in lastDays_data:
        hotId = lastDays_stuff.get('hotId')
        lastUpdate = lastDays_stuff.get('lastUpdate')
        # lastDays = lastDays_stuff.get('lastDays')
        findTime = lastDays_stuff.get('findTime')
        lastUpdate = datetime.strptime(lastUpdate, "%Y-%m-%d %H-%M")
        if (now_date - lastUpdate).days == 0 and (now_date - lastUpdate).seconds<900:
            findTime = datetime.strptime(findTime, "%Y-%m-%d %H-%M")
            to_be_update.append({"hotId": hotId, "lastDays": (now_date - findTime).days+1})
    update_hot_info(to_be_update)


def remove_basicinfo_add():
    db = cli.guangdian
    return db.c_gd_news_basicinfo_add.remove({})

def remove_basicinfo_add_f(entityId):
    db = cli.guangdian
    # for i in entityId:
    db.c_gd_news_basicinfo_add.remove({"entityId": entityId})


def generateTime():
    return time.strftime("%Y-%m-%d %H-%M", time.localtime())


def generateId():
    return uuid.uuid1().__str__()


def calculate_similarity(new_topic, old_hot_topic):
    d1 = {}
    d2 = {}
    mo1 = 0.0
    mo2 = 0.0
    for (word, prob1) in new_topic:
        d1[word] = prob1
        mo1 += (prob1 * prob1)
    for (word, prob2) in old_hot_topic:
        d2[word] = prob2
        mo2 += (prob2 * prob2)
    mo1 = math.sqrt(mo1)
    mo2 = math.sqrt(mo2)
    vec_product = 0
    for word, prob in d1.items():
        if word in d2:
            vec_product += (prob * d2[word])
    if mo1 == 0.0 or mo2 == 0.0:
        return 0.0
    cos = vec_product / (mo1 * mo2)
    return cos


def get_hot_id(new_topic, old_hotId_words):
    '''比较新旧热点，若该新热点在老热点之中，则返回该老热点id，否则，生成一个新的热点id赋予该新热点'''
    similar_id = ""
    max_similar_value = 0
    if old_hotId_words is None:
        return generateId()
    for hotId, words in old_hotId_words.items():
        similar_value = calculate_similarity(new_topic, words)
        if similar_value > max_similar_value:
            max_similar_value = similar_value
            similar_id = hotId
    if max_similar_value > 0.6:
        return similar_id , False
    else:
        new_id = generateId()
        return new_id , True
# def get_tf(text):
#     stopdict = get_stopwords()
#     tf = {}
#     seg = chinese_segment.get_segment(text)
#     word_count = 1
#     for word in seg.split(' '):
#         if not stopdict.__contains__(word) and word.strip().__len__() > 1:
#             word_count += 1
#             if tf.__contains__(word):
#                 tf[word] += 1
#             else:
#                 tf[word] = 1
#     rs = sorted(iteritems(tf), key=lambda d: d[1], reverse=True)
#     rs = [(a[0], round(a[1] / float(word_count), 2)) for a in rs]
#     if rs.__len__() > 10:
#         return rs[:10]
#     else:
#         return rs
# def get_tf_from_list(text_list):
#     text_count = text_list.__len__()
#     tf = {}
#     for text in text_list:
#         for w in text[1]:
#             word = w[0]
#             freq = w[1]
#             if tf.__contains__(word):
#                 tf[word] += freq
#             else:
#                 tf[word] = freq
#     rs = sorted(iteritems(tf), key=lambda d: d[1], reverse=True)
#     rs = [(a[0], round(a[1] / float(text_count), 2)) for a in rs]
#     if rs.__len__() > 20:
#         return rs[:20]
#     else:
#         return rs

def get_hot_info_remark_hotscore(field,fieldvalue):
    db = cli.guangdian
    hot_info = []
    for mongo_file in db.hot_info.find({field: fieldvalue}).batch_size(100):
        hotId = mongo_file.get('hotId')
        lastUpdate = mongo_file.get('lastUpdate')
        hotScore = mongo_file.get('hotScore')
        lastDays = mongo_file.get('lastDays')
        hot_info.append({"hotId": hotId, "lastDays": lastDays, "lastUpdate": lastUpdate,"hotScore":hotScore})

    return hot_info

def remark_hotScore(hot_info):
    re_hotScore={}
    if not hot_info=={}:
        for info in hot_info:
            lastDays = info['lastDays']
            lastUpdate = info['lastUpdate']
            hotScore = info['hotScore']
            hotId = info['hotId']
            now_date = datetime.now()
            lastUpdate = datetime.strptime(lastUpdate, "%Y-%m-%d %H-%M")
            diffd_days = (now_date - lastUpdate).days
            if int(diffd_days) == 0:
                if hotScore>30:
                    hotScore = 20
                elif hotScore<30 and hotScore>20:
                    hotScore = 19.8
                re_hotScore[hotId] = hotScore
            else:
                tmp_score = score_attenuation(diffd_days,hotScore,lastDays)
                if tmp_score>30:
                    tmp_score = 20
                elif tmp_score<30 and tmp_score>20:
                    tmp_score = 19.8
                re_hotScore[hotId] = tmp_score

    return re_hotScore

def update_remark_hotscore(find_field,find_fieldvalue,insert_field,insert_fieldvalue):
    #批量更新热点分数
    db = cli.guangdian
    db.hot_info.update({find_field: find_fieldvalue}, {"$set": {insert_field: insert_fieldvalue}})

def score_attenuation(diffd_days,hotScore,lastDays):

    diff_days=1
    if hotScore >0:
        # diff_days = math.floor((now_date - lastUpdate).days/2)+1
        diff_days = math.floor(diffd_days/2)+1

    hotScore = (1/diff_days)*hotScore+lastDays*0.6
    hotScore = round(hotScore, 2)
    return hotScore

def delete_obsolete_hot(hotId):
    db = cli.guangdian
    db.hot_info.remove({"hotId": hotId})
    db.newsId.remove({"hotId": hotId})
    db.topics.remove({"hotId": hotId})

def delete_bact_hot(hotId):
    db = cli.guangdian
    db.hot_info.remove({'hotId': {'$in': hotId}})
    db.newsId.remove({"hotId": {'$in': hotId}})
    db.topics.remove({"hotId": {'$in': hotId}})

def get_obsolete_hot():
    db = cli.guangdian
    hot_info = []
    for mongo_file in db.hot_info.find().batch_size(100):
        hotId = mongo_file.get('hotId')
        lastUpdate = mongo_file.get('lastUpdate')
        lastDays = mongo_file.get('lastDays')
        hotScore = mongo_file.get('hotScore')

        now_date = datetime.now()
        lastUpdate = datetime.strptime(lastUpdate, "%Y-%m-%d %H-%M")
        if lastDays>30:
            hot_info.append(hotId)
            continue
        if lastDays<2 and (now_date-lastUpdate).days>7 :
            hot_info.append(hotId)
            continue
        if hotScore<1 and lastDays<2 and (now_date-lastUpdate).days>2:
            hot_info.append(hotId)
            continue
    return hot_info

def final_resort_hotscore(tags):
    '''tags=['weibo','website','weixin']'''
    if not tags==[]:
        for tag in tags:

            hotScore_data = get_hot_info_remark_hotscore('datatagcategory',tag)
            re_hotScore = remark_hotScore(hotScore_data)
            find_field = 'hotId'
            insert_field='hotScore'
            for (hotIdvalue,hotScorevalue) in  re_hotScore.items():
                update_remark_hotscore(find_field,hotIdvalue,insert_field,hotScorevalue)

            hotScore_data = get_hotScore_from_hot_info('datatagcategory',tag)  # 获取hotId、hotScore
            hotScore_data = sorted(hotScore_data, key=lambda d: d["hotScore"], reverse=True)  # 按hotScore降序排序
            for i in range(len(hotScore_data)):
                hotScore_data[i]["order"] = i + 1
            update_hot_info(hotScore_data)
    else:print('non value input')

if __name__ == '__main__':

    # get_lastDays_from_hot_info('hotId','fe150a0f-20c4-11e7-8025-ac162d11762c')
    get_newsId_count()
