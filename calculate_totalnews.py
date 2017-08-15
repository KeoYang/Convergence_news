# -*- utf-8 -
from __future__ import division
import pymongo
'''
update totalNews
'''
# cli = pymongo.MongoClient('10.9.201.190', 27017)
cli = pymongo.MongoClient('localhost', 9666)
db = cli.guangdian
db.authenticate("gduser_dev10", "Passw0rd&234$")

def get_hot_info():
    db = cli.guangdian
    hot_info = []
    for mongo_file in db.hot_info.find().batch_size(100):
        hotId = mongo_file.get('hotId')
        hot_info.append(hotId)
    return hot_info

def get_hotId_totalnews(hot_info):
    db = cli.guangdian
    # total={}
    i=1
    for info in hot_info:
        num = db.newsId.find({'hotId': info}).count()
        # total[info] = num
        print '%s' %i ,'total hot numbers %s' %len(hot_info)
        i = i + 1
        if num == 1:
            print ' >>>> pass one'
            continue
        else:
            print' >>>>>>>>>> insert one'
            db.hot_info.update({"hotId": info}, {"$set": {"totalNews": num}})

    # return total

def update_hot_info(totalnews):
    db = cli.guangdian
    for hotId,totalnum in totalnews.items():
        print hotId,' ',totalnum
        db.hot_info.update({"hotId": hotId}, {"$set": {"totalNews": totalnum}})


if __name__ == '__main__':

    hot_info = get_hot_info()
    print 'hot ID over'

    get_hotId_totalnews(hot_info)
    # print 'count over'
    # update_hot_info(totalnews)
    print 'over'
