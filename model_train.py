# -*- coding:utf-8 -*-
import codecs
import math
import datetime
import sys
from six import iteritems
from gensim.models import ldamodel
from collections import defaultdict
import numpy as np
import utils

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
def get_tf(text):
    stopdict = utils.get_stopwords()
    tf = {}
    # seg = utils.chinese_segment.get_segment(text)
    word_count = 1
    for word in text.split(' '):
        if not stopdict.__contains__(word) and word.strip().__len__() > 1:
            word_count += 1
            if tf.__contains__(word):
                tf[word] += 1
            else:
                tf[word] = 1
    rs = sorted(iteritems(tf), key=lambda d: d[1], reverse=True)
    rs = [(a[0], a[1] / float(word_count)) for a in rs]
    if rs.__len__() > 20:
        return rs[:20]
    else:
        return rs
def get_tf_from_list(text_list):
    text_count = text_list.__len__()
    tf = {}
    for text in text_list:
        for w in text[1]:
            word = w[0]
            freq = w[1]
            if tf.__contains__(word):
                tf[word] += freq
            else:
                tf[word] = freq
    rs = sorted(iteritems(tf), key=lambda d: d[1], reverse=True)
    rs = [(a[0], a[1] / float(text_count)) for a in rs]
    return rs
def first_cluster(train_data, entityId_list, publishDateTime_list):
    """result = [[(entityId, text, time)],[]]"""
    if train_data.__len__() != entityId_list.__len__():
        print "err"
        sys.exit()
    data_count = train_data.__len__()
    result = []
    not_use = []
    count = 0
    for i in range(data_count-1):
        if train_data[i] in not_use:
            continue
        else:
            similar = [(entityId_list[i], train_data[i], publishDateTime_list[i])]
            not_use.append(train_data[i])
            for j in range(i+1, data_count):
                if train_data[j] in not_use:
                    continue
                else:
                    cos = calculate_similarity(train_data[i], train_data[j])
                    if cos > 0.55:
                        similar.append((entityId_list[j], train_data[j], publishDateTime_list[j]))
                        not_use.append(train_data[j])
            result.append(similar)
            count += similar.__len__()
    # 处理最后一个数据
    if train_data[data_count-1] not in not_use:
        result.append([(entityId_list[data_count-1], train_data[data_count-1], publishDateTime_list[data_count-1])])
    return result

