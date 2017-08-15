# -*- coding: utf-8 -*-
import hot_news
import utils
from datetime import datetime
import time


print "program start at", datetime.now()

index = 0
while True:

    update_datetime = datetime.now().replace(hour=23, minute=59, second=59)

    if utils.get_data_count() > 100:
        index += 1
        print "=" * 16 + str(index) + "=" * 16 + str(datetime.now())
        if utils.time_to_update(update_datetime):
            hot_news.calculate()
            print "update_lastDays..."
            utils.update_lastDays()
            print "sleep 61 minuteszzzzzzzzzzzzzzzzzzzz" + str(datetime.now())
            time.sleep(61*60)
        else:
            hot_news.calculate()

    else:
        if utils.time_to_update(update_datetime):
            print "update_lastDalys>>>>>>>>>>>>" + str(datetime.now())
            utils.update_lastDays()
            print "sleep 61 minuteszzzzzzzzzzzzzzzzzzzz" + str(datetime.now())
            time.sleep(61*60)
        else:
            print "sleep 59 minuteszzzzzzzzzz" + str(datetime.now())
            time.sleep(59*60)

