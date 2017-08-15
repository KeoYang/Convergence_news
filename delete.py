

import utils
from datetime import datetime


# update_datetime = datetime.now().replace(hour=10, minute=13, second=0)
# print update_datetime
# if utils.time_to_update(update_datetime):
#     print ' begining delete'
#     obsolete_hot = utils.get_obsolete_hot()
#     print len(obsolete_hot)
#     if obsolete_hot != []:
#         print 'process'
#         utils.delete_bact_hot(obsolete_hot)
#
#     print 'over'


obsolete_hot = utils.get_obsolete_hot()
print len(obsolete_hot)
if obsolete_hot != []:
    print 'process'
    utils.delete_bact_hot(obsolete_hot)