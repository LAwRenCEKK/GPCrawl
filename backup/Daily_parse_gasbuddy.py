import getJSON_gasbuddy
from getJSON_gasbuddy import *
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)-15s:%(name)s:%(message)s')
file_handler = logging.FileHandler('Daily_parse_gasbuddy.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def load_reviews(result, id):
    db = MySQLdb.connect(host="130.113.70.243", port=33066, user="liu433", password="liu433", db="gasinfo")
    cursor = db.cursor()
    for i in result['reviews']['byId'].keys():
        content = result['reviews']['byId'][i]['content']
        sentimentScore =  result['reviews']['byId'][i]['sentimentScore']
        date = result['reviews']['byId'][i]['date']
        user_name = result_json['reviews']['byId'][i]['user']["name"]
        try:
            cursor.execute('SELECT * FROM reviews2 where comment_date =%s ', (str(date),))
            entry = cursor.fetchone()
        except:
            logger.exception("Error occured during the process of fetching the data from the database: ")
            raise
        if entry == None:
            logger.info("There is one review updated for  " +id + " at "+ date )
            sql = """insert into reviews2(stationid, review, author, score, comment_date) Values ("%s","%s","%s","%f", "%s")""" \
                 % (id, content,user_name,sentimentScore,date)
            try:
                cursor.execute(sql)
                db.commit()
            except:
                db.rollback()
    db.close()
    return 0


def load_staioninfo2(result, id):
    try:
        rating = result_json['stationInfo']['stationsById'][id]['star_rating']
        phone = result_json['stationInfo']['stationsById'][id]['phone']
        name = result_json['stationInfo']['stationsById'][id]['name']
        KK = result['stationInfo']['stationsById'][id]['amenities']
        address = result_json['stationInfo']['stationsById'][id]['address']
        location = str(address['line_1']) +", " +  str(address['locality'])+ ", " +  str(address['region']) + ", " + str(address['postal_code'])
        PP = " "
        for i in KK:
            PP = PP + i + "|"

        db = MySQLdb.connect(host="130.113.70.243", port=33066, user="liu433", password="liu433", db="gasinfo")
        cursor = db.cursor()
        sql = """insert into stationinfo2(stationid, amenities, rating, contact, name, location) Values ("%s","%s","%f","%s", "%s", "%s")""" \
              % (id, str(PP), rating, phone, name, location)
        try:
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()
        db.close()
        return 0
    except:
        logger.info(id + " failed to get the feature")
        print "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        return 0



station_id = []
file = open('./Dataset.txt','r')
for line in file:
    station_id.append(line.split('|')[4].strip())
index = 0

for id in station_id:
    features = " "
    index = index + 1
    time.sleep(1)
    result_json = get_json(id)
    print type(result_json)
    if type(result_json) == dict:
        load_staioninfo2(result_json, id)
        load_reviews(result_json, id)
        logger.info('~~~~~~~~~'+ id + '~~~~~~~~~~~~~~~~~~~~~~~')
        logger.info("Stationinfo has been uploaded successfully")
        logger.info('reviews have been uploaded successfully')
    else:
        logger.info("For "+  id + " Json is not correct. Pass it for now")
    print index

print index


