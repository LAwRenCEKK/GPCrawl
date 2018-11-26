#!/usr/bin/env python -B
import getJSON_gasbuddy
from getJSON_gasbuddy import *
import logging
import sys
from bs4 import BeautifulSoup

sys.dont_write_bytecode = True
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)-15s:%(name)s:%(message)s')
file_handler = logging.FileHandler('/home/liu433/Project_lawrence_v1/loggings/Daily_parse_gasbuddy.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def load_reviews(result, id):
#    db = MySQLdb.connect(host="172.17.54.91", port=33066, user="liu433", password="liu433", db="gasinfo")
    db = MySQLdb.connect("localhost","liu433", "liu433","gasinfo") 
    cursor = db.cursor()
    for i in result['reviews']['byId'].keys():
        content = result['reviews']['byId'][i]['content']
        sentimentScore =  result['reviews']['byId'][i]['sentimentScore']
        date = result['reviews']['byId'][i]['date']
        user_name = result_json['reviews']['byId'][i]['user']["name"]
#        print content
        try:
            cursor.execute('SELECT * FROM reviews2 where comment_date =%s or author =%s', (str(date),user_name,))
            entry = cursor.fetchone()
        except:
            logger.exception("Error occured during the process of fetching the data from the database: ")
            raise
        if entry == None:
#            logger.info("There is one review updated for  " +id + " at "+ date )
            sql = """insert into reviews2(stationid, review, author, score, comment_date) Values ("%s","%s","%s","%f", "%s")""" \
                 % (id, content,user_name,sentimentScore,date)
            try:
                cursor.execute(sql)
                db.commit()
            except:
                db.rollback()
    db.close()
    return 0


def load_staioninfo2(result, id, CID):
    try:
        rating = result['stationInfo']['stationsById'][id]['star_rating']
        phone = result['stationInfo']['stationsById'][id]['phone']
        name = result['stationInfo']['stationsById'][id]['name']
        KK = result['stationInfo']['stationsById'][id]['amenities']
        address = result['stationInfo']['stationsById'][id]['address']
        location = str(address['line_1']) +", " +  str(address['locality'])+ ", " +  str(address['region']) + ", " + str(address['postal_code'])
        PP = " "
        for i in KK:
            PP = PP + i + "|"
        print CID  + "Station info on tracj"
 #       db = MySQLdb.connect(host="172.17.54.91", port=33066, user="liu433", password="liu433", db="gasinfo")
        db = MySQLdb.connect("localhost","liu433", "liu433","gasinfo")
        cursor = db.cursor()
        sql = """insert into stationinfo2(stationid,amenities, rating, contact, name, location, cid)
                 Values ("%s","%s","%f","%s", "%s", "%s", "%s") on DUPLICATE KEY UPDATE amenities = '%s', rating = '%f', contact = '%s', name = '%s', location = '%s', cid = '%s' """ \
              % (id, str(PP), rating, phone, name, location, CID,str(PP), rating, phone, name, location, CID)
        try:
            cursor.execute(sql)
            db.commit()
        except Exception as e:
            print e
            db.rollback()
        db.close()
        return 0
    except Exception as r:
        print r
        logger.info(id + " failed to get the feature")
        print "~~~~~~~~~~~~~~~~~~~~~~~~~~~ failed for station infor"
        return 0


def load_priceinfo3(id1,id, times = 0):
    dic = {}
    web_constent = requests.get('https://www.gasbuddy.com/station/' + id)
    soup = BeautifulSoup(web_constent.text, 'html.parser')
    content = soup.find_all("div", class_="style__panel___sYmkB style__white___WKe__ style__bordered___1qXL7 style__padded___3c3v6 style__rounded___gFx-T styles__pricePanel___3CZK0")
    if len(content) == 0:
        return 0
    else:
        ty =[]
        pr =[]
        for i in content:
            typ = i.find("h4", class_="styles__fuelTypeHeader___2RL00").get_text()
            ty.append(str(typ))
            try:
                pri = i.find('h1', class_="style__header1___1jBT0 style__header___onURp styles__price___1wJ_R").get_text()
                pr.append(float(str(pri[0:len(pri)-1]))*0.01)
            except Exception as a:
                pr.append("None")
    for i in range(len(ty)):
        dic.update({ty[i]:pr[i]})
    db = MySQLdb.connect("localhost","liu433", "liu433","gasinfo")
    cursor = db.cursor()
    pricel = []
    for key, value in dic.items():
        pricel.append(value*100.0 if value != "None" else 0.0)

    sql = """insert into nearby_stationprice(stationid,nearby_stationid, regular, midgrade, premium, diesel) values ("%s","%s","%0.2f","%0.2f","%0.2f","%0.2f")"""\
          % (id1,id,pricel[0], pricel[1], pricel[2], pricel[3])
    try:
    # Execute the SQL command
        cursor.execute(sql)
    # Commit your changes in the database
        db.commit()
    except:
    # Rollback in case there is any error
        db.rollback()
    # disconnect from server
    db.close()



def get_nearbystation_info(id, json):
    try:
        list_nearby = json['nearby']['stationsById'][id]
        for i in list_nearby:
            load_priceinfo3(id,str(i))
    except:
        return

#load_staioninfo2
#load_staioninfo2(get_json('15235'),'15235', 'something')

station_id = []
cid = []
file = open('/home/liu433/Project_lawrence_v1/Dataset.txt','r')
for line in file:
    station_id.append(line.split('|')[4].strip())
    cid.append(line.split('|')[1].strip())
index = 0
for id in station_id:
    features = " "
    index = index + 1
    time.sleep(1)
    result_json = get_json(id)
    get_nearbystation_info(id, result_json)
    if type(result_json) == dict:
        print "good for " + str(index) 
        load_staioninfo2(result_json, id, cid[index-1])
        load_reviews(result_json, id)
        logger.info('~~~~~~~~~'+ id + '~~~~~~~~~~~~~~~~~~~~~~~')
        logger.info("Stationinfo has been uploaded successfully")
        logger.info('reviews have been uploaded successfully')
    else:
        logger.info("For "+  id + " Json is not correct. Pass it for now")
    print index



