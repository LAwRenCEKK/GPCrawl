from getJSON_gasbuddy import *
import getJSON_gasbuddy
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)-15s:%(name)s:%(message)s')
file_handler = logging.FileHandler('Hourly_parse_gasbuddy.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def load_priceinfo(result, id):
    try:
        regular = result['fuels']['fuelsByStationId'][id]['Regular']['prices'][0]['price']
        premium = result['fuels']['fuelsByStationId'][id]['Premium']['prices'][0]['price']
        midgrade = result['fuels']['fuelsByStationId'][id]['Midgrade']['prices'][0]['price']
        diesel = result['fuels']['fuelsByStationId'][id]['Diesel']['prices'][0]['price']
        re_dic = {'regular': regular, 'premium': premium, 'midgrade': midgrade, 'diesel': diesel}
    except KeyError, e:
        print e.message
        if e.message == id:
            logger.exception("Critical error which need to be investigated later")
            raise
        elif e.message == 'Diesel':
            re_dic = {'regular': regular, 'premium': premium, 'midgrade': midgrade, 'diesel': None}
            logger.info('There is no price info for Diesel')
        else:
            logger.info("Other issues which are needed to investigate")
            raise

    db = MySQLdb.connect(host="130.113.70.243", port=33066, user="liu433", password="liu433", db="gasinfo")
    cursor = db.cursor()
    sql = """insert into priceinfo2(stationid, premium, midgrade, regular,diesel) Values ("%s","%s","%s","%s","%s")""" \
          % (id, re_dic['premium'], re_dic['midgrade'], re_dic['regular'], re_dic['diesel'])
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()
    db.close()




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
    if type(result_json) == dict:
        load_priceinfo(result_json, id)
        logger.info("The priceinfo for " + id + " has been uploaded successfully")
        print "The priceinfo for " + id + " has been uploaded successfully"
    else:
        logger.info("Could get the right JSON file anyways for "+ id+ " .Pass it for now")
