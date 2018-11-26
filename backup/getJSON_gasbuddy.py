import json
import requests
from lxml import html
import MySQLdb
import time
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)-15s:%(name)s:%(message)s')
file_handler = logging.FileHandler('getJSON_gasbuddy.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def get_json(ID, times = 0):
    logger.info(ID + "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print ID
    times = times + 1
    web_constent = requests.get('https://www.gasbuddy.com/station/' + ID)
    tree = html.fromstring(web_constent.content)
    info = tree.xpath('//script[@data-react-helmet="true"]/text()')
    if len(info) == 0:
        if times < 8:
            logger.info("The format of the JSON file from the website is not approprite. This is the %s times trying" %(times))
            return get_json(ID,times)
        else:
            logger.info("Failed to get the right information from the website: " + ID)
            return -1
    else:
        for i in info:
            if i[0:23] == "window.PreloadedState =":
                length = len(info[1])
                result_string = info[1][24:length - 1]
                result_json = json.loads(result_string.encode('utf-8'), encoding='utf-8')
                try:
                    logger.info("Checking: the correctness of the result json")
                    result_json['stationInfo']['stationsById'][ID]
                    logger.info("Json has the complete information")
                    return result_json
                except KeyError,e:
                    logger.info("Json dosen't have the complete information")
                    logger.exception("Traceback: ")
                    if e.message == ID and times < 5:
                        logger.info("The result json doesn't have complete information, trying again %s" %(times))
                       # print result_json['stationInfo']['stationsById'].keys()
                        #time.sleep(4)
                        return get_json(ID, times)
                    else:
                        logger.info("something else is wrong please check")
                        return e
        return -1

