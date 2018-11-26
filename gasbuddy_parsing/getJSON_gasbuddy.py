import json
import requests
from lxml import html
import MySQLdb
import time
import logging
import sys
import os 
#from logging import handlers
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)-15s:%(name)s:%(message)s')
file_handler = logging.handlers.TimedRotatingFileHandler('/home/liu433/Project_lawrence_v1/loggings/getJSON_gasbuddy.log',when='midnight',interval=1, backupCount=10)
#file_handler = logging.FileHandler('/home/liu433/Project_lawrence_v1/loggings/getJSON_gasbuddy.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

from bs4 import BeautifulSoup


def get_json_retry(Id, ti = 0):
    for i in range(8):
        time.sleep(3)
        web_constent = requests.get('https://www.gasbuddy.com/station/' + Id)
        soup = BeautifulSoup(web_constent.text, 'html.parser')
        result_string = str(soup.find_all('script')[1]).replace(
            '<script data-react-helmet="true">window.PreloadedState = ', "").replace(';</script>', '')
        result_json = json.loads(result_string)
        try:
            result_json['fuels']['fuelsByStationId'][Id.strip()]
            print str(Id) + " works"
            return result_json
        except Exception as e:
            if i == 7 and ti == 1:
                logger.debug(Id.strip() + " failed to get ID from the Given list")
            print str(e).strip() + " " + str((i + 1))  +" times trying"
            print result_json['fuels']['fuelsByStationId'].keys()

    return str(Id).strip()

def get_json(ID, ti=0):
    web_constent = requests.get('https://www.gasbuddy.com/station/' + ID)
    soup = BeautifulSoup(web_constent.text, 'html.parser')
    result_string= str(soup.find_all('script')[1]).replace('<script data-react-helmet="true">window.PreloadedState = ' , "").replace(';</script>', '')
    result_json = json.loads(result_string)
    try:
        result_json['fuels']['fuelsByStationId'][ID.strip()]
        print ID.strip() + " : works"

    except:
        print ID.strip() + " : failed"
        result_json = get_json_retry(ID, ti)
    return result_json
