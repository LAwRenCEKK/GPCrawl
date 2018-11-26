#import gp_crawl
from gp_crawl import * 
import googlemaps
import MySQLdb
import requests
import logging
#import lxml

#Set the working directory
working_dir = '/home/liu433/Project_lawrence_v1'
os.chdir(working_dir)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)-15s:%(name)s:%(message)s')
file_handler = logging.FileHandler('Hourly_parse.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def load_busyhour_data(gasinfo, cid):
	week_list = ["Sun","Mon","Tue","Wed","Thur","Fri","Sat"]
	hour_list = [4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,0,1,2,3]
	db = MySQLdb.connect("localhost","liu433","liu433","gasinfo")
	cursor = db.cursor()
	# the indexs for week_list and hour_list
	week_count = 0
	realtime = 0.0

	# Get access for each day in one week
	for weekdays in gasinfo['busyinfo']:
		hour_count = 0
		# Get access for hourly information for each day
		for daily in weekdays:
			#check if the information of current hour include the realtime info
			if type(daily) == unicode:
				realtime = int(daily[10:12])*0.01
				frequency = int(daily[-9:-7])*0.01
				sql = """INSERT INTO busyhour (stationid, weekday, hour, freq, realtimeinfo) VALUES ('%s','%s', '%s', '%f', '%s')"""\
					% (cid,week_list[week_count],hour_list[hour_count],frequency,realtime)
	    	#Regular formation for hourly info
			else:
				sql = """INSERT INTO busyhour (stationid, weekday, hour, freq, realtimeinfo) VALUES ('%s','%s', '%s', '%f', '%s')"""\
					% (cid,week_list[week_count],hour_list[hour_count],daily,realtime)
			cursor.execute(sql)
			# increase the inner index 
			hour_count = hour_count + 1
			realtime = 0.0
			db.commit()
		# increase the outer index
		week_count = week_count + 1
	# after all close the database 
	db.close()


def load_price_data(gasinfo, cid):
	db = MySQLdb.connect("localhost","liu433","liu433","gasinfo")
	cursor = db.cursor()
	sql = """INSERT INTO priceinfo (stationid, premium, midgrade, regular, diesel) VALUES ('%s','%s', '%s', '%s', '%s')""" \
			% (cid,gasinfo['prices']['Premium'],gasinfo['prices']['Midgrade'],gasinfo['prices']['Regular'],gasinfo['prices']['Diesel']) 
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


def load_traffic_data(cid, original, destination): 
	link1 = "https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial&departure_time=now&traffic_model=optimistic&origins="
	link2 = "&destinations="
	link3 = "&key=AIzaSyBEQaXw5dbn3ar55rKIni6S_y6aKkUdDt4"
	myResult = requests.get(link1 + original + link2 + destination + link3).json()    
	time = myResult['rows'][0]['elements'][0]['duration']['value']
	distance = myResult['rows'][0]['elements'][0]['distance']['value']
	db = MySQLdb.connect("localhost","liu433","liu433","gasinfo")
	cursor = db.cursor()
	sql = """INSERT INTO traffic (stationid, startloc, endloc, dist, travelmin) VALUES ('%s','%s', '%s', '%f', '%f')""" \
		% (cid, original, destination, distance, time) 
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


# Lists to store different information for the future use
cidlist=[]
placeidlist=[]
original=[]
destination=[]
# Open the Raw text file
file = open('Dataset.txt','r')
# Append the lists using the information from the text file 
for line in file:
	cidlist.append(line.split('|')[1].strip())
	placeidlist.append(line.split('|')[0].strip())
	original.append(line.split('|')[2].strip())	
	destination.append(line.split('|')[3].strip())

index = 0
for cid in cidlist:
	try:
		content=get_web_page(cid)
		print ('Downlad' + cid + ' succeed...')
		logger.info('Downlad' + cid + ' succeed...')
   		gas_info=get_gas_information(content)
		load_traffic_data(cid, original[index],destination[index])
		load_price_data(gas_info, cid)
		load_busyhour_data(gas_info, cid)
		index = index + 1
	except Exception as e:
		print ('reason: '+ str(e))
		logger.info("\n"+'process '+ cid + ' failed...')
		logger.info('reason: '+ str(e))
		logger.exception('Traceback: ' )

file.close()
