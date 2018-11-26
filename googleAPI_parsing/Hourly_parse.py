import sys
#import gp_crawl
from gp_crawl import * 
import googlemaps
import MySQLdb
import requests
import logging
#import lxml

#Set the working directory(do not move, they are nessesary
working_dir = '/home/liu433/Project_lawrence_v1/googleAPI_parsing'
os.chdir(working_dir)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)-15s:%(name)s:%(message)s')
file_handler = logging.handlers.TimedRotatingFileHandler('/home/liu433/Project_lawrence_v1/loggings/Hourly_parse.log', when = 'midnight', interval = 1, backupCount = 10)
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
	if len(gasinfo['busyinfo']) == 0:
		print "This is Staion has no busyHour infor showing up: " + str(cid).strip()
		logger.debug("This is Staion has no busyHour infor showing up: %s" %(str(cid).strip()))
		return 0
	for weekdays in gasinfo['busyinfo']:
		hour_count = 0
		# Get access for hourly information for each day
		for daily in weekdays:
			#check if the information of current hour include the realtime info
			if type(daily) == unicode:
				first_index = daily.find('%')
				second_index = daily.rfind('%')
				realtime = int(daily[first_index-2:first_index])*0.01
				frequency = int(daily[second_index-2: second_index])*0.01
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
	print gasinfo
	try:
		sql = """INSERT INTO priceinfo (stationid, premium, midgrade, regular, diesel) VALUES ('%s','%s', '%s', '%s', '%s')""" \
				% (cid,gasinfo['prices']['Premium'],gasinfo['prices']['Midgrade'],gasinfo['prices']['Regular'],gasinfo['prices']['Diesel']) 
	except:
#		print "the error occurs during the loading the price data process"
		print  "this is CID causing Price loading error " + cid.strip()
		logger.debug("this is CID causing Price loading error %s" %(cid.strip()))
	try:
    	# Execute the SQL command
		cursor.execute(sql)
    	# Commit your changes in the database
		db.commit()
	except:
#		logger.exception('Got exception on main handler')
#		raise
    	# Rollback in case there is any error
		db.rollback()
	    # disconnect from server
	db.close()


def load_traffic_data(cid, original, destination,times): 
	link1 = "https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial&departure_time=now&traffic_model=optimistic&origins="
	link2 = "&destinations="
	link30 = "&key=AIzaSyBEQaXw5dbn3ar55rKIni6S_y6aKkUdDt4"
	link31 = "&key=AIzaSyADtfuFm_8tuB-f2bWYwosg2WZILPqhzao"
	link32 = "&key=AIzaSyAjSRqnJbjdIwAG9k5AyPwxY08IWsibSes"
	links = [link30,link31,link32]
	link = times % 3
	try:
		print str(cid) +" " + original + " " + destination
		myResult = requests.get(link1 + original + link2 + destination + links[link]).json()
		print myResult
		myResult2 = requests.get(link1 + destination + link2 + original + links[link]).json() 
		time = myResult['rows'][0]['elements'][0]['duration']['value']
		distance = myResult['rows'][0]['elements'][0]['distance']['value']
		time1 = myResult2['rows'][0]['elements'][0]['duration']['value']
		distance1 = myResult2['rows'][0]['elements'][0]['distance']['value']
	except:
		print str(cid) +" " + original + " " + destination
		link = (times + 1) % 3
		myResult = requests.get(link1 + original + link2 + destination + links[2]).json()    
		print myResult
		myResult2 = requests.get(link1 + destination + link2 + original + links[2]).json()    
        time = myResult['rows'][0]['elements'][0]['duration']['value']
        distance = myResult['rows'][0]['elements'][0]['distance']['value']
        time1 = myResult2['rows'][0]['elements'][0]['duration']['value']
        distance1 = myResult2['rows'][0]['elements'][0]['distance']['value'] 

	db = MySQLdb.connect("localhost","liu433","liu433","gasinfo")
	cursor = db.cursor()
	if distance1 > distance * 1.30:
		sql = """INSERT INTO traffic (stationid, startloc, endloc, dist, travelmin) VALUES ('%s','%s', '%s', '%f', '%f')""" \
			% (cid, original, destination, distance, time) 
	else:
		sql = """INSERT INTO traffic (stationid, startloc, endloc, dist, travelmin, dist_reverse, travelmin_reverse) VALUES ('%s','%s', '%s', '%f', '%f', '%f', '%f')""" \
			% (cid, original, destination, distance, time, distance1, time1) 
	try:
        # Execute the SQL command
		cursor.execute(sql)
    	# Commit your changes in the database
		db.commit()
	except:
    	# Rollback in case there is any error
		logger.exception('Got exception on main handler')
		raise
		db.rollback()
    	# disconnect from server 
	db.close()



# Lists to store different information for the future use
cidlist=[]
placeidlist=[]
original=[]
destination=[]

# Open the Raw text file
file = open('/home/liu433/Project_lawrence_v1/Dataset.txt','r')
# Append the lists using the information from the text file 
for line in file:
	cidlist.append(line.split('|')[1].strip())
	placeidlist.append(line.split('|')[0].strip())
	original.append(line.split('|')[2].strip())
	destination.append(line.split('|')[3].strip())
file.close()

for cid in cidlist:
	content=get_web_page(cid)
   	gas_info=get_gas_information(content, cid)
	load_price_data(gas_info, cid)
	load_busyhour_data(gas_info, cid)

index = 0
for line2 in cidlist:
	load_traffic_data(line2,original[index],destination[index],index)
	index = index + 1
