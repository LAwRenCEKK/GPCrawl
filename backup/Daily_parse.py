#from gp_crawl import * 
import googlemaps
import MySQLdb
import requests
import logging 


workding_dir='/home/liu433/Project_lawrence_v1'
os.chdir(working_dir)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)-15s:%(name)s:%(message)s')
file_handler = logging.FileHandler('Daily_parse.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def load_station_data(fileJ,cid):
	db = MySQLdb.connect("localhost","liu433","liu433","gasinfo")
	cursor = db.cursor()
	name = fileJ['result']['name']
	location = fileJ['result']['formatted_address']
	lat = fileJ['result']['geometry']["location"]['lat']
	lng = fileJ['result']['geometry']["location"]['lng']
	rating = fileJ['result']['rating']
	sql = """INSERT INTO stationinfo (stationid, name, location, lat, lng, rating) VALUES ('%s', '%s', '%s', '%f', '%f', '%f')"""\
		% (cid,name,location,lat,lng,rating)
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


def load_review_data(url,cid,times):
	# Fetch the googleplace info in the json format
	fileJ = requests.get(url).json()
	try:
		# check if the result json imformation contains the review information
		fileJ['result']['reviews']
		db = MySQLdb.connect("localhost","liu433","liu433","gasinfo")
		cursor = db.cursor()
		for i in range (0, len(fileJ['result']['reviews'])):
			review = fileJ['result']['reviews'][i]['text']
			sql = """INSERT INTO reviews (stationid, review) VALUES ('%s', '%s')"""\
				% (cid, review)
			try:
    			# Execute the SQL command
				cursor.execute(sql)
    			# Commit your changes in the database
				db.commit()
			except:
    			# Rollback in case there is any erro
				db.rollback()
		db.close()

	# if the returned json information is not complete, then will request it again by calling the same function recursively
	except:
		times = times+1
		logger.warning("Warning " + cid + " : The json file returned by Google API doesn't include review key. Will try again %d "%(times))
		if (times <= 5):
			load_review_data(url, cid,times)
		else:
			logger.warning("Warning" + cid + " : Can't access the proper json file from server after trying 10 times")
			raise

link1 = "https://maps.googleapis.com/maps/api/place/details/json?placeid="
link2 = "&key=AIzaSyAwpKsKuWuw4WWbg7SlXZw0lmts36i5QFs"

placeidlist=[]
file = open('Dataset.txt','r')
for line in file:
	f=requests.get(link1 + line.split('|')[0].strip()+ link2).json()
	cid = line.split('|')[1].strip()
	url = link1 + line.split('|')[0].strip() + link2
	try:
		load_station_data(f,cid) 
		logger.info("Load station data "+cid + " successfully")
		load_review_data(url,cid,0)
		logger.info("Load review data "+ cid + " successfully")
	except:
		logger.info('process '+ cid + ' failed...')
    #    logger.info('reason: '+ str(e))
		logger.exception('Traceback: ' )
	logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")


file.close()
