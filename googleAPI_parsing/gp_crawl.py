import urllib2
import urllib
from bs4 import BeautifulSoup  
import datetime,os,json
import MySQLdb
import datetime
import time 
import logging
from logging.handlers import TimedRotatingFileHandler


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)-15s:%(name)s:%(message)s')
file_handler = logging.handlers.TimedRotatingFileHandler('/home/liu433/Project_lawrence_v1/loggings/gp_crawl.log', when = 'midnight', interval = 1, backupCount = 10)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

#get the google map webpage
def get_web_page(cid,splash_server='localhost:8050',timeout=15,wait=0.5):
	try:
		google_map_addr = 'https://maps.google.com/?cid='+cid;
		google_map_addr = urllib.quote_plus(google_map_addr)
		request_url = 'http://'+splash_server+'/render.html?url='+google_map_addr+'&timeout='+str(timeout)+'&wait='+str(wait)
		response = urllib2.urlopen(request_url)
		html=response.read()
#		logger.info("website has been downloaded succesfully: "+ cid)
		return html
	except:
		logger.info("WebPage Download unsuccesfully: "+ cid)
		logger.exception("Traceback:")

def get_gas_prices(soup):
	prices_list=soup.find_all("div", class_="section-gas-prices-price")
    #print(prices_list)
	prices={}
	for node in prices_list:
		label=node.find("div",class_='section-gas-prices-label').text
		priceinfo = node.find("span").text
		if label != None:
			prices[label] = priceinfo
            #print(label+":"+subnode)
	return prices

#parse the busy information
def parse_busy_str(busystr, cid):
	try:
		fields = busystr.split()
		busynum = float(fields[0].strip('%'))*1.0/100
		hour = fields[-2]+fields[-1]
		return (hour,busynum)
	except ValueError:
		logger.debug("There is a live data %s " %(str(cid).strip()))
		print "There is a live data " + str(cid).strip()
		print busystr
		return ('live',busystr)

#get the busy hours a week
def get_busy_hours(soup, cid):
	assert(soup != None)
	week_busy_hours=[]
	busyweek = soup.find("div",class_ = "section-popular-times-container")
	try:
		days=busyweek.findChildren(recursive=False)
	except:
		print "return None for the busy Hours " + cid
		return []
	for i in range(len(days)):
		busyhours = days[i].find_all("div",class_ = "section-popular-times-bar")
		busydesc=[]
		for i in range(len(busyhours)):
			hourinfo=parse_busy_str(busyhours[i].attrs['aria-label'], cid)
            #busydesc[hourinfo[0]]=hourinfo[1]
			busydesc.append(hourinfo[1])
		week_busy_hours.append(busydesc)
    # To unify the busyhour data format: -1 means Data is N/A 
	for i in range(7):
		if len(week_busy_hours[i]) == 18:
			for j in range(2):
				week_busy_hours[i].insert(0,-1)
			for k in range(4):
				week_busy_hours[i].insert(len(week_busy_hours[i]),-1)
		if len(week_busy_hours[i]) == 1:
			week_busy_hours[i] = [-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1]
		elif len(week_busy_hours[i]) != 24:
			print ("The busyhour information for current station is in the different formation")
			logger.debug("The Current Station %s has different information format " %(str(cid).strip()))
	return week_busy_hours

#get the rating information
def get_rating(soup):
	ratingnode=soup.find("span",class_ = "section-star-display")
	return ratingnode.text

#get the review summary
def get_review_summary(soup):
	reviewnodes = soup.find_all("div",class_ = "section-review-snippet-line")
	reviews=[]
	for reviewnode in reviewnodes:
		pieces = reviewnode.find_all("span",class_="section-review-snippet-text")
		pieces = [piece.text for piece in pieces]
		review=" ".join(pieces)
		reviews.append(review)
	return reviews

#Get the information from
def get_gas_information(html, cid):
	soup = BeautifulSoup(html,'html.parser') 
	prices=get_gas_prices(soup)
	busyinfo=get_busy_hours(soup,cid)
	rating =get_rating(soup)
	review = get_review_summary(soup)
#	print {'prices':prices,'busyinfo':busyinfo,'rating':rating,'review':review}
	return {'prices':prices,'busyinfo':busyinfo,'rating':rating,'review':review}
