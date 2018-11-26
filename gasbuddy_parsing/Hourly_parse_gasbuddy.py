from getJSON_gasbuddy import *
import getJSON_gasbuddy
import logging,os
from bs4 import BeautifulSoup
os.system("export PYTHONDONTWRITEBYTECODE=true")
#set working directory(keep this two lines).
working_dir = '/home/liu433/Project_lawrence_v1/gasbuddy_parsing'
os.chdir(working_dir)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)-15s:%(name)s:%(message)s')
file_handler = logging.handlers.TimedRotatingFileHandler('/home/liu433/Project_lawrence_v1/loggings/Hourly_parse_gasbuddy.log',when='midnight', interval=1, backupCount=10)
#file_handler = logging.FileHandler('/home/liu433/Project_lawrence_v1/loggings/Hourly_parse_gasbuddy.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.propagate = True

# It will call the getJSON module
# There will be two logging files generated once this program got executed 
# The result is 4X4 matrice 
def load_priceinfo2(json_file, ID, times = 0):
    type = ['Regular','Midgrade', 'Premium', 'Diesel']
    price = []
    updatetime = []
    updateperson = []
    for i in type:
    #expected result is four prices, Price for Diesel will be 0.0
	#Or there won't be any price number there
        try:
            price.append(str(json_file['fuels']['fuelsByStationId'][ID][i]['prices'][0]['price']).strip())
            try:
                updateperson.append(str(json_file['fuels']['fuelsByStationId'][ID][i]['prices'][0]['reportedBy']).strip())
                updatetime.append(str(json_file['fuels']['fuelsByStationId'][ID][i]['prices'][0]['postedTime']).strip())
            except Exception as k:
                price.pop(-1)
                price.append('None')
                updateperson.append("Unknown")
                updatetime.append("Unknown")
        except Exception as r:
            print r
            price.append('None')
            updateperson.append("Unknown")
            updatetime.append("Unknown")
	#will return the dictionary
    return [price, updateperson, updatetime]


def upload_priceinfo_gasbudday(Id, priceinfo, connection):
    db = MySQLdb.connect('localhost','liu433','liu433','gasinfo')
    cursor = db.cursor()
    sql = """insert into priceinfo2(stationid, premium, midgrade, regular,diesel, updateBy_Premium,updateTiem_Premium,updateBy_Reg,updateTiem_Reg,updateBy_Mid,updateTiem_Mid,updateBy_Diesel,updateTiem_Diesel)  Values ("%s","%s","%s","%s","%s","%s", "%s", "%s","%s", "%s", "%s","%s","%s")""" \
            % (Id, priceinfo[0][2],priceinfo[0][1],priceinfo[0][0],priceinfo[0][3], priceinfo[1][2],priceinfo[2][2],priceinfo[1][1],priceinfo[2][1],priceinfo[1][0],priceinfo[2][0],priceinfo[1][3],priceinfo[2][3])
    try:
        cursor.execute(sql)
        db.commit()
    except:
        logger.exception("Got Exception on main log: ")
        raise
        db.rollback()
    db.close()

#since the JSON file get from the Gasbuddy may not be complete, which will potentially casue the failures 
#extract the station ID retrived from Gasbuddy
file = open("/home/liu433/Project_lawrence_v1/Dataset.txt", 'r')
final_retry = []
for i in file:
    i = i.split("|")[-1]
    i = i.strip()
    result = get_json(i)
#Put the failed ID into the list for the final try
    if result == str(i):
        final_retry.append(result)
    else:
        priceinfo = load_priceinfo2(result, i)
        print priceinfo
        upload_priceinfo_gasbudday(i, priceinfo,["172.17.54.91",33066,"liu433","liu433", "gasinfo"])
print final_retry
logger.debug("There are {} elements in the list needed to be retried".format(len(final_retry)))
if len(final_retry) != 0:
    for i in final_retry:
        final = get_json(i,1)
        if final != str(i):
            priceinfo = load_priceinfo2(final, i)
            upload_priceinfo_gasbudday(i, priceinfo,["172.17.54.91",33066,"liu433","liu433", "gasinfo"])
        else:
            pass
