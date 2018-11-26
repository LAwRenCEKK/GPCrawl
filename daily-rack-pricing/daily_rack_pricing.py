import requests
import MySQLdb
from bs4 import BeautifulSoup

def get_extra_info():

    # Retrive the HTML from URL
    f = requests.get('https://www.petro-canada.ca/en/rack-pricing/daily-rack-pricing.aspx')
    soup = BeautifulSoup(f.text, 'html.parser')

    # locate the table and extract information
    lis = soup.find_all('table', class_="rackHighlight infotable")

    # key[] list will be used into the dic
    final_dic = {}
    key = []

    # extract the key values
    for i in str(lis[0]).split('<tr>')[2:]:
        sub = (
            i.split('<td class="charttextwhite-s">')[0].replace('<td class=" tableheader-s" style="text-align:left;">',
                                                                ''))
        sub = sub.replace('<br/>', ' ')
        sub = sub.replace('</td>', '')
        key.append(sub.strip())

    # extract the element values
    ind = 0
    for i in str(lis[0]).split('<tr>')[2:]:
        sub = (i.split('<td class="charttextwhite-s">'))[1:]
        element = []
        for i in sub:
            subs = i.replace("</td>", "")
            subs = subs.replace("</tr>", "")
            subs = subs.replace("</table>", "")
            try:
                element.append(float(subs.strip()))
            except Exception as e:
                element.append(0.0)

        final_dic.update({key[ind]: element})
        ind = ind + 1
    print final_dic
    return final_dic


def load_rack_price(dic_final):
    db = MySQLdb.connect("localhost","liu433","liu433","gasinfo")
    cursor = db.cursor()
    for key, value in dic_final.items():
        sql = """INSERT INTO Daily_rack_price (location, Reg_UL_Oct_87, Mid_UL_Oct_89, Sup_UL_Oct_91, Reg_UL_E_10, Mid_UL_E_5, ULS_Diesel, ULS_Diesel_No1, Seas_FFO, Stove_Oil) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"""\
                 % (key, value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7], value[8])
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


final = get_extra_info()
load_rack_price(final)
