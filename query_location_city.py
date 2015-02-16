import urllib2, json, time, MySQLdb
from db_control import insta_db
import mysql_latin1_codec

keys = [
        "AIzaSyDtu84nv89uwcfwiahxgnKDDWYo_as4Vpo",
        "AIzaSyAsHtrHOQ4p1KgoHMOMm_8nXcqR_Q77ENU",
        "AIzaSyBr3tsqQ9fwaNf6G33DU0wT_BHXdVgGBkI",
        "AIzaSyCp_ZBei6nKAvoFARwRO61oMtI8rdHg4DM",
        "AIzaSyArrg9Okd0MtPZu3NaMImoYi6Ajt8Y_-Ko",
        "AIzaSyBvGinN9_0SH3kXS-yRDGp3Z1xO1da9SXo"
#        "AIzaSyDbbS0Shsymj6jNAEDdcnEUfhExV4S8yVE"
       ]
def getdata(lat, lon, key = None):
    if key is not None:
#        content = urllib2.urlopen("https://maps.googleapis.com/maps/api/geocode/json?latlng="+str(lat)+","+str(lon)+"&sensor=false&key="+key).read()
        content = urllib2.urlopen("https://maps.googleapis.com/maps/api/geocode/json?latlng="+str(lat)+","+str(lon)+"&sensor=false&language=zh-tw&key="+key).read()
    else:
        content = urllib2.urlopen("https://maps.googleapis.com/maps/api/geocode/json?latlng="+str(lat)+","+str(lon)+"&sensor=false").read()

    return content
#def getaddress(content
if __name__ == '__main__':
    mydb = insta_db()
    db = mydb.db_connect()
#    mydb.create_table_location_address(db)
#    getdata(24.813924697, 121.350044753)
    cur = db.cursor()
    sql = '''SELECT lat, lon, location_id FROM location WHERE city_chinese is NULL AND district_chinese is NULL'''
    #sql = '''SELECT lat, lon, location_id FROM location'''
    try:
        cur.execute(sql)
    except:
        print "error in selecting locations"
    fetchdata = cur.fetchall()
    i = 0
    for row in fetchdata:
        temp = json.loads(getdata(row[0], row[1], keys[i]))
        if temp['status'] != "OK":
            print temp
            continue
        result = temp['results'][0]
        address_components = result['address_components']
        for name in address_components:
            if name['types'][0] == "administrative_area_level_3":
                second = name['long_name']
            if name['types'][0] == "administrative_area_level_2":
                first = name['long_name']
            if name['types'][0] == "administrative_area_level_1":
                first = name['long_name']
        print "keys "+str(i)+", location_id "+str(row[2])+" "+first.encode('utf-8')+" "+second.encode('utf-8')
        try:
            cur.execute("""UPDATE location SET city_chinese=%s, district_chinese=%s WHERE location_id=%s""", (first.encode('utf-8'), second.encode('utf-8'), row[2]))
            db.commit()
        except MySQLdb.Error, e:
            db.rollback()
            try:
                print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(e)
        i = (i + 1) % len(keys)
        time.sleep(5)
