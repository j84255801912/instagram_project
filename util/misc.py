import inspect
import MySQLdb
import ConfigParser
import pycurl
import time
import sys
from StringIO import StringIO

from usage import error

def try_cache_image(db):
    cur = db.cursor()
    try:
        cur.execute("""SELECT image_url FROM image WHERE available=-1""")
    except Exception, e:
        print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        sys.exit(1)
    fetchdata = cur.fetchall()
    buffer = StringIO()
    curl = pycurl.Curl()
    count = 0
    for row in fetchdata:
        try:
            curl.setopt(curl.URL, row[0])
            curl.setopt(curl.WRITEDATA, buffer)
            curl.perform()
            status = curl.getinfo(pycurl.HTTP_CODE)
            print "%d , status = %d , %s" % (count, status, row[0])
        except Exception, e:
            error("ERROR in", inspect.stack()[0][3], e)
            continue
        try:
            cur.execute("""UPDATE image SET available=%s WHERE image_url=%s""", (1 if status is 200 else 0, row[0]))
            db.commit()
        except Exception, e:
            db.rollback()
            error("ERROR in", inspect.stack()[0][3], e)
        count += 1
    curl.close()

def test_broken_image(db):
    cur = db.cursor()
    try:
        cur.execute("""SELECT set_broken_image.image_id, image.image_url FROM set_broken_image LEFT JOIN image ON set_broken_image.image_id=image.seq_id WHERE operation=0""")
    except Exception, e:
        print e
        sys.exit(1)
    fetchdata = cur.fetchall()
    buffer = StringIO()
    curl = pycurl.Curl()
    for row in fetchdata:
        curl.setopt(curl.URL, row[1])
        curl.setopt(curl.WRITEDATA, buffer)
        curl.perform()
        status = curl.getinfo(pycurl.HTTP_CODE)
        if status is 200:
            print "========================================="
            print "image %d : %s is incorrectly set broken" % (row[0], row[1],)
            try:
                cur.execute("""UPDATE image SET available=%s WHERE seq_id=%s""", (1, row[0],))
                db.commit()
                print "Recover it to AVAILABLE"
            except Exception, e:
                print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
            try:
                cur.execute("""INSERT INTO set_broken_image (image_id, operation) VALUES (%s, %s)""", (row[0], 1,))
                db.commit()
                print "INSERT the RECOVERY operation into set_broken_image table"
            except Exception, e:
                print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
            print "========================================="
        else:
            print "image %d is broken, status code = %d" % (row[0], status,)
