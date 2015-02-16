import inspect
import time
import sys
import csv

def insert_location(db, dir_path):
    cur = db.cursor()

    f = open(dir_path, 'r')
    count = 0
    for line in csv.DictReader(f, delimiter='\t'):
        try:
            cur.execute("""INSERT INTO location_v2 (location_id, location_name, lat, lon, number_of_img, category) VALUES (%s, %s, %s, %s, %s, %s)""", (int(line['location_id']), line['location_name'], float(line['lat']), float(line['lon']), int(line['number_of_img']), line['category']))
            db.commit()
            print str(count) + " location_id = " + line['location_id'] + " , name = " + line['location_name']
        except Exception, e:
            db.rollback()
            print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
            sys.exit(1)
        count += 1
    f.close()
def insert_image_with_face(db, dir_path):
    cur = db.cursor()

    f = open(dir_path, 'r')
    count = 0
    for line in csv.DictReader(f, delimiter='\t'):
        try:
            # location_id image_url image_instagram_url ranking description
            cur.execute("""INSERT INTO image_with_face_v2 (location_id, image_url, image_url_small, image_instagram_url, ranking, description) VALUES (%s, %s, %s, %s, %s, %s)""", (int(line['location_id']), line['image_url'], line['image_url_small'], line['image_instagram_url'], int(line['ranking']), line['description']))
            db.commit()
            print str(count) + " location_id = " + line['location_id'] + " , name = " + line['image_url']
        except Exception, e:
            db.rollback()
            print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
            sys.exit(1)
        count += 1
    f.close()

def insert_image_without_face(db, dir_path):
    cur = db.cursor()

    f = open(dir_path, 'r')
    count = 0
    for line in csv.DictReader(f, delimiter='\t'):
        try:
            # location_id image_url image_instagram_url ranking description
            cur.execute("""INSERT INTO image_without_face_v2 (location_id, image_url, image_url_small, image_instagram_url, ranking, description) VALUES (%s, %s, %s, %s, %s, %s)""", (int(line['location_id']), line['image_url'], line['image_url_small'], line['image_instagram_url'], int(line['ranking']), line['description']))
            db.commit()
            print str(count) + " location_id = " + line['location_id'] + " , name = " + line['image_url']
        except Exception, e:
            db.rollback()
            print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
            sys.exit(1)
        count += 1
    f.close()

def insert_image_representative(db, dir_path):
    cur = db.cursor()

    f = open(dir_path, 'r')
    count = 0
    for line in csv.DictReader(f, delimiter='\t'):
        try:
            # location_id image_url image_url_small
            cur.execute("""INSERT INTO image_representative_v2 (location_id, image_url, image_url_small) VALUES (%s, %s, %s)""", (int(line['location_id']), line['image_url'], line['image_url_small']))
            db.commit()
            print str(count) + " location_id = " + line['location_id'] + " , name = " + line['image_url']
        except Exception, e:
            db.rollback()
            print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
            sys.exit(1)
        count += 1
    f.close()
