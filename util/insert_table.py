import inspect
import time
import sys
import csv
import os

def insert_location(db, dir_path):
    cur = db.cursor()

    f = open(dir_path, 'r')
    count = 0
    for line in csv.DictReader(f, delimiter='\t'):
        try:
            cur.execute("""INSERT INTO location (location_id, location_name, lat, lon, number_of_img, category) VALUES (%s, %s, %s, %s, %s, %s)""", (int(line['location_id']), line['location_name'], float(line['lat']), float(line['lon']), int(line['number_of_img']), line['category']))
            db.commit()
            print str(count) + " location_id = " + line['location_id'] + " , name = " + line['location_name']
        except Exception, e:
            db.rollback()
            print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        count += 1
    f.close()
'''
def insert_image(db, dir_path):
    cur = db.cursor()

    f = open(dir_path, 'r')
    with_face = 0 if dir_path.find("image_with_face") == -1 else 1
    count = 0
    for line in csv.DictReader(f, delimiter='\t'):
        try:
            # location_id image_url image_instagram_url ranking description
            cur.execute("""INSERT INTO image (location_id, image_url, image_url_small, image_instagram_url, ranking, description, with_face) VALUES (%s, %s, %s, %s, %s, %s, %s)""", (int(line['location_id']), line['image_url'], line['image_url_small'], line['image_instagram_url'], int(line['ranking']), line['description'], with_face))
            db.commit()
            print str(count) + " location_id = " + line['location_id'] + " , name = " + line['image_url']
        except Exception, e:
            db.rollback()
            print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        count += 1
    f.close()
'''

def insert_image_with_face(db, dir_path):
    cur = db.cursor()

    f = open(dir_path, 'r')
    count = 0
    for line in csv.DictReader(f, delimiter='\t'):
        try:
            # location_id image_url image_instagram_url ranking description
            cur.execute("""INSERT INTO image_with_face (location_id, image_url, image_url_small, image_instagram_url, ranking, description) VALUES (%s, %s, %s, %s, %s, %s)""", (int(line['location_id']), line['image_url'], line['image_url_small'], line['image_instagram_url'], int(line['ranking']), line['description']))
            db.commit()
            print str(count) + " location_id = " + line['location_id'] + " , name = " + line['image_url']
        except Exception, e:
            db.rollback()
            print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        count += 1
    f.close()

def insert_image_without_face(db, dir_path):

    cur = db.cursor()

    f = open(dir_path, 'r')
    count = 0
    for line in csv.DictReader(f, delimiter='\t'):
        try:
            # location_id image_url image_instagram_url ranking description
            cur.execute("""INSERT INTO image_without_face (location_id, image_url, image_url_small, image_instagram_url, ranking, description) VALUES (%s, %s, %s, %s, %s, %s)""", (int(line['location_id']), line['image_url'], line['image_url_small'], line['image_instagram_url'], int(line['ranking']), line['description']))
            db.commit()
            print str(count) + " location_id = " + line['location_id'] + " , name = " + line['image_url']
        except Exception, e:
            db.rollback()
            print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        count += 1
    f.close()

def insert_image(db, dir_path):

    cur = db.cursor()

    with open(dir_path, 'r') as f:
        count = 0
        for line in csv.DictReader(f, delimiter='\t'):
            try:
                # location_id image_url image_instagram_url ranking description
                cur.execute("""INSERT INTO image (location_id, image_url, image_url_small, image_instagram_url, ranking, description, with_face) VALUES (%s, %s, %s, %s, %s, %s, %s)""", (int(line['location_id']), line['image_url'], line['image_url_small'], line['image_instagram_url'], int(line['ranking']), line['description'], int(line['with_face'])))
                db.commit()
                print str(count) + " location_id = " + line['location_id'] + " , name = " + line['image_url']
            except Exception, e:
                db.rollback()
                print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
            count += 1


def insert_image_representative(db, dir_path):
    cur = db.cursor()

    f = open(dir_path, 'r')
    count = 0
    for line in csv.DictReader(f, delimiter='\t'):
        try:
            # location_id image_url image_url_small
            cur.execute("""INSERT INTO image_representative (location_id, image_url, image_url_small) VALUES (%s, %s, %s)""", (int(line['location_id']), line['image_url'], line['image_url_small']))
            db.commit()
            print str(count) + " location_id = " + line['location_id'] + " , name = " + line['image_url']
        except Exception, e:
            db.rollback()
            print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        count += 1
    f.close()

def insert_tags(db, dir_path):

    cur = db.cursor()

    for filename in os.listdir('.'):
        with open('{}/{}'.format(dir_path, filename)) as f:
            for line in csv.DictReader(f, delimiter='\t', fieldnames=['name', 'count']):
                try:
                    cur.execute("""INSERT INTO db_tags (name, location_id, count) VALUES (%s, %s, %s)""", (line['name'], int(filename), int(line['count'])))
                    db.commit()
                except Exception, e:
                    db.rollback()
                    print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
