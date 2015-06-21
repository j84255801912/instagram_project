import inspect
import time
import sys
import csv

version = ""

def create_table_location(db):
    cur = db.cursor()
    table_name = "location" + version
    sql =   '''
            CREATE TABLE IF NOT EXISTS %s (
                id                  INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                location_id         INT,
                location_name       VARCHAR(100),
                lat                 DOUBLE,
                lon                 DOUBLE,
                number_of_img       INT,
                category            VARCHAR(100),
                city                VARCHAR(100),
                city_chinese        VARCHAR(100),
                district            VARCHAR(100),
                district_chinese    VARCHAR(100),
                UNIQUE  (location_id)
            )
            ''' % (table_name,)
    try:
        cur.execute(sql)
        print "Created table %s" % (table_name,)
    except Exception, e:
        print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        sys.exit(1)

'''
    this is the old version.
'''
def create_table_location_image_cache(db):

    cur = db.cursor()
    # check if the location_image_cache table exists.
    # if not, we should build one.
    # else, create a new one. Back up the old one and use the new one.
    sql = "SHOW TABLES"
    try:
        cur.execute(sql)
    except Exception, e:
        print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        sys.exit(1)

    table_names = [i[0].encode('utf-8') for i in cur.fetchall()]
    num_photo = 5

    if "location_image_cache" not in table_names:
        table_name = "location_image_cache"
    else:
        table_name = "location_image_cache_" + time.strftime("%Y%m%d%H%M", time.localtime())

    sql =   """
                CREATE TABLE IF NOT EXISTS %s (
                    PRIMARY KEY id (id),
                    UNIQUE (location_id),
                    INDEX number_of_img (number_of_img)
                )
                    SELECT l.*, p.urls FROM location l LEFT JOIN
                    (
                        SELECT k.location_id, GROUP_CONCAT(image_url_small) as urls FROM
                        (
                            SELECT t.*, @num := if(@location_id = location_id, @num + 1, 1) AS row_number, @location_id := location_id AS dummy FROM
                            (
                                SELECT * FROM image WHERE available!=0 ORDER BY location_id ASC, with_face ASC, ranking ASC
                            ) t
                            GROUP BY location_id, row_number HAVING row_number <= %d
                        ) k JOIN
                        (
                            SELECT @num:=0, @location_id=NULL
                        ) q
                        GROUP BY location_id
                    ) p
                    ON l.location_id=p.location_id
            """ % (table_name, num_photo)
    try:
        cur.execute(sql)
    except Exception, e:
        print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        sys.exit(1)
    print "Created table %s" % (table_name,)

    if table_name != "location_image_cache":
        # rename the old table to table_with_time and rename the new table to
        # location_image_cache
        sql =   "RENAME TABLE location_image_cache TO tmp_table, %s TO location_image_cache, tmp_table TO %s" % (table_name, table_name)
        try:
            cur.execute(sql)
        except Exception, e:
            print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
            sys.exit(1)
        print "Swapped the old one and the new one"

'''
this is the complicated version.
def create_table_location_image_cache(db):
    cur = db.cursor()
    # check if the location_image_cache table exists.
    # if not, we should build one.
    # else, create a new one. Back up the old one and use the new one.
    sql = "SHOW TABLES"
    try:
        cur.execute(sql)
    except Exception, e:
        print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        sys.exit(1)
    table_names = [i[0].encode('utf-8') for i in cur.fetchall()]
    if "location_image_cache" not in table_names:
        table_name = "location_image_cache"
    else:
        table_name = "location_image_cache_" + time.strftime("%Y%m%d%H%M", time.localtime())

    sql =   """
                CREATE TABLE IF NOT EXISTS %s AS
                (
                    SELECT l.*, p.urls FROM location l LEFT JOIN
                    (
                        SELECT k.location_id, GROUP_CONCAT(image_url_small) as urls FROM
                        (
                            SELECT t.*, @num := if(@location_id = location_id, @num + 1, 1) AS row_number, @location_id := location_id AS dummy FROM
                            (
                                (
                                    SELECT i.*, w.with_face FROM image_without_face AS i JOIN
                                    (
                                        SELECT @temp:=0 AS with_face
                                    ) w
                                )
                                UNION ALL
                                (
                                    SELECT j.*, w.with_face FROM image_with_face AS j JOIN
                                    (
                                        SELECT @temp:=1 AS with_face
                                    ) w
                                )
                                ORDER BY location_id ASC, with_face ASC, ranking ASC
                            ) t
                            GROUP BY location_id, row_number HAVING row_number <= 3
                        ) k JOIN
                        (
                            SELECT @num:=0, @location_id=NULL
                        ) q
                        GROUP BY location_id
                    ) p
                    ON l.location_id=p.location_id
                )
            """ % (table_name,)
    try:
        cur.execute(sql)
    except Exception, e:
        print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        sys.exit(1)
    print "Created table %s" % (table_name,)

    if table_name != "location_image_cache":
        # rename the old table to table_with_time and rename the new table to location_image_cache
        sql =   "RENAME TABLE location_image_cache TO tmp_table, %s TO location_image_cache, tmp_table TO %s" % (table_name, table_name)
        try:
            cur.execute(sql)
        except Exception, e:
            print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
            sys.exit(1)
        print "Swapped the old one and the new one"
'''

def create_table_set_broken_image(db):
    cur = db.cursor()
    table_name = "set_broken_image" + version
    sql = '''
             CREATE TABLE IF NOT EXISTS %s (
                id              INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                image_url       VARCHAR(200),
                operation       INT,
             )
          ''' % (table_name,)
#                CONSTRAINT uc_ImageOp UNIQUE (image_url,operation)
    try:
        cur.execute(sql)
        print "Created table %s" % (table_name,)
    except Exception, e:
        print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        sys.exit(1)

def create_table_image_with_face(db):
    cur = db.cursor()
    table_name = "image_with_face" + version
    sql =   '''
            CREATE TABLE IF NOT EXISTS %s (
                id                  INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                location_id         INT,
                description         VARCHAR(200),
                image_url           VARCHAR(200),
                image_url_small     VARCHAR(200),
                image_instagram_url VARCHAR(200),
                ranking             INT,
                available           INT DEFAULT -1,
                UNIQUE  (image_url),
                INDEX location_id (location_id)
            )
            ''' % (table_name,)
    try:
        cur.execute(sql)
        print "Created table %s" % (table_name,)
    except Exception, e:
        print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        sys.exit(1)

def create_table_image(db):
    cur = db.cursor()
    table_name = "image_with_face" + version
    sql =   '''
            CREATE TABLE IF NOT EXISTS %s (
                id                  INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                location_id         INT,
                description         VARCHAR(200),
                image_url           VARCHAR(200),
                image_url_small     VARCHAR(200),
                image_instagram_url VARCHAR(200),
                ranking             INT,
                available           INT DEFAULT -1,
                UNIQUE  (image_url),
                INDEX location_id (location_id)
            )
            ''' % (table_name,)
    try:
        cur.execute(sql)
        print "Created table %s" % (table_name,)
    except Exception, e:
        print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        sys.exit(1)

def create_table_image_without_face(db):
    cur = db.cursor()
    table_name = "image_without_face" + version
    sql =   '''
            CREATE TABLE IF NOT EXISTS %s (
                id                  INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                location_id         INT,
                description         VARCHAR(200),
                image_url           VARCHAR(200),
                image_url_small     VARCHAR(200),
                image_instagram_url VARCHAR(200),
                ranking             INT,
                available           INT DEFAULT -1,
                UNIQUE  (image_url),
                INDEX location_id (location_id)
            )
            ''' % (table_name,)
    try:
        cur.execute(sql)
        print "Created table %s" % (table_name,)
    except Exception, e:
        print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        sys.exit(1)

def create_table_image_representative(db):
    cur = db.cursor()
    table_name = "image_representative" + version
    sql =   '''
            CREATE TABLE IF NOT EXISTS %s (
                id                  INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                location_id         INT,
                image_url           VARCHAR(200),
                image_url_small     VARCHAR(200),
                available           INT DEFAULT -1,
                UNIQUE  (image_url),
                INDEX location_id (location_id)
            )
            ''' % (table_name,)
    try:
        cur.execute(sql)
        print "Created table %s" % (table_name,)
    except Exception, e:
        print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        sys.exit(1)
