from db_control import insta_db
mydb = insta_db()
db = mydb.db_connect()
mydb.try_cache_image(db)
db.close()
