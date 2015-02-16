import inspect
import MySQLdb
import ConfigParser

def db_connect():
    cf = ConfigParser.ConfigParser()
    cf.read("db.conf")
    db_host = cf.get("db", "host")
    db_name = cf.get("db", "name")
    db_user = cf.get("db", "user")
    db_password = cf.get("db", "password")
    db_port = cf.get("db", "port")
    try:
        db = MySQLdb.connect(host=db_host,
                             user=db_user,
                             passwd=db_password,
                             db=db_name,
                             use_unicode=True,
                             charset="utf8")
        return db
    except Exception, e:
        print "ERROR in %s : %s" % (inspect.stack()[0][3], str(e),)
        return None
