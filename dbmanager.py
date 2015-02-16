#!/usr/bin/python

import argparse
import sys

from util.db import *
from util.create_table import *
from util.insert_table import *

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='Handle with instagram project tables')
    argparser.add_argument("-m", "--mode", help="create/insert", default="")
    argparser.add_argument("-t", "--tablename", help="table", default="")
    argparser.add_argument("-f", "--csvfile", help="csv file path", default="")
    args = argparser.parse_args()

    db = db_connect()
    if db != None:
        if args.mode == "create":
            if args.tablename == "location":
                create_table_location(db)
            elif args.tablename == "image_with_face":
                create_table_image_with_face(db)
            elif args.tablename == "image_without_face":
                create_table_image_without_face(db)
            elif args.tablename == "image_representative":
                create_table_image_representative(db)
            elif args.tablename == "set_broken_image":
                create_table_set_broken_image(db)
        elif args.mode == "insert":
            if args.tablename == "location":
                insert_location(db, args.csvfile)
            elif args.tablename == "image_with_face":
                insert_image_with_face(db, args.csvfile)
            elif args.tablename == "image_without_face":
                insert_image_without_face(db, args.csvfile)
            elif args.tablename == "image_representative":
                insert_image_representative(db, args.csvfile)
        db.close()
    else:
        print "db_connect() failed"
        sys.exit(1)
