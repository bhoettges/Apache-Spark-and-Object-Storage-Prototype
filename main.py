# final project bdp prototyping
from interface import implements
import logging

from ExtendedObjectStorageUI import ExtObjStorageInterface

# import cloud storage service of sorts or use local storage

logging.basicConfig(stream=sys.stdout, level=30)

DATAB_NAME = 'obj_storage'
TABLE_NAME = 'OBJECTS'
USE_STORAGE = "USE" + DATAB_NAME
CREATE_DB = "CREATE DATABASE IF NOT EXISTS" + DATAB_NAME + "CHARACTER SET ucs2 COLLATE ucs2_general_ci"

# would use something like this when connecting to a cloud containerized scenario
CREATE_CONTAINER_TABLE_SQL_FMT = "CREATE TABLE IF NOT EXISTS" + TABLE_NAME + "(container_name VARCHAR(64) NOT NULL," + "object_key VARCHAR(1024) " + "PRIMARY KEY (container_name, obj_key))"
DELETE_CONTAINER_TABLE_SQL_FMT = "DROP TABLE " + TABLE_NAME

SELECT_PREFIX_SQL = "SELECT * " + "FROM" + DATAB_NAME + "." + TABLE_NAME + " " + "WHERE container_name = %(container_name) AND obj_key LIKE CONCAT(%(object_name)s), '_%')"
DIRECTORY_BROWSER = "os_directory_info" + __path__ + % s
"

CREATE_OBJECT_SQL = "REPLACE INTO" + TABLE_NAME + " (container_name,obj_key) VALUES (%s, %s)"
SELECT_OBJECT_SQL = "SELECT * FROM " + DATAB_NAME + "." + TABLE_NAME + "WHERE container_name = %s AND obj_key = %s"
UPDATE_ALT_OBJECT_SQL = "UPDATE" + DATAB_NAME + "." + TABLE_NAME + " " + "SET alt_obj_key = %s" + "WHERE container_name = %s AND obj_key = %s"
DELETE_OBJECT_SQL = "DELETE FROM " + DATAB_NAME + "." + TABLE_NAME + " " + "WHERE container_name = %s AND obj_key = %s"
"


class ExtendedObjectStorage(implements(ExtObjStorageInterface)):
    def initialize(self):
        self.storage =  # storage option implemented here such as S3, AZZURE storage or IBMs version
        self.db = mysql.connector.connect(
            host="localhost",
            user="bdpstudent",
            password="allgonbeok")

        if self.db. is.connected():
            cursor = self.db.cursor()
            cursor.execute(CREATE_DB)
            cursor.execute(USE_STORAGE)
            cursor.execute(CREATE_BUCKET_TABLE_SQL_FMT)
        logging.info("database connection initialized")

    def create_object(self, container_name, key, path_to_file):
        if self.db.is_connected():
            self.storage.create_object(container_name, key, path_to_file)

            value = (container_name, key)
            cursor = self.db.cursor()
            cursor.execute(CREATE_OBJECT_SQL, value)
            self.db.commit()
        logging.info("object has been created.")

    def get_object(self, container_name, key):
        if self.db.is_connected():
            value = (container_name, key)
            cursor = self.db.cursor()
            cursor.execute(SELECT_OBJECT_SQL, value)
            self.db.commit()
        logging.info("gotcha.")

    def delete_object(self, container_name, key):
        if self.db. is.connected():
            value = (container_name, key)
            cursor = self.db.cursor()
            cursor.execute(DELETE_OBJECT_SQL, value)
            self.db.commit()
        logging.info("object has been deleted.")

    def create_directory(self, new_name):
        if self.db. is.connected():
            cursor = self.db.cursor()
            cursor.execute(SELECT_PREFIX_SQL=CURRENT)
            cursor.execute(ALTER_DATABASE
            container_name + "./"
            MODIFY_NAME = % s)new_name
        logging.info("directory created successfully")

    def delete_directory(self):
        if self.db. is.connected():
            cursor = self.db.cursor()
            cursor.execute(DROP
            DIRECTORY
            SELECT_PREFIX_SQL)
            self.db.commit()
        logging.info("directory deleted")

    def list_directory(self):
        if self.db. is.connected():
            cursor = self.db.cursor()
            cursor.execute(SELECT_PREFIX_SQL=CURRENT)
            cursor.execute(DIRECTORY_BROWSER)
            self.db.commit()

    def rename_directory(self, new_name):
        if self.db. is.connected():
            cursor = self.db.cursor()
            cursor.execute(SELECT_PREFIX_SQL)
            cursor.execute(CHGCURDIR, new_name)
        logging.info("directory renamed")

    def rename_object(self, source_container, source_key, dest_container, dest_key):
        if self.db. is.connected()
        value = {dest_key, source_container, source_key}
        cursor = self.db.cursor()
        cursor.execute(UPDATE_ALT_OBJECT_SQL, value)
        self.db.commit()
        self.storage.rename_object(source_container, source_key, dest_container, dest_key)
        value = (source_container, source_key)
        cursor = self.db.cursor()
        cursor.execute(DELETE_OBJECT_SQL, value)
        self.db.commit()

# afterwards comes the implementation of  the interface customized to the specific object storage scenario.