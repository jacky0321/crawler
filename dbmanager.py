from mysql.connector import errorcode
from mysql.connector import pooling
import mysql.connector
import redis

import hashlib
import time
import sys


class CrawlDatabaseManager(object):
    def __init__(self, max_num_thread):
        self.host = '127.0.0.1'
        self.port = 3306
        self.db = 'crawler'
        self.user = 'root'
        self.password = '112233'

        dbconfig = {
            "database": self.db,
            "user":     self.user,
            "host":     self.host,
            "password":	self.password
        }

        try:
        	self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name = "mypool",
                                     								pool_size = max_num_thread,
                                                          			**dbconfig)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err.msg)
                sys.exit(1)

        self.redis_client = redis.StrictRedis(host=self.host, port=6379, db=0)


    def enqueueUrl(self, url, depth):
    	num = self.redis_client.get(url)
    	if num is not None:
    		self.redis_client.set(url, int(num)+1)
    	else:
            self.redis_client.set(url, 1)
            con = self.cnxpool.get_connection()
            cursor = con.cursor()
            try:
                add_url = ("INSERT INTO urls (url, md5, depth) VALUES (%s, %s, %s)")
                data_url = (url, hashlib.md5(url).hexdigest(), depth)
                cursor.execute(add_url, data_url)
                con.commit()
            except mysql.connector.Error as err:
                print('enqueueUrl() ' + err.msg)
                return None
            finally:
	            cursor.close()
	            con.close()


    def dequeueUrl(self):
        con = self.cnxpool.get_connection()
        cursor = con.cursor(dictionary=True)
        try:
            query = ("SELECT `id`, `url`, `depth` FROM urls WHERE status='0' ORDER BY `id` ASC LIMIT 1 FOR UPDATE")
            cursor.execute(query)
            row = cursor.fetchone()

            if row is None:
                return None
            else:
                update_query = ("UPDATE urls SET `status`='1' WHERE `id`=%d") % (row['id'])
                cursor.execute(update_query)
                con.commit()
                return row['url'], row['depth'], row['id']

        except mysql.connector.Error as err:
            print('dequeueUrl() ' + err.msg)
            return None
        finally:
            cursor.close()
            con.close()


    def finishUrl(self, index):
        con = self.cnxpool.get_connection()
        cursor = con.cursor()
        try:
            # we don't need to update done_time using time.strftime('%Y-%m-%d %H:%M:%S') as it's auto updated
            update_query = ("UPDATE urls SET `status`='2', `end_time`='%s' WHERE `id`=%d") % (time.strftime('%Y-%m-%d %H:%M:%S'), index)
            cursor.execute(update_query)
            con.commit()
        except mysql.connector.Error as err:
            print('finishUrl() ' + err.msg)
            return None
        finally:
            cursor.close()
            con.close()


    def product(self, pid, title, pro_price, price, sale_volume, comment_number):
        con = self.cnxpool.get_connection()
        cursor = con.cursor()
        try:
            insert_query = ("""INSERT INTO product(`pid`, `title`, `pro_price`, `price`, `sale_volume`, `comment_number`)
                            values(%d, '%s', '%s', '%s', %d, %d)""" %(pid, title, pro_price, price, sale_volume, comment_number))
            cursor.execute(insert_query)
            con.commit()
        except mysql.connector.Error as err:
            print('product() ' + err.msg)
            return None
        finally:
            cursor.close()
            con.close()


    def comment(self, pid, user, user_level, content, reply, date):
        con = self.cnxpool.get_connection()
        cursor = con.cursor()
        try:
            insert_query = ("""INSERT INTO comment(`pid`, `user`, `user_level`, `content`, `reply`, `date`)
                            values(%d, '%s', '%s', '%s', '%s', '%s')""" %(pid, user, user_level, content, reply, date))
            cursor.execute(insert_query)
            con.commit()
        except mysql.connector.Error as err:
            print('comment() ' + err.msg)
            return None
        finally:
            cursor.close()
            con.close()

'''
create table url(
    id int auto_increment primary key,
    url varchar(128) not null,
    md5 varchar(32) not null,
    status enum('0','1','2') not null default '0',
    depth tinyint not null,
    start_time timestamp not null default current_timestamp,
    end_time timestamp not null
    );


create table product(
    id int auto_increment primary key,
    title varchar(128) not null,
    price decimal(10,2) not null default 0.00,
    pro_price decimal(10,2) not null default 0.00,
    sale_volume int not null default 0,
    comment_number int not null default 0,
    );


create table comment(
    id int auto_increment primary key,
    pid int not null,
    user varchar(32) not null,
    content varchar(512),
    month tinyint not null,
    date tinyint not null
    foreign key(pid) references product(id)
    );
'''