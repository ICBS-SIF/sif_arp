"""Database Layer to insert and fetch the ask bid prices"""
import mysql.connector
import configparser
from BrokerFeeder.Database import Queries


class DataBaseTemplate:
    def __init__(self):
        """Innit will establish the connect and creat a instance to keep it alive."""
        self.__config = configparser.ConfigParser()
        self.__config.read('../properties/arp_app.ini')
        hostname = self.__config['database']['hostname']
        user = self.__config['database']['user']
        password = self.__config['database']['password']
        database = self.__config['database']['database']
        self.db_instance = mysql.connector.connect(host=hostname,
                                                   user=user,
                                                   password=password,
                                                   database=database)
        # TODO: Encrypt and decrypt password
        self.db_cursor = self.db_instance.cursor(dictionary=True)

    def insert_data(self, timestamp, req_type, price):
        """Inserts into the table.
        :return 1 if successful
        :return None if unsuccessful (also rollback)"""
        try:
            self.db_cursor.execute(Queries.INSERT_BA.format(timestamp=timestamp, req_type=req_type, price=price))
            self.db_instance.commit()
        except mysql.connector.Error as e:
            print(e.errno)
            self.db_instance.rollback()
        finally:
            return 1

    def fetch_last_n(self, req_type, n_rows):
        """Fetch n instances of the req_type
        :param req_type: either ASK or BID
        :param n_rows: last n prices"""
        try:
            self.db_cursor.execute(Queries.FETCH_LAST_FIVE.format(req_type=req_type, n=n_rows))
            result = self.db_cursor.fetchall()
            return [x['PRICE'] for x in result]
        except mysql.connector.Error as e:
            print(e.errno)
            return None
