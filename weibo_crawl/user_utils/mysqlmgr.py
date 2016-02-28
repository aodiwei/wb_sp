# coding=utf-8
import MySQLdb as moduledb
from weibo_crawl import settings

class BusyError(Exception):
    pass

class Cursor:
    def __init__(self, cursor, logger=None):
        self.__cursor = cursor
        self.__logger = logger
        #self.__cursor.execute("START TRANSACTION")

    def execute(self, *args, **kwargs):
        try:
            self.__cursor.execute(*args, **kwargs)
        except Exception, e:
            #self.__cursor.execute("ROLLBACK")
            if len(args) == 1:
                with open(settings.LOG_FILE, "a") as f:
                    f.write(args[0])
                # self.__logger.info(args[0])
            elif len(args) == 2:
                # self.__logger.info(args[0] % args[1])
                with open(settings.LOG_FILE, "a") as f:
                    f.write(args[0] % args[1])
            if self.__logger:
                self.__logger.exception(e)
            raise e

    def executemany(self, *args, **kwargs):
        try:
            if self.__logger and not ("SELECT" in args[0] or "select" in args[0]):
                self.__logger.info(args)
            self.__cursor.executemany(*args, **kwargs)
        except Exception, e:
            #self.__cursor.execute("ROLLBACK")
            if self.__logger:
                self.__logger.error(e)
            raise e

    def fetchone(self, *args, **kwargs):
        return self.__cursor.fetchone(*args, **kwargs)

    def fetchall(self, *args, **kwargs):
        return self.__cursor.fetchall(*args, **kwargs)

    @property
    def lastrowid(self):
        return self.__cursor.lastrowid

    def close(self):
        #self.__cursor.execute("COMMIT")
        self.__cursor.close()

class Conn:
    def __init__(self, logger=None, *args, **kwargs):
        self.__args = args
        self.__kwargs = kwargs
        self.__conn = moduledb.connect(*self.__args, **kwargs)
        self.__conn.autocommit(1)
        self.__logger = logger

    def __connection(self):
        retry_num = 0
        while True:
            try:
                # set max retry times 3 times, when > 3 stop retry and conn = None
                if retry_num > 3:
                    self.__conn = None
                    break
                self.__conn.ping()
                break
            except Exception, e:
                retry_num += 1
                # if self.__logger:
                #     self.__logger.warning("mysql ping failed, reconnecting...")
                self.__conn = moduledb.connect(*self.__args, **self.__kwargs)  # retry
        return self.__conn

    def cursor(self):
        conn = self.__connection()
        return Cursor(conn.cursor(), self.__logger)

    def close(self):
        self.__conn.close()

class MysqlMgr:
    """
         packaging MySQLdb
    """
    def __init__(self, host='localhost', user='root', pwd='root', db='sinacrawl', logger=None):
        """
        """
        self.__host = host
        self.__user = user
        self.__pwd = pwd
        self.__db = db
        self.__charset = 'utf8'
        if logger is not None:
            self.__logger = logger
        else:
            Logger.initialize("scrapy.log", None, withConsole=False, level=logging.DEBUG)
            self.__logger = Logger.getInstance()
        self.__connection = Conn(self.__logger, self.__host, self.__user, self.__pwd, self.__db, charset=self.__charset)

    def get_connection(self):
        return self.__connection

    def close_connection(self):
        self.__connection.close()

    def release_connection(self, conn):
        if self.__logger:
            self.__logger.debug("release connection")
        if conn:
            conn.close()

    def start_transaction(self, cursor):
        if self.__logger:
            self.__logger.debug("start transaction")
        cursor.execute("START TRANSACTION")

    def commit(self, cursor):
        if self.__logger:
            self.__logger.debug("commit")
        cursor.execute("COMMIT")

    def rollback(self, cursor):
        if self.__logger:
            self.__logger.debug("roll back")
        cursor.execute("ROLLBACK")

    @classmethod
    def get_default_mysql_conn(cls, logger=None):
        return MysqlMgr.get_default_mysql(logger).get_connection()

    @classmethod
    def get_default_mysql(cls, logger=None):
        ip = settings.MYSQL_HOST
        username = settings.USER_NAME
        passwd = settings.DB_PASSWORD
        dbname = settings.DATABASE
        try:
            mysqlMgr = MysqlMgr(ip, username, passwd, dbname, logger)
        except Exception, e:
            raise e
        return mysqlMgr

