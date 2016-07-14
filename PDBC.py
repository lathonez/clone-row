#! /usr/bin/python
""" Connection wrapper for MySQLdb and PSQLdb """

# external imports
import MySQLdb

class PDBC(object):
    """ PDBC constructor """

    #
    # PRIVATE methods
    #

    def __init__(self, driver):
        self.con = None
        self.driver = PDBC._get_driver(driver)

    @classmethod
    def _get_driver(cls, driver):
        return {
            'mysql': MySQLdb,
            'psql': None,
        }.get(driver)

    #
    # PUBLIC methods
    #

    def affected_rows(self):
        """
        straight passthrough
        """
        return self.con.affected_rows()

    def autocommit(self, autocommit):
        """
        straight passthrough
        """
        self.con.autocommit(autocommit)

    def connect(self, args):
        """
        connect to the database
            args: host, user, port, db, password
        """
        self.con = self.driver.connect(**args)
        return self.con.get_server_info()

    def close(self):
        """
        straight passthrough
        """
        return self.con.close()

    def commit(self):
        """
        straight passthrough
        """
        return self.con.commit()

    def cursor(self):
        """
        straight passthrough
        """
        return self.con.cursor()

    def escape_string(self, string):
        """
        straight passthrough
        """
        return self.con.escape_string(string)

    def get_column_sql(self, table, column):
        """
        return sql to add or drop a given column from the table passed in on the command line

        Keyword arguments:
        con -- MySQLdb.connection object of the database we're using (source or target)
        column -- the column to return sql for
        """
        drop_sql = 'alter table `{0}` drop column `{1}`;'.format(table, column)
        # using con.query instead of con.cursor.execute gives us a nicer return format
        self.query('show fields from {0} where field = \'{1}\''.format(table, column))
        res = self.store_result()
        # using how=1 and parsing into a dict lets us manipulate with the returned data easily
        column_info = dict(res.fetch_row(how=1)[0])
        not_null = '' if column_info['Null'] == 'YES' else ' not null'
        default = '' if column_info['Default'] is None else ' default ' + column_info['Default']
        add_sql = 'alter table `{0}` add column `{1}` {2}{3}{4};'.format(
            table, column, column_info['Type'], default, not_null
        )
        return {
            'add_sql': add_sql,
            'drop_sql': drop_sql
        }

    def get_encoding(self, database, table):
        """
        return character set and collation for the table we're working on
        """
        sql = """select
                ccsa.character_set_name,
                ccsa.collation_name
            from
                information_schema.tables t,
                information_schema.collation_character_set_applicability ccsa
            where
                ccsa.collation_name = t.table_collation and
                t.table_schema = '{0}' and
                t.table_name = '{1}';
            """
        sql = sql.format(database, table)
        self.query(sql)
        return self.store_result()

    def get_exception_class(self, exception_class):
        """
        returns the driver's operational error to the calling code
        """
        return {
            'OperationalError': self.driver.OperationalError,
            'ProgrammingError': self.driver.ProgrammingError,
        }.get(exception_class)

    def get_server_info(self):
        """
        straight passthrough
        """
        return self.con.get_server_info()

    def query(self, sql):
        """
        straight passthrough
        """
        return self.con.query(sql)

    def rollback(self):
        """
        straight passthrough
        """
        return self.con.rollback()

    def store_result(self):
        """
        straight passthrough
        """
        return self.con.store_result()
