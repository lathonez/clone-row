#! /usr/bin/python
""" Connection wrapper for MySQLdb and psycopg2 """

# internal imports
from subprocess import Popen

# external imports
import MySQLdb
import psycopg2        # pylint: disable=locally-disabled,import-error
import psycopg2.extras # pylint: disable=locally-disabled,import-error

class PDBC(object):
    """ PDBC constructor """

    #
    # PRIVATE methods
    #

    def __init__(self, driver):
        self.con = None
        self.driver = PDBC._get_driver(driver)

    def _is_postgres(self):
        """
        return true if this instance is running against postgres
        """
        return self.driver == PDBC._get_driver('psql')

    @classmethod
    def _get_driver(cls, driver):
        return {
            'mysql': MySQLdb,
            'psql': psycopg2,
        }.get(driver)

    @classmethod
    def _map_connect_args(cls, args):
        """
        map connect args from mysql to psql
        db -> database
        passwd -> password
        """
        ret = {
            'user': args['user'],
            'host': args['host'],
            'port': args['port'],
            'database': args['db']
        }

        if 'passwd' in args:
            ret['password'] = args['passwd']

        return ret

    #
    # PUBLIC methods
    #

    def adapt_param(self, param):
        """
        parse parameter with relevant adapter based on type
        """
        if self._is_postgres():
            if isinstance(param, list) or isinstance(param, dict):
                return psycopg2.extras.Json(param)
        else:
            return param

    def affected_rows(self, cursor):
        """
        mysql doesn't use cursors in RL, so the connection holds that info
        """
        if self._is_postgres():
            return cursor.rowcount
        else:
            return self.con.affected_rows()

    def autocommit(self, autocommit):
        """
        set autocommit on or off depending on passed in autocommit boolean
        """
        if self._is_postgres():
            self.con.set_session(autocommit=autocommit)
        else:
            self.con.autocommit(autocommit)

    def connect(self, args):
        """
        connect to the database
            args: host, user, port, db, password
        """
        if self._is_postgres():
            args = PDBC._map_connect_args(args)

        self.con = self.driver.connect(**args)

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

    def dump(self, args):
        """
            get the appropriate command line args for dumping to disk
            args is a dict containing connection vars, host, user, pass etc
        """
        if self._is_postgres():
            # pg_dump can't doesn't have a where filter
            # psql doesn't do (global) temporary tables
            # this seems like the best solution
            cur = self.cursor()
            select_sql = 'select * from {0} where {1} = %s'.format(
                args['table'], args['column']
            )
            select_sql = cur.mogrify(select_sql, (args['filter'], ))
            copy_sql = 'copy ({0}) to STDOUT'.format(select_sql)
            outfile = open(args['dump_file'], 'wb', 0)
            cur.copy_expert(copy_sql, outfile)
            cur.close()
            outfile.close()
        else:
            dump_args = [
                'mysqldump', '--host', args['host'], '--user', args['user'],
                '--port', args['port'], '--no-create-info', '--databases', args['database'],
                '--tables', args['table'], '--where',
                '{0} = \'{1}\''.format(args['column'], args['filter'])
            ]
            if 'password' in args:
                dump_args.append('-p' + args['password'])

            with open(args['dump_file'], 'wb', 0) as outfile, \
                open(args['error_log'], 'wb', 0) as errfile:
                dump_process = Popen(dump_args, stdout=outfile, stderr=errfile)
            ret = dump_process.wait()

            return ret

    def dict_query(self, sql):
        """
        function to return a dict array [{column: value}] from an sql query
        """
        if self._is_postgres():
            cur = self.con.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(sql)
            if cur.rowcount == 0:
                return []
            if cur.rowcount > 1:
                return ['foo', 'bar']
            row = cur.fetchone()
            cur.close()
            return [row]
        else:
            self.con.query(sql)
            res = self.con.store_result()
            if res.num_rows() == 0:
                return []
            if res.num_rows() > 1:
                return ['foo', 'bar']
            row = res.fetch_row(how=1)

            return [dict(row[0])]

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
        if self._is_postgres():
            # seems postgres does database level encoding
            return self.con.get_parameter_status('server_encoding')
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
        res = self.store_result()
        row = dict(res.fetch_row(how=1)[0])
        return '{0}:{1}'.format(row['character_set_name'], row['collation_name'])

    def get_exception_class(self, exception_class):
        """
        returns the driver's operational error to the calling code
        """
        return {
            'OperationalError': self.driver.OperationalError,
            'ProgrammingError': self.driver.ProgrammingError,
        }.get(exception_class)

    def get_last_executed(self, cursor):
        """
        returns statement last executed by given cursor
        """
        if self._is_postgres():
            return cursor.query
        else:
            # no public access in mysql
            return cursor._last_executed # pylint: disable=locally-disabled,protected-access

    def get_server_info(self):
        """
        straight passthrough
        """
        if self._is_postgres():
            return self.con.server_version
        else:
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

    def validate_dump(self, dump_file):
        """
        validate a file dumped by dump, returning true or false
        dump_file - string filename
        """
        handle = open(dump_file)
        ret = False
        if self._is_postgres():
            num_lines = sum(1 for line in handle)
            if num_lines == 1:
                ret = True
        else:
            # do some basic sanity checking - we should only have one INSERT line
            num_lines = sum(1 for line in handle if line.find('INSERT') == 0)
            if num_lines == 1:
                ret = True

        handle.close()
        return ret
