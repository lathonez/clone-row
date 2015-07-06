#! /usr/local/bin/python
"""
    Credit for DictDiffer to hughdbrown:
    https://github.com/hughdbrown/dictdiffer
"""
class DictDiffer(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values
    """
    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)
    def added(self):
        """ doc """
        return self.set_current - self.intersect
    def removed(self):
        """ doc """
        return self.set_past - self.intersect
    def changed(self):
        """ doc """
        return set(o for o in self.intersect if self.past_dict[o] != self.current_dict[o])
    def unchanged(self):
        """ doc """
        return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])

import MySQLdb as mdb
import ConfigParser, time, datetime

class CloneRow(object):
    """ TODO : DOC """

    # instance variables
    config = None
    database = None
    source_con = None
    target_con = None
    table = None
    column = None
    column_filter = None
    target_insert = False

    def __init__(self):

        self.config = ConfigParser.ConfigParser(allow_no_value=True)
        self.config.readfp(open('CloneRow.cfg'))
        self.database = self.config.get('command_line_args', 'database')
        self.table = self.config.get('command_line_args', 'table')
        self.column = self.config.get('command_line_args', 'column')
        self.column_filter = self.config.get('command_line_args', 'column_filter')

    def error(self, message):
        """ wrapper for raising errors that does housekeeping too """
        self.housekeep()
        raise Exception('FATAL: ' + message)

    def connect(self, user, host, port, password=None):
        """ TODO - doc """
        if password is not None:
            con = mdb.connect(
                host=host,
                user=user,
                db=self.database,
                port=port,
                passwd=password
            )
        else:
            con = mdb.connect(
                host=host,
                user=user,
                db=self.database,
                port=port
            )

        version = con.get_server_info()
        print 'Connected to {0}@${1}:{2} - Database version : {3} '.format(
            user, host, self.database, version
        )

        return con

    def get_row(self, con):
        """ TODO - doc """
        # we're not using cursors here because we want the nice object with column headers
        select_sql = 'select * from {0} where {1} = {2}'.format(
            self.table,
            self.column,
            self.quote_sql_param(con.escape_string(self.column_filter))
        )
        con.query(select_sql)
        res = con.store_result()
        # we should only _ever_ be playing with one row, per host, at a time
        if res.num_rows() == 0:
            return None
        if res.num_rows() != 1:
            self.error('get_row: Only one row expected -- cannot clone on multiple rows!')

        row = res.fetch_row(how=1)
        return dict(row[0])

    @classmethod
    def check_config_chmod(cls):
        """ make sure the read permissions of clone-row.cfg are set correctly """
        # TODO
        return True

    @classmethod
    def find_deltas(cls, source_row, target_row):
        """ use DictDiffer to find what's different between target and source """
        delta = DictDiffer(source_row, target_row)
        return {
            'new_columns_in_source': delta.added(),
            'new_columns_in_target': delta.removed(),
            'delta_columns': delta.changed(),
            'unchanged_columns': delta.unchanged()
        }

    @classmethod
    def quote_sql_param(cls, sql_param):
        """ 'quote' a param if necessary, else return it as is.
            param should be escaped (Connection.escape_string) before it's passed in
            we should use cursors and parameterisation where possible, but sometimes we
            need to use the Connection.query method, so this is necessary
        """
        if isinstance(sql_param, str) or isinstance(sql_param, datetime.datetime):
            return '\'{0}\''.format(sql_param)
        else:
            # doesn't need quoting
            return sql_param

    def get_column_sql(self, con, column):
        """ return sql to add or drop a given column from the table we're working on """
        drop_sql = 'alter table `{0}` drop column `{1}`;'.format(self.table, column)
        con.query('show fields from {0} where field = \'{1}\''.format(self.table, column))
        res = con.store_result()
        if res.num_rows() != 1:
            self.error('get_column_sql: only one row expected!')
        column_info = dict(res.fetch_row(how=1)[0])
        not_null = '' if column_info['Null'] == 'yes' else ' not null'
        default = '' if column_info['Default'] is None else ' default ' + column_info['Default']
        add_sql = 'alter table `{0}` add column `{1}` {2}{3}{4};'.format(
            self.table,
            column,
            column_info['Type'],
            default,
            not_null
        )
        return {
            'add_sql': add_sql,
            'drop_sql': drop_sql
        }

    def show_ddl_updates(self, mode, deltas):
        """ display SQL statements to adjust database for column deltas
            mode: (source|target)
            con: database connection (for source or target)
            table: table we're working on
            deltas: column differences
        """
        working_db = mode
        other_db = 'source' if working_db == 'target' else 'source'
        con = self.source_con if working_db == 'source' else self.target_con

        for column in deltas:
            print '\n|----------------------|column: {0}|----------------------|\n'.format(column)
            print '\'{0}\' exists in the {1} database but not in the source {2}\n'.format(
                column, working_db, other_db
            )
            info = self.get_column_sql(con, column)
            print 'ADD: to add column \'{0}\' to \'{1}\', run the following SQL:\n'.format(
                column, other_db)
            print info['add_sql'], '\n'
            print 'DROP: to drop column \'{0}\' from \'{1}\', run the following SQL:\n'.format(
                column, working_db)
            print info['drop_sql'], '\n'
            print '|-----------------------{0}-----------------------|'.format(
                '-' * len('column: ' + column)
            )

    def update_target(self, source_row, deltas):
        """ update the data in the target database with differences from source """
        if not len(deltas):
            return
        cur = self.target_con.cursor()
        # generate update sql for everything in the deltas
        for column in deltas:
            # doing updates one by one is just easier and more readable
            update_sql = "update {0} set {1} = %s where {2} = %s".format(
                self.table,
                column,
                self.column
            )
            # run the update
            print 'updating {0}.{1}'.format(self.table, column)
            cur.execute(update_sql, (source_row[column], self.column_filter,))
            if self.target_con.affected_rows() != 1:
                self.target_con.rollback()
                cur.close()
                self.error('update_target: expected to update a single row')
        # don't commit anything until all updates have gone in ok
        cur.close()
        self.target_con.commit()
        return

    def unload_target(self):
        """ unload the row we're working on from the target_db in case we ruin it """
        cur = self.target_con.cursor()
        unload_file = self.config.get('backup', 'unload_dir')
        unload_file += '/{0}-{1}-{2}-{3}'.format(
            self.table,
            self.column,
            self.column_filter,
            int(round(time.time() * 1000))
        )
        cur.execute('select * into outfile \'{0}\' from {1} where {2} = %s'.format(
            unload_file,
            self.table,
            self.column
        ), (self.column_filter, ))
        if self.target_con.affected_rows() != 1:
            self.error('unload_target: unable to verify unload file')
        return unload_file

    def restore_target(self, unload_file):
        """ restore data unloaded from unload_target """
        cur = self.target_con.cursor()
        delete_sql = 'delete from {0} where {1} = %s'.format(
            self.table,
            self.column
        )
        cur.execute(delete_sql, (self.column_filter, ))
        if self.target_con.affected_rows() != 1:
            cur.close()
            self.target_con.rollback()
            self.error('restore_target: expected to delete only one row')
        if self.target_insert:
            print 'not restoring from backup as target was inserted from scratch'
            cur.close()
            self.target_con.commit()
            return
        restore_sql = 'load data infile \'{0}\' into table {1}'.format(
            unload_file,
            self.table
        )
        cur.execute(restore_sql)
        if self.target_con.affected_rows() != 1:
            cur.close()
            self.target_con.rollback()
            self.error('restore_target: expected to load only one row')
        cur.close()
        self.target_con.commit()

    @classmethod
    def print_delta_columns(cls, deltas):
        """ helper function to print out columns which will be updated """
        print '\n\n|----------------------------------------------------------|'
        print 'The following columns will be updated in the target db:'
        for column in deltas:
            print '\t- ' + column
        print '|----------------------------------------------------------|'

    @classmethod
    def user_happy(cls):
        """ Give the user a chance to restore from backup easily beforer we terminate """
        print 'Row has been cloned successfully..'
        print 'Type \'r\' to (r)estore from backup, anything else to termiate'
        descision = raw_input()
        if descision == 'r':
            print 'restoring from backup..'
            return False
        else:
            print 'have a fantastic day..'
            return True

    def minimal_insert(self):
        """ insert as little data as possible into the target database
            this will allow us to reselect and continue as normal if
            the row doesn't exist at all
            TODO - we could find all the columns that require default values
                   and spam defaults of the appropriate datatype in there..
        """
        cur = self.target_con.cursor()
        insert_sql = 'insert into {0} ({1}) values (%s)'.format(
            self.table,
            self.column
        )
        cur.execute(insert_sql, (self.column_filter,))
        if self.target_con.affected_rows() != 1:
            cur.close()
            self.error('somehow we\'ve inserted multiple rows')
        # now we have a row, we can return it as usual
        return self.get_row(self.target_con)

    def housekeep(self):
        """ close connections / whatever else """
        print 'housekeeping..'
        self.source_con.close()
        self.target_con.close()

    def main(self):
        """ TODO - doc """
        if not self.check_config_chmod:
            raise 'clone-row.cfg needs to be secure\n\nchmod 0600 clone-row.cfg\n\n'
        print 'connecting to source database..'
        self.source_con = self.connect(
            self.config.get('source_db', 'username'),
            self.config.get('source_db', 'host'),
            self.config.getint('source_db', 'port'),
            self.config.get('source_db', 'password')
        )
        print 'connecting to target database..'
        self.target_con = self.connect(
            self.config.get('target_db', 'username'),
            self.config.get('target_db', 'host'),
            self.config.getint('target_db', 'port'),
            self.config.get('target_db', 'password')
        )
        # we don't want mysql commit stuff unless we've okay'd it
        self.target_con.autocommit(False)
        print 'getting source row..'
        source_row = self.get_row(self.source_con)
        if source_row is None:
            self.error('row does not exist in source database')
        print 'getting target row..'
        target_row = self.get_row(self.target_con)
        if target_row is None:
            print 'row does not exist at all in target, running a minimal insert..'
            self.target_insert = True
            target_row = self.minimal_insert()
        print 'finding deltas..'
        deltas = self.find_deltas(source_row, target_row)
        self.show_ddl_updates('source', deltas['new_columns_in_source'])
        self.show_ddl_updates('target', deltas['new_columns_in_target'])
        if not len(deltas['delta_columns']):
            print '\ndata is identical in target and source, nothing to do..'
            self.housekeep()
            return True
        self.print_delta_columns(deltas['delta_columns'])
        print 'backing up target row..'
        backup = self.unload_target()
        print 'backup file can be found at {0} on the target host'.format(backup)
        print self.update_target(source_row, deltas['delta_columns'])
        if not self.user_happy():
            self.restore_target(backup)
        self.housekeep()

DOLLY = CloneRow()
DOLLY.main()
