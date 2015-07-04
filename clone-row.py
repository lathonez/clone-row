#! /usr/local/bin/python
""" TODO - doc """

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
import ConfigParser

def connect(user, host, port, database, password=None):
    """ TODO - doc """
    if password is not None:
        con = mdb.connect(
            host=host,
            user=user,
            db=database,
            port=port,
            passwd=password
        )
    else:
        con = mdb.connect(
            host=host,
            user=user,
            db=database,
            port=port
        )

    version = con.get_server_info()
    print 'Connected to {0}@${1}:{2} - Database version : {3} '.format(
        user, host, database, version
    )

    return con

def get_row(con, table, column_filter, value):
    """ TODO - doc """
    con.query('select * from {0} where {1} = \'{2}\''.format(table, column_filter, value))
    res = con.store_result()
    # we should only _ever_ be playing with one row, per host, at a time
    if res.num_rows() != 1:
        con.close()
        raise 'Only one row expected -- cannot clone on multiple rows!'

    row = res.fetch_row(how=1)
    return dict(row[0])

def check_config_chmod():
    """ make sure the read permissions of clone-row.cfg are set correctly """
    # TODO
    return True

def find_deltas(source_row, target_row):
    """ use DictDiffer to find what's different between target and source """
    delta = DictDiffer(source_row, target_row)
    return {
        'new_columns_in_source': delta.added(),
        'new_columns_in_target': delta.removed(),
        'delta_rows': delta.changed(),
        'unchanged_rows': delta.unchanged()
    }

def get_column_sql(con, table, column):
    """ return sql to add or drop a given column from a given table """
    drop_sql = 'alter table `{0}` drop column `{1}`;'.format(table, column)
    con.query('show fields from {0} where field = \'{1}\''.format(table, column))
    res = con.store_result()
    if res.num_rows() != 1:
        con.close()
        raise 'Only one row expected -- cannot display sql for multiple rows!'
    column_info = dict(res.fetch_row(how=1)[0])
    not_null = '' if column_info['Null'] == 'yes' else ' not null'
    default = '' if column_info['Default'] is None else ' default ' + column_info['Default']
    add_sql = 'alter table `{0}` add column `{1}` {2}{3}{4};'.format(
        table,
        column,
        column_info['Type'],
        default,
        not_null
    )
    return {
        'add_sql': add_sql,
        'drop_sql': drop_sql
    }

def show_ddl_updates(mode, con, table, deltas):
    """ display SQL statements to adjust database for column deltas
        mode: (source|target)
        con: database connection (for source or target)
        table: table we're working on
        deltas: column differences
    """
    working_db = mode
    other_db = 'source' if working_db == 'target' else 'source'

    for column in deltas:
        print '\n|----------------------|column: {0}|----------------------|\n'.format(column)
        print '\'{0}\' exists in the {1} database but not in the source {2}\n'.format(
            column, working_db, other_db
        )
        info = get_column_sql(con, table, column)
        print 'ADD: to add column \'{0}\' to \'{1}\', run the following SQL:\n'.format(
            column, other_db)
        print info['add_sql'], '\n'
        print 'DROP: to drop column \'{0}\' from \'{1},/\' run the following SQL:\n'.format(
            column, working_db)
        print info['drop_sql'], '\n'
        print '|-----------------------{0}-----------------------|'.format(
            '-' * len('column: ' + column)
        )

def main():
    """ TODO - doc """

    if not check_config_chmod:
        raise 'clone-row.cfg needs to be secure\n\nchmod 0600 clone-row.cfg\n\n'

    config = ConfigParser.ConfigParser(allow_no_value=True)
    config.readfp(open('clone-row.cfg'))
    print 'connecting to source database..'
    source_con = connect(
        config.get('source_db', 'username'),
        config.get('source_db', 'host'),
        config.getint('source_db', 'port'),
        config.get('command_line_args', 'database'),
        config.get('source_db', 'password')
    )
    print 'connecting to target database..'
    target_con = connect(
        config.get('target_db', 'username'),
        config.get('target_db', 'host'),
        config.getint('target_db', 'port'),
        config.get('command_line_args', 'database'),
        config.get('target_db', 'password')
    )
    print 'getting source row..'
    source_row = get_row(
        source_con,
        config.get('command_line_args', 'table'),
        config.get('command_line_args', 'column'),
        config.get('command_line_args', 'column_filter')
    )
    print 'getting target row..'
    target_row = get_row(
        target_con,
        config.get('command_line_args', 'table'),
        config.get('command_line_args', 'column'),
        config.get('command_line_args', 'column_filter')
    )
    print 'finding deltas..'
    deltas = find_deltas(source_row, target_row)
    show_ddl_updates(
        'source',
        source_con,
        config.get('command_line_args', 'table'),
        deltas["new_columns_in_source"]
    )
    show_ddl_updates(
        'target',
        target_con,
        config.get('command_line_args', 'table'),
        deltas['new_columns_in_target']
    )

    print 'housekeeping..'
    source_con.close()
    target_con.close()

main()