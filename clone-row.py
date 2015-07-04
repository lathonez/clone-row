#! /usr/local/bin/python
""" TODO - doc """

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

    cur = con.cursor()
    cur.execute("select version()")

    version = cur.fetchone()
    cur.close()
    print "Connected to {0}@${1}:{2} - Database version : {3} ".format(
        user, host, database, version
    )

    return con

def get_row(con, table, column_filter, value):
    """ TODO - doc """
    cur = con.cursor()
    cur.execute("select * from {0} where {1} = '{2}'".format(table, column_filter, value))
    # we should only _ever_ be playing with one row, per host, at a time
    if con.affected_rows() != 1:
        cur.close()
        con.close()
        raise 'Only one row expected -- cannot match multiple rows!'
    row = cur.fetchone()
    cur.close()
    return row

def check_config_chmod():
    """ make sure the read permissions of clone-row.cfg are set correctly """
    # TODO
    return True

def main():
    """ TODO - doc """

    if not check_config_chmod:
        raise 'clone-row.cfg needs to be secure\n\nchmod 0600 clone-row.cfg\n\n'

    config = ConfigParser.ConfigParser(allow_no_value=True)
    config.readfp(open('clone-row.cfg'))
    source_con = connect(
        config.get('source_db', 'username'),
        config.get('source_db', 'host'),
        config.getint('source_db', 'port'),
        config.get('command_line_args', 'database'),
        config.get('source_db', 'password')
    )
    target_con = connect(
        config.get('target_db', 'username'),
        config.get('target_db', 'host'),
        config.getint('target_db', 'port'),
        config.get('command_line_args', 'database'),
        config.get('target_db', 'password')
    )
    source_row = get_row(
        source_con,
        config.get('command_line_args', 'table'),
        config.get('command_line_args', 'column'),
        config.get('command_line_args', 'column_filter')
    )
    target_row = get_row(
        target_con,
        config.get('command_line_args', 'table'),
        config.get('command_line_args', 'column'),
        config.get('command_line_args', 'column_filter')
    )

    for key in source_row:
        print key

    for key in target_row:
        print key

    source_con.close()
    target_con.close()

main()
