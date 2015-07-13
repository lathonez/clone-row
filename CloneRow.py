#! /usr/local/bin/python
""" Python module for cloning a MYSQL row from one host to another """

import MySQLdb as mdb
import argparse, coloredlogs, ConfigParser, datetime, logging, time, os, stat, sys
from DictDiffer import DictDiffer

class CloneRow(object):
    """ CloneRow constructor, doesn't take any args """
    # TODO:
    #   - pretty logging
    #   - catch the usual mysql errors and print nicely
    #   - better documentation with params etc
    #   - read everything and comment out where necessary
    #   - redo example config
    #   - README.md

    def __init__(self):
        log_format = '[%(asctime)s]: %(message)s'
        log_level = logging.DEBUG
        coloredlogs.install(show_hostname=False, show_name=False, show_severity=False)
        logging.basicConfig(format=log_format, level=log_level)
        self.config = ConfigParser.ConfigParser(allow_no_value=True)
        self.config.readfp(open('CloneRow.cfg'))
        self.source = {
            'alias': None,
            'print_alias': None,
            'connection': None,
            'row': None
        }
        self.target = {
            'alias': None,
            'print_alias': None,
            'connection': None,
            'row': None,
            'backup': None
        }
        self.database = {
            'table': None,
            'column': None,
            'filter': None,
            'ignore_columns': [],
            'deltas': None
        }
        self.target_insert = False

    #
    # PRIVATE classmethods
    #

    @classmethod
    def _quote_sql_param(cls, sql_param):
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

    #
    # PUBLIC classmethods
    #

    @classmethod
    def check_config_chmod(cls):
        """ make sure the read permissions of CloneRow.cfg are set correctly """
        chmod = oct(stat.S_IMODE(os.stat('CloneRow.cfg').st_mode))
        if chmod != '0600':
            logging.error('CloneRow.cfg needs to be secure: `chmod 0600 CloneRow.cfg`')
            sys.exit(1)

    #
    # PRIVATE methods
    #

    def _connect(self, host_alias):
        """ connect to a mysql database, returning a MySQLdb.Connection object """
        logging.info('attempting to connect to %s..', host_alias)
        hostname = self.config.get('host.' + host_alias, 'hostname')
        user = self.config.get('host.' + host_alias, 'username')
        port = self.config.getint('host.' + host_alias, 'port')
        database = self.config.get('host.' + host_alias, 'database')
        password = self.config.get('host.' + host_alias, 'password')
        if password is not None:
            con = mdb.connect(
                host=hostname,
                user=user,
                db=database,
                port=port,
                passwd=password
            )
        else:
            con = mdb.connect(
                host=hostname,
                user=user,
                db=database,
                port=port
            )
        version = con.get_server_info()
        logging.info(
            'connected to %s@%s:%s - Database version : %s',
            user, host_alias, database, version
        )
        return con

    def _dump_update_sql(self, cursor):
        """ dump the last executed update statement to a file """
        # naughty naughty, accessing a protected member.. show me a better way
        logging.info('dumping update sql to disk..')
        sql = cursor._last_executed
        sql_file = self.config.get('clone_row', 'unload_filepath') + '.sql'
        with open(sql_file, "w") as outfile:
            outfile.write(sql)
        logging.warning('update sql is available for inspection at %s on this machine', sql_file)

    def _error(self, message):
        """ wrapper for raising errors that does housekeeping too """
        self._housekeep()
        raise Exception('FATAL: ' + message)

    def _get_column_sql(self, con, column):
        """ return sql to add or drop a given column from the table we're working on """
        drop_sql = 'alter table `{0}` drop column `{1}`;'.format(self.database['table'], column)
        con.query('show fields from {0} where field = \'{1}\''.format(
            self.database['table'],
            column
        ))
        res = con.store_result()
        if res.num_rows() != 1:
            self._error('get_column_sql: only one row expected!')
        column_info = dict(res.fetch_row(how=1)[0])
        not_null = '' if column_info['Null'] == 'YES' else ' not null'
        default = '' if column_info['Default'] is None else ' default ' + column_info['Default']
        add_sql = 'alter table `{0}` add column `{1}` {2}{3}{4};'.format(
            self.database['table'],
            column,
            column_info['Type'],
            default,
            not_null
        )
        return {
            'add_sql': add_sql,
            'drop_sql': drop_sql
        }

    def _get_log_break(self, string=''):
        """ return |-------{string}--------| always same length """
        length = self.config.getint('clone_row', 'log_header_length')
        sep = '-'
        end = '|'
        n_seps = (length - len(string) - 2) / 2
        log_header = end + (sep * n_seps) + string + (sep * n_seps)
        if len(log_header) < length -1:
            # uneven length
            log_header += sep
        log_header += end
        return log_header

    def _get_row(self, host):
        """ Run a select query (MYSQLdb.Connection.query)
            returning a dict including column headers.
            Should always return a single row.
        """
        logging.info('getting %s row..', host['alias'])
        con = host['connection']
        if self.config.getboolean('clone_row', 'schema_only'):
            # if we're only doing schema diffs we don't care about columns or filters
            select_sql = 'select * from {0} limit 1'.format(self.database['table'])
        else:
            # we're not using cursors here because we want the nice object with column headers
            select_sql = 'select * from {0} where {1} = {2}'.format(
                self.database['table'],
                self.database['column'],
                self._quote_sql_param(con.escape_string(self.database['filter']))
            )
        con.query(select_sql)
        res = con.store_result()
        # we should only _ever_ be playing with one row, per host, at a time
        if res.num_rows() == 0:
            return None
        if res.num_rows() != 1:
            self._error('get_row: Only one row expected -- cannot clone on multiple rows!')
        row = res.fetch_row(how=1)
        return dict(row[0])

    def _get_table_config(self, table):
        """ get table specific config items, if any """
        table_section = 'table.' + table
        if not self.config.has_section(table_section):
            logging.warning('no table specific config defined for %s', table)
            return
        try:
            self.database['ignore_columns'] = self.config.get(
                table_section, 'ignore_columns'
            ).rsplit(',')
            ignore_string = 'The following columns will be ignored: '
            for column in self.database['ignore_columns']:
                ignore_string += column + ' '
            logging.warning(ignore_string)
        except ConfigParser.NoOptionError:
            logging.warning('_get_table_config: no ignore_columns for %s', table)
            return

    def _get_unload_filepath(self):
        """ return the unload filepath for us to use for backups and sql dumps """
        unload_file = self.config.get('clone_row', 'unload_dir')
        unload_file += '/{0}-{1}-{2}-{3}'.format(
            self.database['table'],
            self.database['column'],
            self.database['filter'],
            int(round(time.time() * 1000))
        )
        return unload_file

    def _housekeep(self):
        """ close connections / whatever else """
        logging.info('housekeeping..')
        if self.source['connection'] is not None:
            self.source['connection'].close()
        if self.target['connection'] is not None:
            self.target['connection'].close()

    def _print_delta_columns(self, deltas):
        """ helper function to print out columns which will be updated """
        logging.info(self._get_log_break('|Delta Columns|'))
        logging.info('  The following columns will be updated on ' + self.target['alias'])
        for column in deltas:
            if column in self.database['ignore_columns']:
                continue
            logging.info('    -%s ', column)
        logging.info(self._get_log_break())

    def _restore_target(self):
        """ restore data unloaded from unload_target """
        cur = self.target['connection'].cursor()
        delete_sql = 'delete from {0} where {1} = %s'.format(
            self.database['table'],
            self.database['column']
        )
        cur.execute(delete_sql, (self.database['filter'], ))
        if self.target['connection'].affected_rows() != 1:
            cur.close()
            self.target['connection'].rollback()
            self._error('restore_target: expected to delete only one row')
        if self.target_insert:
            logging.warning('deleting (not restoring) as target row was inserted from scratch')
            cur.close()
            self.target['connection'].commit()
            return
        restore_sql = 'load data infile \'{0}\' into table {1}'.format(
            self.target['backup'],
            self.database['table']
        )
        cur.execute(restore_sql)
        if self.target['connection'].affected_rows() != 1:
            cur.close()
            self.target['connection'].rollback()
            self._error('restore_target: expected to load only one row')
        cur.close()
        self.target['connection'].commit()

    def _unload_target(self):
        """ unload the row we're working on from the target_db in case we ruin it """
        logging.info('backing up target row..')
        cur = self.target['connection'].cursor()
        unload_file = self.config.get('clone_row', 'unload_filepath') + '.backup'
        cur.execute('select * into outfile \'{0}\' from {1} where {2} = %s'.format(
            unload_file,
            self.database['table'],
            self.database['column']
        ), (self.database['filter'], ))
        if self.target['connection'].affected_rows() != 1:
            self._error('unload_target: unable to verify unload file')
        logging.warning('backup file can be found at %s on %s', unload_file, self.target['alias'])
        return unload_file

    #
    # PUBLIC methods
    #

    def exit(self):
        """ wrapper for exiting the script successfully """
        logging.info('operation completed successfully, have a fantastic day')
        self._housekeep()
        sys.exit(0)

    def find_deltas(self):
        """ use DictDiffer to find what's different between target and source """
        logging.info('finding deltas..')
        delta = DictDiffer(self.source['row'], self.target['row'])
        self.database['deltas'] = {
            'new_columns_in_source': delta.added(),
            'new_columns_in_target': delta.removed(),
            'delta_columns': delta.changed(),
            'unchanged_columns': delta.unchanged()
        }

    def get_rows(self):
        """ get rows from soure and target """
        self.source['row'] = self._get_row(self.source)
        self.target['row'] = self._get_row(self.target)
        # we really need a source row..
        if self.source['row'] is None:
            self._error('get_rows: no row found in {0} database'.format(self.source['alias']))

    def insert_target(self):
        """ insert as little data as possible into the target database, if nothing exists
            there already. This allows us to reselect and continue as normal
        """
        # TODO - we could find all the columns that require default values
        #        and spam defaults of the appropriate datatype in there..
        if self.target['row'] is not None:
            # we only need to do this if there's no target row
            return
        logging.info('inserting a minimal row into target database..')
        cur = self.target['connection'].cursor()
        insert_sql = 'insert into {0} ({1}) values (%s)'.format(
            self.database['table'],
            self.database['column']
        )
        cur.execute(insert_sql, (self.database['filter'],))
        if self.target['connection'].affected_rows() != 1:
            cur.close()
            self._error('somehow we\'ve inserted multiple rows')
        # now we have a row, we can return it as usual
        self.target['row'] = self._get_row(self.target)

    def parse_cla(self):
        """ parse command line arguments """
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        aliases = [section for section in self.config.sections() if 'host.' in section]
        parser.add_argument(
            '--schema_only', '-s',
            help='diff schema only, do not consider data (column and filter not required)',
            action='store_true',
            default=False
        )
        parser.add_argument(
            'source_alias',
            help='source host alias (for host.* config section)',
            choices=[alias[5:] for alias in aliases]
        )
        parser.add_argument(
            'target_alias',
            help='target host alias (for host.* section)',
            choices=[alias[5:] for alias in aliases]
        )
        parser.add_argument('table', help='table to consider: select from <table>')
        parser.add_argument(
            'column',
            nargs='?',
            help='column to consider: select from table where <column>'
        )
        parser.add_argument(
            'filter',
            nargs='?',
            help='value to filter column: select from table where column = <filter>'
        )
        args = parser.parse_args()
        if not args.schema_only and (args.column is None or args.filter is None):
            print '\n', \
                'column & filter arguments must be supplied unless running with --schema_only/-s\n'
            parser.print_help()
            sys.exit(0)
        self.source['alias'] = args.source_alias
        self.target['alias'] = args.target_alias
        if self.source['alias'] == self.target['alias']:
            self._error('source and target alias are identical')
        self.database['table'] = args.table
        self.database['column'] = args.column
        self.database['filter'] = args.filter
        self._get_table_config(self.database['table'])
        self.config.set('clone_row', 'unload_filepath', self._get_unload_filepath())
        self.config.set('clone_row', 'schema_only', str(args.schema_only))

    def print_restore_sql(self):
        """ tell the user how to rollback by hand after script has run """
        restore_manual_sql = '  begin;\n'
        restore_manual_sql += '  delete from {0} where {1} = {2};\n'.format(
            self.database['table'],
            self.database['column'],
            self._quote_sql_param(self.database['filter'])
        )
        restore_manual_sql += '  -- the above should have delete one row, '
        restore_manual_sql += 'if not, run: rollback;\n'
        restore_manual_sql += '  load data infile \'{0}\' into table {1};\n'.format(
            self.target['backup'], self.database['table']
        )
        restore_manual_sql += '  commit;\n'
        logging.warning(self._get_log_break())
        logging.warning('To rollback manually, run the following sql on %s', self.target['alias'])
        logging.warning(restore_manual_sql)
        return

    def set_connections(self):
        """ setup soure and target connection objects """
        self.source['connection'] = self._connect(self.source['alias'])
        self.target['connection'] = self._connect(self.target['alias'])
        # we don't want mysql commit stuff unless we've okay'd it
        self.target['connection'].autocommit(False)

    def show_schema_updates(self):
        """ display SQL statements to adjust database for column deltas """
        for mode in ['source', 'target']:
            deltas = self.database['deltas']['new_columns_in_' + mode]
            working_db = self.source['alias'] if mode == 'source' else self.target['alias']
            other_db = self.target['alias'] if mode == 'source' else self.source['alias']
            con = self.source['connection'] \
                if working_db == self.source['alias'] else self.target['connection']

            for column in deltas:
                logging.info(self._get_log_break('|column: {0}|'.format(column)))
                logging.info(
                    '    \'%s\' exists in the %s database but not in %s',
                    column, working_db, other_db
                )
                info = self._get_column_sql(con, column)
                logging.info(
                    '    ADD COLUMN: \'%s\' to %s, run the following SQL on %s:',
                    column, other_db, other_db
                )
                logging.warning('    ' + info['add_sql'])
                logging.info(
                    '    DROP COLUMN: \'%s\' from %s, run the following SQL on %s:',
                    column, working_db, working_db
                )
                logging.warning('    ' + info['drop_sql'])
                logging.info(self._get_log_break())

        if self.config.getboolean('clone_row', 'schema_only'):
            # we're done if only diffing schema
            self.exit()

    def update_target(self):
        """ update the data in the target database with differences from source """
        delta_columns = self.database['deltas']['delta_columns']
        if not len(delta_columns):
            logging.warning('data is identical in target and source, nothing to do..')
            self.exit()
        if set(delta_columns). \
            issubset(set(self.database['ignore_columns'])):
            logging.warning('all changes are configured to be ignored, nothing to do..')
            self.exit()
        self._print_delta_columns(delta_columns)
        self.target['backup'] = self._unload_target()
        cur = self.target['connection'].cursor()
        update_sql = None
        update_params = []
        # generate update sql for everything in the deltas
        columns = [c for c in delta_columns if c not in self.database['ignore_columns']]
        for column in columns:
            if not update_sql:
                update_sql = 'update {0} set {1} = %s'.format(self.database['table'], column)
            else:
                update_sql += ', {0} = %s'.format(column)
            update_params.append(self.source['row'][column])
        update_sql += ' where {0} = %s'.format(self.database['column'])
        update_params.append(self.database['filter'])
        # run the update
        cur.execute(update_sql, tuple(update_params))
        self._dump_update_sql(cur)
        if self.target['connection'].affected_rows() != 1:
            self.target['connection'].rollback()
            cur.close()
            self._error('update_target: expected to update a single row')
        # don't commit anything until all updates have gone in ok
        cur.close()
        self.target['connection'].commit()
        return

    def user_happy(self):
        """ Give the user a chance to restore from backup easily beforer we terminate """
        logging.info('Row has been cloned successfully..')
        logging.warning('Type \'r\' to (r)estore from backup, anything else to exit')
        descision = raw_input()
        if descision == 'r':
            logging.warning('restoring from backup..')
            self._restore_target()
            return False
        return True

#
#   main execution path
#   https://en.wikipedia.org/wiki/Dolly_(sheep)
#
DOLLY = CloneRow()
# make sure the config file has correct permissions (0600)
DOLLY.check_config_chmod()
# parse command line arguments from the user
DOLLY.parse_cla()
# establish a connection to source and target databases
DOLLY.set_connections()
# grab a single row from both databases
DOLLY.get_rows()
# if no row exists in the target, insert it here
DOLLY.insert_target()
# find differences between source and target
DOLLY.find_deltas()
# display SQL updates to bring source and target tables in-line
DOLLY.show_schema_updates()
# update the target database (and back it up)
DOLLY.update_target()
# check whether or not the user is happy.. will backup if not
if DOLLY.user_happy():
    # print restore SQL so the user can restore from SQL manually later if necessary
    DOLLY.print_restore_sql()
# all done, cleanup and exit
DOLLY.exit()
