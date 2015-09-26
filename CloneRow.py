#! /usr/local/bin/python
""" Python module for cloning a MYSQL row from one host to another """

import argparse, coloredlogs, ConfigParser, datetime
import logging, MySQLdb, os, stat, sys, time, traceback
from subprocess import Popen, PIPE
from DictDiffer import DictDiffer

class CloneRow(object):
    """ CloneRow constructor """

    def __init__(self):
        # make sure the config file has correct permissions (0600)
        self._check_config_chmod()
        coloredlogs.install(show_hostname=False, show_name=False, show_severity=False)
        logging.info('Reading configuration..')
        self.config = ConfigParser.ConfigParser(allow_no_value=True)
        try:
            self.config.readfp(open(os.path.dirname(os.path.realpath(__file__)) + '/CloneRow.cfg'))
        except IOError:
            logging.error('You have not setup a CloneRow.cfg file for your requirements')
            logging.info('take a look at CloneRow.example.cfg')
            logging.info('https://github.com/lathonez/mysql-clone-row#configuration')
            sys.exit(1)
        self.source = {
            'alias': None,
            'connection': None,
            'db_name': None,
            'row': None
        }
        self.target = {
            'alias': None,
            'backup': None,
            'connection': None,
            'db_name': None,
            'new_insert': False,
            'row': None
        }
        self.database = {
            'table': None,
            'column': None,
            'filter': None,
            'ignore_columns': [],
            'deltas': None
        }

    #
    # PRIVATE methods
    #

    @classmethod
    def _check_config_chmod(cls):
        """ make sure the file permissions of CloneRow.cfg are 0600 """
        cfg_path = os.path.dirname(os.path.realpath(__file__)) + '/CloneRow.cfg'
        chmod = oct(stat.S_IMODE(os.stat(cfg_path).st_mode))
        if chmod != '0600':
            logging.error('CloneRow.cfg needs to be secure: `chmod 0600 CloneRow.cfg`')
            sys.exit(1)

    def _connect(self, host_alias):
        """
        connect to a mysql database, returning a MySQLdb.Connection object

        Keyword arguments:
        host_alias -- the configured alias of the host we're connecting to
                      e.g. local (defined as host.local in config)
        """
        logging.info('attempting to connect to %s..', host_alias)
        con_args = {}
        con_args['host'] = self.config.get('host.' + host_alias, 'hostname')
        con_args['user'] = self.config.get('host.' + host_alias, 'username')
        con_args['port'] = self.config.getint('host.' + host_alias, 'port')
        con_args['db'] = self.config.get('host.' + host_alias, 'database')
        password = self.config.get('host.' + host_alias, 'password')
        if password is not None:
            con_args['passwd'] = password
        try:
            con = MySQLdb.connect(**con_args)
        except MySQLdb.OperationalError, mysqlex:
            if password is not None:
                # don't want to be logging out passwords really, use * instead
                con_args['passwd'] = '*' * len(con_args['passwd'])
            logging.error('Failed to connect to database %s with credentials:', host_alias)
            for key, val in con_args.iteritems():
                logging.error('  %s: %s', key, val)
            self._error(exception=mysqlex)
        version = con.get_server_info()
        logging.info(
            'connected to %s@%s:%s - Database version : %s',
            con_args['user'], host_alias, con_args['db'], version
        )
        return con

    def _check_encoding(self):
        """
        the encoding should match for source and target tables
        if it doesn't, error out and warn the user
        """
        logging.info('checking encoding..')
        source_enc = self._get_encoding(self.source)
        target_enc = self._get_encoding(self.target)
        logging.info(
            'source encoding %s:%s',
            source_enc['character_set_name'],
            source_enc['collation_name']
        )
        logging.info(
            'target encoding %s:%s',
            target_enc['character_set_name'],
            target_enc['collation_name']
        )

        if set(source_enc.values()) - set(target_enc.values()):
            logging.error('FATAL - encoding mismatch')
            logging.info(self._get_log_break('|encoding changes|'))
            logging.info(
                'To change encoding on %s to %s:%s run the following sql on %s',
                self.source['alias'],
                source_enc['character_set_name'],
                source_enc['collation_name'],
                self.source['alias']
            )
            logging.warning(
                '  alter table %s convert to character set %s collate %s;',
                self.database['table'],
                target_enc['character_set_name'],
                target_enc['collation_name']
            )
            logging.info(
                'To change encoding on %s to %s:%s run the following sql on %s',
                self.target['alias'],
                source_enc['character_set_name'],
                source_enc['collation_name'],
                self.source['alias']
            )
            logging.warning(
                '  alter table %s convert to character set %s collate %s;',
                self.database['table'],
                target_enc['character_set_name'],
                target_enc['collation_name']
            )
            logging.info(self._get_log_break())
            self._error('Fix the encoding mismatch and re-run')

    def _dump_update_sql(self, cursor):
        """
        dump the last executed update statement to a file

        Keyword arguments:
        cursor -- MySQLdb.cursor object of the connection we're dumping
        """
        logging.info('dumping update sql to disk..')
        # naughty naughty, accessing a protected member.. show me a better way
        sql = cursor._last_executed
        sql_file = self.config.get('clone_row', 'unload_filepath') + '.sql'
        with open(sql_file, "w") as outfile:
            outfile.write(sql)
        logging.warning('update sql is available for inspection at %s on this machine', sql_file)

    def _error(self, message=None, exception=None):
        """
        wrapper for raising errors that housekeeps, prints traceback and evntually exits (1)

        Keyword arguments:
        message -- a string message to log at error level
        exception -- if an exception is caught it can be passed in here, traceback will be printed
        """
        self._housekeep()
        if message is not None:
            logging.error(message)
        if exception is not None:
            # if you re-raise the original exception (e.g. raise exception), you lose traceback
            logging.error('original traceback below:')
            traceback.print_exc()
        sys.exit(1)

    def _get_column_sql(self, con, column):
        """
        return sql to add or drop a given column from the table passed in on the command line

        Keyword arguments:
        con -- MySQLdb.connection object of the database we're using (source or target)
        column -- the column to return sql for
        """
        drop_sql = 'alter table `{0}` drop column `{1}`;'.format(self.database['table'], column)
        # using con.query instead of con.cursor.execute gives us a nicer return format
        con.query('show fields from {0} where field = \'{1}\''.format(
            self.database['table'],
            column
        ))
        res = con.store_result()
        if res.num_rows() != 1:
            self._error('get_column_sql: only one row expected!')
        # using how=1 and parsing into a dict lets us manipulate with the returned data easily
        column_info = dict(res.fetch_row(how=1)[0])
        not_null = '' if column_info['Null'] == 'YES' else ' not null'
        default = '' if column_info['Default'] is None else ' default ' + column_info['Default']
        add_sql = 'alter table `{0}` add column `{1}` {2}{3}{4};'.format(
            self.database['table'], column, column_info['Type'], default, not_null
        )
        return {
            'add_sql': add_sql,
            'drop_sql': drop_sql
        }

    def _get_encoding(self, host):
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
        sql = sql.format(
            self.config.get('host.' + host['alias'], 'database'),
            self.database['table']
        )
        host['connection'].query(sql)
        res = host['connection'].store_result()
        # we should only _ever_ be playing with one row, per host, at a time
        if res.num_rows() == 0:
            # this is an error case on the source database, will be dealt with later
            self._error('_check_encoding: failed to find encoding for ' + host['alias'])
        if res.num_rows() != 1:
            self._error('_check_encoding: Only one row expected')
        row = res.fetch_row(how=1)
        return dict(row[0])

    @classmethod
    def _get_log_break(cls, string=''):
        """
        return a string in the form of {end}{sep}{string}{sep}{end},
        seps are repeated so the returned string will always be the same length
        e.g. |------------{string}------------|

        Keyword arguments:
        string -- the string to insert into the log break (if any)
        """
        length = 80
        sep = '-'
        end = '|'
        # how many seps do wwe need each side of the string?
        n_seps = (length - len(string) - 2) / 2
        # generate the log header, this might be one sep shorter than we need
        # if the string length is not an even number
        log_header = end + (sep * n_seps) + string + (sep * n_seps)
        if len(log_header) < length -1:
            # uneven length
            log_header += sep
        # finish it off with the end character
        log_header += end
        return log_header

    def _get_row(self, host):
        """
        Run a select query (MYSQLdb.Connection.query) returning a dict including column headers.
        Should always return a single row.

        Keyword arguments:
        host -- host dict containing params of the host we're selecting from
        """
        logging.info('getting %s row..', host['alias'])
        con = host['connection']
        if self.config.getboolean('clone_row', 'schema_only'):
            # if we're only doing schema diffs we don't care about columns or filters
            # we can just select the first row from the table
            select_sql = 'select * from {0} limit 1'.format(self.database['table'])
        else:
            select_sql = 'select * from {0} where {1} = {2}'.format(
                self.database['table'],
                self.database['column'],
                self._quote_sql_param(con.escape_string(self.database['filter']))
            )
        try:
            # again, we're not using cursors here because we want a nicely formatted dict
            con.query(select_sql)
        except MySQLdb.ProgrammingError, mysqlex:
            logging.error('Failed to execute query on %s', host['alias'])
            logging.error('  ' + select_sql)
            self._error(exception=mysqlex)
        res = con.store_result()
        # we should only _ever_ be playing with one row, per host, at a time
        if res.num_rows() == 0:
            # this is an error case on the source database, will be dealt with later
            return None
        if res.num_rows() != 1:
            self._error('get_row: Only one row expected -- cannot clone on multiple rows!')
        row = res.fetch_row(how=1)
        return dict(row[0])

    def _get_table_config(self, table):
        """
        get table specific config items, if any, as defined in config (table.mytable)

        Keyword arguments:
        table -- the name of the table, e.g. 'mytable' in the example above
        """
        table_section = 'table.' + table
        if not self.config.has_section(table_section):
            logging.warning('no table specific config defined for %s', table)
            return
        try:
            # unfortunately ConfigParser doesn't support lists, this is as nice as anything
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
        """
        return the unload filepath for us to use for backups and sql dumps
        in the format of /dir/table-column-filter-millis
        we're after something unique for this run hence millis
        """
        unload_file = self.config.get('clone_row', 'unload_dir')
        unload_file += '/{0}-{1}-{2}-{3}'.format(
            self.database['table'],
            self.database['column'],
            self.database['filter'],
            int(round(time.time() * 1000))
        )
        return unload_file

    def _housekeep(self):
        """ close any existing connections """
        logging.info('housekeeping..')
        if self.source['connection'] is not None:
            self.source['connection'].close()
        if self.target['connection'] is not None:
            self.target['connection'].close()

    def _print_delta_columns(self, deltas):
        """
        helper function to log columns which will be updated by this script (if any)

        Keyword arguments:
        deltas -- list of columns containing differences
        """
        logging.info('')
        logging.info(self._get_log_break('|Data Changes|'))
        logging.info('  The following columns will be updated on ' + self.target['alias'])
        # don't print stuff that we've been told to ignore
        deltas = [d for d in deltas if d not in self.database['ignore_columns']]
        for column in deltas:
            logging.info('    -%s ', column)
        logging.info(self._get_log_break())
        logging.info('')

    @classmethod
    def _quote_sql_param(cls, sql_param):
        """
        encapsulate an sql paramter in quotes if necessary
        param should be escaped (Connection.escape_string) before it's passed in

        Keyword arguments:
        sql_param -- the sql paramter to operate on
        """
        # we want to quote strings and dates (and probably more..?)
        if isinstance(sql_param, str) or isinstance(sql_param, datetime.datetime):
            return '\'{0}\''.format(sql_param)
        else:
            # doesn't need quoting
            return sql_param

    def _restore_target(self):
        """ restore data unloaded from the target database """
        cur = self.target['connection'].cursor()
        delete_sql = 'delete from {0} where {1} = %s'.format(
            self.database['table'], self.database['column']
        )
        cur.execute(delete_sql, (self.database['filter'], ))
        if self.target['connection'].affected_rows() != 1:
            cur.close()
            self.target['connection'].rollback()
            self._error('restore_target: expected to delete only one row')
        if self.target['new_insert']:
            logging.warning('deleting (not restoring) as target row was inserted from scratch')
            # there was no row here when we started the script, so we just need to
            # delete what was inserted (done above)
            cur.close()
            self.target['connection'].commit()
            return
        for line in open(self.target['backup']):
            # only run the insert:
            #   - ignore locks (only one row)
            #   - ignore encoding (handled herein separately)
            if line.find('INSERT') == 0:
                cur.execute(line)
        if self.target['connection'].affected_rows() != 1:
            cur.close()
            self.target['connection'].rollback()
            self._error('restore_target: expected to load only one row')
        cur.close()
        self.target['connection'].commit()

    def _unload_target(self):
        """ unload the row we're working on from the target database for backup purposes """
        logging.info('backing up target row..')
        unload_file = self.config.get('clone_row', 'unload_filepath') + '.backup'
        error_log = '/tmp/mysqldump.error'
        target_alias = self.target['alias']
        password = self.config.get('host.' + target_alias, 'password')

        args = [
            'mysqldump',
            '--host',
            self.config.get('host.' + target_alias, 'hostname'),
            '--user',
            self.config.get('host.' + target_alias, 'username'),
            '--port',
            self.config.get('host.' + target_alias, 'port'),
            '--no-create-info',
            '--databases',
            self.target['db_name'],
            '--tables',
            self.database['table'],
            '--where',
            '{0} = \'{1}\''.format(self.database['column'], self.database['filter'])
        ]

        if password is not None:
            args.append('-p' + password)

        # do the mysql dump and pipe it through James Mishra's mysqldump_to_csv
        # so we can use it to insert later if restoring from backup
        with open(unload_file, 'wb', 0) as outfile, open(error_log, 'wb', 0) as errfile:
            mysqldump_process = Popen(args, stdout=outfile, stderr=errfile)
        ret = mysqldump_process.wait()

        if ret > 0:
            logging.error('an issue occurred running mysqldump, see ' + errfile)
            self._error('mysqldump exited with non zero error code of ' + str(ret))

        # do some basic sanity checking - we should only have one INSERT line
        num_lines = sum(1 for line in open(unload_file) if line.find('INSERT') == 0)

        if num_lines != 1:
            self._error('unload_target: unable to verify unload file ' + unload_file)
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
        """ use DictDiffer to find differences between target and source databases """
        logging.info('finding deltas..')
        delta = DictDiffer(self.source['row'], self.target['row'])
        self.database['deltas'] = {
            'new_columns_in_source': delta.added(),
            'new_columns_in_target': delta.removed(),
            'delta_columns': delta.changed(),
            'unchanged_columns': delta.unchanged()
        }

    def get_rows(self):
        """ get a single row from soure and target databases """
        self.source['row'] = self._get_row(self.source)
        self.target['row'] = self._get_row(self.target)
        # we really need a source row..
        if self.source['row'] is None:
            self._error('get_rows: no row found in {0} database'.format(self.source['alias']))

    def insert_target(self):
        """
        insert as little data as possible into the target database, if nothing
        exists there already. This allows us to reselect and continue as normal
        """
        if self.target['row'] is not None:
            # we only need to do this if there's no target row
            return
        logging.info('inserting a minimal row into target database..')
        cur = self.target['connection'].cursor()
        insert_sql = 'insert into {0} ({1}) values (%s)'.format(
            self.database['table'], self.database['column']
        )
        # what we're doing here is just putting a single row containing the column
        # and any columns which have default values into the db, we will update them
        # as normal later on in the script
        cur.execute(insert_sql, (self.database['filter'],))
        if self.target['connection'].affected_rows() != 1:
            cur.close()
            self._error('somehow we\'ve inserted multiple rows')
        # now we have a row, we can return it as usual
        self.target['row'] = self._get_row(self.target)
        self.target['new_insert'] = True

    def parse_cla(self):
        """ parse command line arguments and setup config based on them """
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        aliases = [section for section in self.config.sections() if 'host.' in section]
        parser.add_argument(
            '--schema_only', '-s',
            help='diff schema only, do not consider data (column and filter not required)',
            action='store_true',
            default=False
        )
        parser.add_argument(
            '--unload_dir', '-u',
            help='directory to unload backups and update sql dumps to',
            default='/tmp'
        )
        parser.add_argument(
            '--feeling_lucky', '-f',
            action='store_true',
            help='do not prompt the user to restore, backup SQL will still be logged',
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
        parser.add_argument('column', nargs='?', help='column to consider')
        parser.add_argument(
            'filter',
            nargs='?',
            help='value to filter column: where column = <filter>'
        )
        args = parser.parse_args()
        # we either need --schema_only or column AND filter passed in
        if not args.schema_only and (args.column is None or args.filter is None):
            print '\n', \
                'column & filter arguments must be supplied unless running with --schema_only/-s\n'
            parser.print_help()
            sys.exit(0)
        self.source['alias'] = args.source_alias
        self.target['alias'] = args.target_alias
        if self.source['alias'] == self.target['alias']:
            self._error('source and target alias are identical')
        self.source['db_name'] = self.config.get('host.' + args.source_alias, 'database')
        self.target['db_name'] = self.config.get('host.' + args.target_alias, 'database')
        self.database['table'] = args.table
        self.database['column'] = args.column
        self.database['filter'] = args.filter
        self._get_table_config(self.database['table'])
        self.config.add_section('clone_row')
        self.config.set('clone_row', 'unload_dir', args.unload_dir)
        self.config.set('clone_row', 'unload_filepath', self._get_unload_filepath())
        self.config.set('clone_row', 'schema_only', str(args.schema_only))
        self.config.set('clone_row', 'feeling_lucky', str(args.feeling_lucky))

    def print_restore_sql(self):
        """ provide sql steps to rollback by hand after script has run """
        target_alias = self.target['alias']
        restore_sql = [
            '  mysql -h {0} -P {1} -u {2} -p {3}'.format(
                self.config.get('host.' + target_alias, 'hostname'),
                self.config.get('host.' + target_alias, 'port'),
                self.config.get('host.' + target_alias, 'username'),
                self.target['db_name']
            )
        ]
        restore_sql.append('    begin;')
        restore_sql.append('    delete from {0} where {1} = {2};'.format(
            self.database['table'],
            self.database['column'],
            self._quote_sql_param(self.database['filter'])
        ))
        restore_sql.append('    -- if more than one row has been deleted above run `rollback;`')
        if not self.target['new_insert']:
            restore_sql.append('    source ' + self.target['backup'])
        restore_sql.append('    commit;')
        logging.info('')
        logging.info(self._get_log_break('|Manual Rollback Steps|'))
        logging.info('  To rollback manually, run the following steps on this machine')
        for line in restore_sql:
            logging.warning(line)
        logging.info(self._get_log_break())
        logging.info('')
        return

    def set_connections(self):
        """ setup soure and target MySQLdb.connection objects """
        self.source['connection'] = self._connect(self.source['alias'])
        self.target['connection'] = self._connect(self.target['alias'])
        # we don't want mysql commit stuff unless we've okay'd it
        self.target['connection'].autocommit(False)
        # make sure the encoding is all good
        self._check_encoding()

    def show_schema_updates(self):
        """ display SQL statements to adjust database for schema differences on this table """
        for mode in ['source', 'target']:
            deltas = self.database['deltas']['new_columns_in_' + mode]
            working_db = self.source['alias'] if mode == 'source' else self.target['alias']
            other_db = self.target['alias'] if mode == 'source' else self.source['alias']
            con = self.source['connection'] \
                if working_db == self.source['alias'] else self.target['connection']
            for column in deltas:
                logging.info('')
                logging.info(self._get_log_break('|Schema Change - Column: {0}|'.format(column)))
                logging.info(
                    '  Column \'%s\' exists in the %s database but not in %s',
                    column, working_db, other_db
                )
                info = self._get_column_sql(con, column)
                logging.info(
                    '  To Add Column \'%s\' to %s, run the following SQL on %s:',
                    column, other_db, other_db
                )
                logging.warning('    ' + info['add_sql'])
                logging.info(
                    '  To Drop Column \'%s\' from %s, run the following SQL on %s:',
                    column, working_db, working_db
                )
                logging.warning('    ' + info['drop_sql'])
                logging.info(self._get_log_break())
                logging.info('')
        if self.config.getboolean('clone_row', 'schema_only'):
            # we're done if only diffing schema
            self.exit()

    def update_target(self):
        """ apply differences in the source database to the target """
        delta_columns = self.database['deltas']['delta_columns']
        if not len(delta_columns):
            logging.warning('data is identical in target and source, nothing to do..')
            self.exit()
        if set(delta_columns). \
            issubset(set(self.database['ignore_columns'])):
            logging.warning('all changes are configured to be ignored, nothing to do..')
            self.exit()
        self._print_delta_columns(delta_columns)
        if not self.target['new_insert']:
            self.target['backup'] = self._unload_target()
        else:
            logging.info('not backing up target on new insert..')
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
        # dump the actual update sql out to disk so we can look at it later if necessary
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
        """ Give the user a chance to restore from backup automatically beforer we terminate """
        logging.info('Row has been cloned successfully..')
        if self.config.getboolean('clone_row', 'feeling_lucky'):
            logging.warning('Not prompting to restore from backup as you\'re felling lucky today')
            return True
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
# display SQL updates to bring source and target table definitions in-line
DOLLY.show_schema_updates()
# update the target database (and back it up)
DOLLY.update_target()
# check whether or not the user is happy.. will backup if not
if DOLLY.user_happy():
    # print restore SQL so the user can restore from SQL manually later if necessary
    DOLLY.print_restore_sql()
# all done, cleanup and exit
DOLLY.exit()
