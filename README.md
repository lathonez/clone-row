# mysql-clone-row
Clone a row from one mysql database to another

## Features
* Ignore schema differences, common columns will be cloned regardless of definitions that are missing from target or source databases
* Fail-safe operation, with automated and manual rollback procedures provided
* Checkpointing, so you can check the target system before 'committing' the changes
* Check that the encoding of source and target databases matches
* Hint at schema (and encoding) updates required, providing SQL to bring source table in line with target, or vice versa
* Copy "transaction logs" (backups and update statements) to a remote log server as part of deployment. Handy if you have multiple developers releasing data updates from thier own machines and you need to keep an audit
* Ignore columns you never want to update (typically serials)
* Setup database aliases for ease of use (e.g. local, dev, test, integration, prod)

## There are existing tools for this!
There are many industry standard tools that could (and should) be used instead of mysql-clone-row, if applicable. Examples include [mysqldump](https://dev.mysql.com/doc/refman/5.1/en/mysqldump.html), [replication](https://dev.mysql.com/doc/refman/5.0/en/replication.html) and simply [select into outfile](https://dev.mysql.com/doc/refman/5.1/en/select-into.html).
However, I have found that there are several use-cases for this application:
* Many databases containing rich data which is modified little and often
* Wide tables with multiple changes that are hard to keep track of during the release cycle
* No version control for data (or data insertion scripts), making changes hard to audit
* No replication solution or budget for installing one
* No CMS for making simple changes to data rows across multiple databases

## Configuration
* An example configration file [CloneRow.example.cfg](https://github.com/lathonez/mysql-clone-row/blob/master/CloneRow.example.cfg) is provided
* This needs to be copied to `CloneRow.cfg` in the same directory and configured for your system
* The main sections of the config file are host aliases. These allow you to configre multiple databases hosts and refer to them easily from the command line.
```
[host.example_one]
username: example_one_user
password: example_one_pass
hostname: one.example.com
port: 3306
database: example_one_db
```
* `CloneRow.cfg` needs to have 0600 permissions as it is likely to contain mysql passwords. If you do not set the correct permissions the script will not run.
* Use 127.0.0.1 instead of localhost. If you speciy localhost, mysql will use unix sockets and ignore the port argument you have configured
* If you don't need to use a password to access your database, leave the value as empty, e.g. `password:` (see example linked above)

## Usage

```
usage: CloneRow.py [-h] [--schema_only] [--unload_dir UNLOAD_DIR]
                   [--feeling_lucky]
                   {example_one,example_two,example_nopass,example_one_tunnelled}
                   {example_one,example_two,example_nopass,example_one_tunnelled}
                   table [column] [filter]

positional arguments:
  {example_one,example_two}  source host alias (for host.* config section)
  {example_one,example_two}  target host alias (for host.* section)
  table                      table to consider: select from <table>
  column                     column to consider (default: None)
  filter                     value to filter column: where column = <filter> (default: None)

optional arguments:
  -h, --help                 show this help message and exit
  --schema_only, -s          diff schema only, do not consider data (column and
                             filter not required) (default: False)
  --unload_dir UNLOAD_DIR, -u UNLOAD_DIR
                             directory to unload backups and update sql dumps to (default: /tmp)
  --feeling_lucky, -f        do not prompt the user to restore, backup SQL will still be logged (default: False)
```

## Usage example
Taking the following two host aliases (defined in your CloneRow.cfg):
```
[host.example_one]
username: example_one_user
password: example_one_pass
hostname: one.example.com
port: 3306
database: example_one_db

[host.example_two]
username: example_two_user
password: example_two_pass
hostname: two.example.com
port: 3306
database: example_two_db

[table.my_table]
ignore_columns: id,lastUpdated
```
If the script is run as follows, `example_one` is the source and `example_two` is the target:

`CloneRow.py example_one example_two my_table my_column my_filter`

The equivalent in 'sql':
```sql
select
    * -- (everything apart from id and lastUpdated, which are ignored by the ignore_columns config for my_table)
into
    example_two.my_table
from
    example_one.my_table
where
    example_one.my_table.my_column = my_filter
```
If you want to just show schema differences between the two databases on a single table, you can do:

`CloneRow.py --schema_only example_one example_two my_table`

This saves you having to find a column filter if you just want to work out the schema updates

## Exit Codes
- 0: successfully executed
- 1: CloneRow.py encountered an error during operation, there should be an error message and stack trace printed
- 2: Invalid arguments supplied (check the error message)
- 3: CloneRow.cfg is not configured correctly
- 4: CloneRow.cfg is not secure (chmod 0600)
- 5: No rows were updated (e.g. all target and source data was identical)
- 6: There were changes but CloneRow.cfg has been configured such that they were ignored (e.g. table.my_table ignore_columns)

## Installation

### Prerequisites

* Python 2.7: Unfortunately we're dependent on python 2.7 due to our dependency on [MySQL-python](https://pypi.python.org/pypi/MySQL-python/1.2.5)
* python-dev
* python-pip
* libmysqlclient-dev

#### ubuntu

`sudo apt-get install python-pip python-dev libmysqlclient-dev`

#### arch

`TODO`

#### mac

`TODO`


```
git clone https://github.com/lathonez/mysql-clone-row.git
sudo pip install -r mysql-clone-row/requirements.txt
# add the following to .bashrc
export PATH=$PATH:/path/to/mysql-clone-row
```

## Acknowledgements

This project relies heavily on these libs:

* [mysql-python](http://mysql-python.sourceforge.net/MySQLdb.html)
* [DictDiffer](https://github.com/hughdbrown/dictdiffer)
