# mysql-clone-row
Clone a row from one mysql database to another

## Features
* Ignore schema differences, common columns will be cloned regardless of definitions that are missing from target or source databases
* Fail-safe operation, with automated and manual rollback procedures provided
* Checkpointing, so you can check the target system before 'committing' the changes
* Hint at schema updates required, providing SQL to bring source table in line with target, or vice versa
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
* The main sections of the config file are host aliases. These allow you to configre multiple databases hosts and refer to them easily from the command line. An example is as follows:
```
[host.example_one]
username: example_one_user
password: example_one_pass
hostname: one.example.com
port: 3306
database: example_one_db
```
* The config file needs to have 0600 permissions as it is likely to contain mysql passwords. If you do not set the correct permissions the script will not run.
* Use 127.0.0.1 instead of localhost. If you speciy localhost, mysql will use unix sockets and ignore the port argument you have configured
* If you don't need to use a password to access your database, leave the value as empty, e.g. `password:` (see example linked above)

## Usage

```
usage: CloneRow.py [-h] [--schema_only] [--unload_dir UNLOAD_DIR]
                   {host_one,host_two} {host_one,host_two} table [column]
                   [filter]

positional arguments:
  {host_one,host_two}   source host alias (for host.* config section)
  {host_one,host_two}   target host alias (for host.* section)
  table                 table to consider: select from <table>
  column                column to consider: select from table where <column>
                        (default: None)
  filter                value to filter column: select from table where column
                        = <filter> (default: None)

optional arguments:
  -h, --help            show this help message and exit
  --schema_only, -s     diff schema only, do not consider data (column and
                        filter not required) (default: False)
  --unload_dir UNLOAD_DIR, -u UNLOAD_DIR
                        directory to unload backups and update sql dumps to
                        (default: /tmp)
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
```
If the script is run as follows, `example_one` is the source and `example_two` is the target:

`CloneRow.py example_one example_two my_table my_column my_filter`

The equivalent in 'sql':
```sql
select
    *
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

## Installation
```
git clone https://github.com/lathonez/mysql-clone-row.git
sudo pip install -r mysql-clone-row/requirements.txt
# add the following to .bashrc
export PATH=$PATH:/path/to/mysql-clone-row
```

### Creating a tunnel to mysql
Sometimes you may not have direct access to the mysql database (e.g. the port is not exposed). To get around this you can use an ssh tunnel, if you have ssh access to the box:

`ssh -L 33306:localhost:3306 my.mysql.server`

You can now access the server on my.mysql.server on localhost port 33306.

### Installation Errors
Common issues and remedies during installation
####mysql-python install failing

```
sudo pip install -r mysql-clone-row/requirements.txt
...
Traceback (most recent call last):

  File "<string>", line 17, in <module>

  File "/tmp/pip_build_shazleto/MySQL-python/setup.py", line 17, in <module>

    metadata, options = get_config()

  File "setup_posix.py", line 43, in get_config

    libs = mysql_config("libs_r")

  File "setup_posix.py", line 25, in mysql_config

    raise EnvironmentError("%s not found" % (mysql_config.path,))

EnvironmentError: mysql_config not found

----------------------------------------
Cleaning up...
Command python setup.py egg_info failed with error code 1 in /tmp/pip_build_shazleto/MySQL-python
```

If you see the above error, try installing libmysqlclient:

`sudo apt-get install libmysqlclient-dev`

####bad interpreter

```
$ ./CloneRow.py
-bash: ./CloneRow.py: /usr/local/bin/python: bad interpreter: No such file or directory
```

If you see the above error, your python interpreter is probably not located in the usual location (/usr/local/bin/python). You can either symlink in the correct location:

```
$ which python
/usr/bin/python
$ cd /usr/local/bin/
$ sudo ln -s /usr/bin/python
```

or just run the script as `python CloneRow.py`

##Credits
* [mysql-python](http://mysql-python.sourceforge.net/MySQLdb.html)
* [DictDiffer](https://github.com/hughdbrown/dictdiffer)
