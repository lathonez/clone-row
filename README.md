# mysql-clone-row
Python utility to clone a row in mysql, from a target to source database, field by field



## Configuration
* The config file CloneRow.cfg needs to have 0600 permissions as it may contain mysql passwords
* `source_host` and `target_host` (command line arguments), must be sections defined in config (as per the example CloneRow.cfg)
* Use 127.0.0.1 instead of localhost. If you speciy localhost, mysql will use unix sockets and ignore the port argument
* If you don't need to use a password to access your database, leave the value as empty, e.g. `password:`

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

## Installation
```
https://github.com/lathonez/mysql-clone-row.git
sudo pip install -r mysql-clone-row/requirements.txt
# add the following to .bashrc
export PATH=$PATH:/path/to/mysql-clone-row
```

### Installation Errors
Common issues and remedies during installation
####mysql-python install failing

```shell
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

```shell
dev:~/mysql-clone-row$ ./CloneRow.py
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


## Creating a tunnel to mysql
Sometimes you may not have direct access to the mysql database (e.g. the port is not exposed). To get around this you can use an ssh tunnel, if you have ssh access to the box:

```shell
    ssh -L 33306:localhost:3306 my.mysql.server
```

You can now access the server on my.mysql.server on localhost port 33306.