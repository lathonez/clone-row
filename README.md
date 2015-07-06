# mysql-clone-row
Python utility to clone a row in mysql, from a target to source database, field by field

## Installation
* `sudo pip install python-mysqldb ConfigParser`
* `git clone git@github.com:lathonez/mysql-clone-row.git`

## Configuration
* The config file CloneRow.cfg needs to have 0600 permissions as it may contain mysql passwords
* `source_host` and `target_host` (command line arguments), must be sections defined in config (as per the example CloneRow.cfg)
* Use 127.0.0.1 instead of localhost. If you speciy localhost, mysql will use unix sockets and ignore the port argument
* If you don't need to use a password to access your database, leave the value as empty, e.g. `password:`

## Usage

```shell
    usage: CloneRow.py [-h]
                       source_host target_host database table column column_filter

    positional arguments:
      source_host    source hostname: should be defined in config
      target_host    target hostname: should be defined in config
      database       database name: same on source and target host
      table          table to consider: select from <table>
      column         column to consider: select from table where <column>
      column_filter  value to filter column: select from table where column =
                     <column_filter>

    optional arguments:
      -h, --help     show this help message and exit
```

## Creating a tunnel to mysql
Sometimes you may not have direct access to the mysql database (e.g. the port is not exposed). To get around this you can use an ssh tunnel, if you have ssh access to the box:

```shell
    ssh -L 33306:localhost:3306 my.mysql.server
```

You can now access the server on my.mysql.server on localhost port 33306.