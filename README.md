# mysql-clone-row
Python utility to clone a row in mysql, from a target to source database, field by field

## Installation
* `sudo pip install python-mysqldb ConfigParser`
* `git clone git@github.com:lathonez/mysql-clone-row.git`

## Configuration
* The config file clone-row.cfg needs to have 0600 permissions as it may contain mysql passwords
* Use 127.0.0.1 instead of localhost. If you speciy localhost, mysql will use unix sockets and ignore the port argument
* If you don't need to use a password to access your database, leave the value as empty, e.g. `password:`

## Creating a tunnel to mysql
Sometimes you may not have direct access to the mysql database (e.g. the port is not exposed). To get around this you can use an ssh tunnel, if you have ssh access to the box:

    ``` shell
        ssh -L 33306:localhost:3306 my.mysql.server
    ```

You can now access the server on my.mysql.server on localhost port 33306.
