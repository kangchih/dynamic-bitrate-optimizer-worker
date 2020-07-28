"""The UseDatabase context manager for working with MySQL/MariaDB.
Simple example usage:

    from DBcm import UseDatabase, SQLError

    config = { 'host': '127.0.0.1',
               'user': 'myUserid',
               'password': 'myPassword',
               'database': 'myDB' }

    with UseDatabase(config) as cursor:
        _SQL = "select * from log"
        cursor.execute(_SQL)
        data = cursor.fetchall()
"""

##############################################################################
# Context manager for connecting/disconnecting to a database.
##############################################################################

import MySQLdb as db

class ConnectionError(Exception):
    """Raised if the backend-database cannot be connected to."""
    pass


class CredentialsError(Exception):
    """Raised if the database is up, but there's a login issue."""
    pass


class SQLError(Exception):
    """Raised if the query caused problems."""
    pass


class UseDatabase:

    def __init__(self, config: dict):
        """Add the database configuration parameters to the object.

        This class expects a single dictionary argument which needs to assign
        the appropriate values to (at least) the following keys:

            host - the IP address of the host running MySQL/MariaDB.
            user - the MySQL/MariaDB username to use.
            password - the user's password.
            database - the name of the database to use.

        For more options, refer to the mysql-connector-python documentation.
        """
        self.configuration = config

    def __enter__(self) -> 'cursor':
        """Connect to database and create a DB cursor.

        Return the database cursor to the context manager.
        Raise ConnnectionError if the database can't be found.
        Raise CredentialsError if the wrong username/password used.
        """
        try:

            self.conn = db.connect(**self.configuration)
            self.cursor = self.conn.cursor()
        except db.InterfaceError as err:
            raise ConnectionError(err) from err
        except db.ProgrammingError as err:
            raise CredentialsError(err) from err
        return self.cursor

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Destroy the cursor as well as the connection (after committing).
        """
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
        if exc_type is db.ProgrammingError:
            raise SQLError(exc_value)
        elif exc_type:
            raise exc_type(exc_value)

