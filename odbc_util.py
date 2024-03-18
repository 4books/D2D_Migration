import pyodbc

SOURCE = "source"
TARGET = "target"


def get_db_connect(owner, db_type):
    try:
        if db_type == SOURCE:
            connection_str = f"DSN=DataSource;DBQ=DB;UID=id;PWD=password"
        elif db_type == TARGET:
            connection_str = f"DSN=DataSource;DBQ=DB;UID=id;PWD=password"
        else:
            raise ValueError('get_db_connect db_type must be entered only with source or target')

        connection = pyodbc.connect(connection_str)
        cursor = connection.cursor()

        return connection, cursor

    except Exception as e:
        print("==============================")
        print("_get_db_connect error!", e)
        print("==============================")
        raise e
