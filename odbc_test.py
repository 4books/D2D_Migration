import pyodbc

if __name__ == '__main__':
    connection = None
    cursor = None
    try:
        connection_str = f"DSN=OracleODBC;DBQ=test.com:1521/dev;UID=scott;PWD=tiger"

        connection = pyodbc.connect(connection_str)
        cursor = connection.cursor()

        cursor.execute("select 1 from dual")

        print(cursor.fetchone()[0])

    except Exception as e:
        print(e)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
