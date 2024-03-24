import pyodbc

from constants import *


def get_db_connect(owner, db_type) -> tuple[pyodbc.Connection, pyodbc.Cursor]:
    try:
        # TODO 테스트 개발서버 and QA 서버
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
        print("get_db_connect error!", e)
        print("==============================")
        raise e


def truncate_table(owner: str, table_name: str, connection: pyodbc.Connection, cursor: pyodbc.Cursor) -> None:
    try:
        cursor.execute(f"TRUNCATE TABLE {owner}.{table_name} REUSE STORAGE")
        connection.commit()

        print(owner, table_name, "table truncated")
    except Exception as e:
        print("truncate_table error!", e)
        raise e


def get_pk_columns_info(owner: str, table_name: str, cursor: pyodbc.Cursor) -> list[str]:
    pk_columns = []
    select_query = f"""
        SELECT COL.COLUMN_NAME COLUMN_NAME
        FROM ALL_CONSTRAINTS CONS
        INNER JOIN ALL_CONS_COLUMNS COL
            ON CONS.OWNER = COL.OWNER
            AND CONS.TABLE_NAME = COL.TABLE_NAME
            AND CONS.CONSTRAINT_NAME = COL.CONSTRAINT_NAME
        WHERE CONS.OWNER = '{owner}'
        AND CONS.TABLE_NAME = '{table_name}'
        AND CONS.CONSTRAINT_TYPE = 'P'
        ORDER BY COL."POSITION"
    """

    cursor.execute(select_query)
    for column in cursor.fetchall():
        pk_columns.append(column[0])

    return pk_columns


def get_columns_info(owner: str, table_name: str, cursor: pyodbc.Cursor) -> tuple[list[str], list[str]]:
    # 컬럼 정보만 가져오기 위해서 1 = 0
    select_query = f"""
        SELECT *
        FROM {owner}.{table_name}
        WHERE 1 = 0
    """

    cursor.execute(select_query)

    column_names = [column[0] for column in cursor.description]
    columns_type = [column[1] for column in cursor.description]

    return column_names, columns_type


def get_lob_type(owner: str, table_name: str, cursor: pyodbc.Cursor) -> str:
    select_query = f"""
        SELECT DATA_TYPE 
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = '{owner}' 
        and TABLE_NAME = '{table_name}' 
    """
    try:
        cursor.execute(select_query)

        lob_type = NONE
        for row in cursor.fetchall():
            data_type = row[0]
            if data_type == BLOB:
                return BLOB  # BLOB은 바로 리턴해주어야 함
            elif data_type == CLOB:
                lob_type = CLOB
        return lob_type
    except Exception as e:
        print("get_lob_type error!", e)
        raise e
