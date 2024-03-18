import time
from datetime import datetime
import sys

import pyodbc

from decorators import measure_time
from odbc_util import get_db_connect
from odbc_util import SOURCE
from odbc_util import TARGET
from csv_processor import get_mig_list

NONE = "NONE"
BLOB = "BLOB"
CLOB = "CLOB"
ERROR = "ERROR"


def _create_migration_result_csv(file_path, header=None):
    # TODO
    # 결과를 입력 받을 csv파일 생성
    pass


def _log_migration_result(
        connection: pyodbc.Connection,
        cursor: pyodbc.Cursor,
        owner: str,
        tablename: str,
        result: str,
        total_count=0,
        exe_time=0,
        message=""
):
    # TODO
    # csv로 입력하는 방식으로 변경?
    pass


def get_row_count(owner: str, tablename: str) -> int:
    connection = None
    cursor = None
    try:
        connection, cursor = get_db_connect(owner, SOURCE)
        select_query = f"SELECT COUNT(0) FROM {owner}.{tablename}"
        cursor.execute(select_query)
        row_count = cursor.fetchone()[0]

        return int(row_count)
    except Exception as e:
        print("_get_row_count error!", e)
        raise e
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def _get_lob_type(owner: str, tablename: str, cursor: pyodbc.Cursor) -> str:
    select_query = f"""
        SELECT DATA_TYPE 
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = '{owner}' 
        and TABLE_NAME = '{tablename}' 
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
        print("_get_lob_type error!", e)
        return ERROR


def _get_columns_info(owner: str, tablename: str, cursor: pyodbc.Cursor) -> tuple[list, list]:
    pk_columns = []
    select_query = f"""
        SELECT COL.COLUMN_NAME COLUMN_NAME
        FROM ALL_CONSTRAINTS CONS
        INNER JOIN ALL_CONS_COLUMNS COL
            ON CONS.OWNER = COL.OWNER
            AND CONS.TABLE_NAME = COL.TABLE_NAME
            AND CONS.CONSTRAINT_NAME = COL.CONSTRAINT_NAME
        WHERE CONS.OWNER = '{owner}'
        AND CONS.TABLE_NAME = '{tablename}'
        AND CONS.CONSTRAINT_TYPE = 'P'
        ORDER BY COL."POSITION"
    """

    cursor.execute(select_query)
    for column in cursor.fetchall():
        pk_columns.append(column[0])

    columns = []
    select_query = f"""
        SELECT COLUMN_NAME COLUMN_NAME
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = '{owner}'
        AND TABLE_NAME = '{tablename}'
        ORDER BY COLUMN_ID
    """

    cursor.execute(select_query)
    for column in cursor.fetchall():
        columns.append(column[0])

    # pk 컬럼이 없는 경우를 위해
    if not pk_columns:
        pk_columns.append(columns[0])

    return pk_columns, columns


def _truncate_table(owner: str, tablename: str) -> None:
    connection = None
    cursor = None

    try:
        # Truncate를 하기 위해 각 스키마에 맞게 새로 접속
        connection_str = f"DSN=DataSource;UID={owner};PWD=password"
        connection = pyodbc.connect(connection_str)
        cursor = connection.cursor()

        cursor.execute(f"TRUNCATE TABLE {owner}.{tablename} REUSE STORAGE")
        connection.commit()

        print(owner, tablename, "table truncated")
    except Exception as e:
        print("_truncate_table error!", e)
        raise e
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


@measure_time
def _migrate_normal_size():
    pass


@measure_time
def _migrate_large_size():
    pass


def do_migration():
    mig_list = None
    if sys.platform.startswith("win"):
        mig_list = get_mig_list("input/migration_list.csv")
    else:
        mig_list = get_mig_list("input/migration_list.csv")

    for mig in mig_list:
        owner = mig[0]
        table_name = mig[1]

        target_connection, target_cursor = get_db_connect(owner, TARGET)

        lob_type = _get_lob_type(owner, table_name, target_cursor)
        row_count = get_row_count(owner, table_name)

        if lob_type == NONE and row_count < 1_000_000:
            _migrate_normal_size()
        elif lob_type == CLOB or row_count >= 1_000_000:
            _migrate_large_size()
        elif lob_type == BLOB:
            _migrate_large_size()
        else:  # Error
            # TODO 실패 입력?
            continue


if __name__ == '__main__':
    start_datetime = datetime.now()
    start = time.time()
    print("D2D Migration start")
    print("시작 시간:", start_datetime.strftime("%Y-%m-%d %H:%M:%S"))

    do_migration()

    print("D2D Migration end")
    print("=======================================")
    print("시작 시간:", start_datetime.strftime("%Y-%m-%d %H:%M:%S"))
    print("종료 시간:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print(f"총 소요 시간: {time.time() - start:.3f} sec")
    print("=======================================")
