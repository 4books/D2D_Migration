import time
from datetime import datetime
import sys

import pyodbc

from decorators import measure_time
from odbc_util import get_db_connect
from odbc_util import get_lob_type
from odbc_util import truncate_table
from odbc_util import get_columns_info
from odbc_util import get_pk_columns_info
from csv_processor import get_mig_list
from constants import *


def _create_migration_result_csv(file_path, header=None):
    # TODO
    # 결과를 입력 받을 csv파일 생성
    pass


def _log_migration_result(
        source_connection: pyodbc.Connection,
        source_cursor: pyodbc.Cursor,
        owner: str,
        table_name: str,
        result: str,
        total_count=0,
        exe_time=0,
        message=""
):
    # TODO
    # csv로 입력하는 방식으로 변경?
    pass


def get_row_count(owner: str, table_name: str) -> int:
    source_connection = None
    source_cursor = None
    try:
        source_connection, source_cursor = get_db_connect(owner, SOURCE)
        select_query = f"SELECT COUNT(0) FROM {owner}.{table_name}"
        source_cursor.execute(select_query)
        row_count = source_cursor.fetchone()[0]

        return int(row_count)
    except Exception as e:
        print("get_row_count error!", e)
        raise e
    finally:
        if source_cursor:
            source_cursor.close()
        if source_connection:
            source_connection.close()


@measure_time
def insert_noraml_data(owner: str, table_name: str, rows: list, target_connection: pyodbc.Connection,
                       target_cursor: pyodbc.Cursor):
    try:
        columns = get_columns_info(owner, table_name, target_cursor)

        insert_query = f"""
            INSERT INTO {owner}.{table_name} NOLOGGING ({", ".join(columns)}) 
            VALUES({", ".join([f":{i + 1}" for i in range(len(columns))])})
        """

        target_cursor.executemany(insert_query, rows)
        target_connection.commit()

    except Exception as e:
        print("insert_noraml_data error!", e)
        raise e


@measure_time
def _migrate_normal_size(owner: str, table_name: str, target_connection: pyodbc.Connection,
                         target_cursor: pyodbc.Cursor):
    rows = select_normal_data(owner, table_name)

    if len(rows) == 0:
        print(f"{owner}.{table_name} No rows")
        # TODO 결과 insert
        return

    insert_noraml_data(owner, table_name, rows, target_connection, target_cursor)


@measure_time
def select_normal_data(owner, table_name) -> list[pyodbc.Row]:
    source_connection = None
    source_cursor = None
    try:
        select_query = f"SELECT * FROM {owner}.{table_name}"
        source_connection, source_cursor = get_db_connect(owner, SOURCE)
        source_cursor.execute(select_query)
        rows = source_cursor.fetchall()
    except Exception as e:
        print("select_normal_data error!", e)
        raise e
    finally:
        if source_cursor:
            source_cursor.close()
        if source_connection:
            source_connection.close()

    return rows


@measure_time
def _migrate_large_size():
    pass


def do_migration():
    if sys.platform.startswith("win"):
        mig_list = get_mig_list("input/migration_list.csv")
    else:
        mig_list = get_mig_list("input/migration_list.csv")

    for mig in mig_list:
        owner = mig[0]
        table_name = mig[1]
        is_truncate = mig[2]

        print(owner, table_name, "migration start")
        start_time = time.time()

        target_connection = None
        target_cursor = None
        try:
            target_connection, target_cursor = get_db_connect(owner, TARGET)

            if is_truncate == 'Y':
                truncate_table(owner, table_name, target_connection, target_cursor)

            lob_type = get_lob_type(owner, table_name, target_cursor)
            row_count = get_row_count(owner, table_name)

            if lob_type == NONE and row_count < 1_000_000:
                _migrate_normal_size(owner, table_name, target_connection, target_cursor)
            elif lob_type == NONE and row_count >= 1_000_000:
                _migrate_large_size()
            elif lob_type == CLOB:
                _migrate_large_size()
            elif lob_type == BLOB:
                _migrate_large_size()

        except Exception as e:
            print(owner, table_name, "error...", e)
            continue
        finally:
            if target_cursor:
                target_cursor.close()
            if target_connection:
                target_connection.close()

        elapsed = time.time() - start_time
        print(owner, table_name, "migration end")
    # End of for


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
