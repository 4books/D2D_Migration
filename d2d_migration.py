import multiprocessing
import time
from datetime import datetime
import sys
from multiprocessing import Queue, Process, Manager

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
        table: str,
        result: str,
        total_count=0,
        exe_time=0,
        message=""
):
    # TODO
    # csv로 입력하는 방식으로 변경?
    pass


@measure_time
def migrate_table(owner: str, table: str, result_dict: dict, complete_queue: multiprocessing.Queue,
                  is_truncate: bool = False) -> None:
    source_pk_connection = None
    source_pk_cursor = None
    source_data_connection = None
    source_data_cursor = None
    target_connection = None
    target_cursor = None
    commit_size = 1000
    max_retry = 10  # TODO 재시도...?
    partition = 0
    total_count = 0

    try:
        source_pk_connection, source_pk_cursor = get_db_connect(owner, SOURCE)
        source_data_connection, source_data_cursor = get_db_connect(owner, SOURCE)
        target_connection, target_cursor = get_db_connect(owner, TARGET)

        column_names, columns_type = get_columns_info(owner, table, source_data_cursor)
        pk_columns = get_pk_columns_info(owner, table, source_data_cursor)

        # pk가 없는 테이블은 제일 첫번째 컬럼을 가져온다
        if not pk_columns:
            pk_columns.append(column_names[0])

        select_pk_query = f"SELECT {', '.join(pk_columns)} FROM {owner}.{table}"

        select_data_query = f"""
            SELECT {', '.join(column_names)} FROM {owner}.{table} 
            WHERE {" AND ".join(f"{column} = ?" for column in pk_columns)}
        """

        insert_query = f"""
            INSERT INTO {owner}.{table} NOLOGGING ({", ".join(column_names)})
            values ({", ".join("TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')" if column_type == pyodbc.SQL_TYPE_DATE else "?"
                               for column_type in columns_type)})
        """

        # 테이블 초기화
        if is_truncate:
            truncate_table(owner, table, target_connection, target_cursor)

        lob_type = get_lob_type(owner, table, target_cursor)
        batch_size = 1000
        if lob_type == NONE:
            batch_size = 1000
        elif lob_type == CLOB or lob_type == BLOB:
            batch_size = 100

        source_pk_cursor.execute(select_pk_query)

        key_list = []
        while True:
            start_time = time.time()
            for _ in range(batch_size):
                key_value = source_pk_cursor.fetchone()
                if key_value:
                    key_list.append(key_value)
                else:
                    break

            print("======================================")
            print(f" {partition + 1}번째 pk select {len(key_list)}건 완료 {time.time() - start_time:.3f}sec 소요")

            if not key_list:
                target_connection.commit()
                print(f"{owner}.{table} 남은 데이터 commit 완료")

                # TODO 성공 로그 입력
                print(f"성공 {owner}.{table} total_count: {total_count}")  # TODO 임시용
                result_dict[f"{owner}{table}"] = SUCC
                return

            start_time = time.time()
            insert_data = []
            while key_list:
                key_value = key_list.pop()

                source_data_cursor.execute(select_data_query, key_value)
                row = source_data_cursor.fetchone()

                insert_data.append(tuple(
                    bytes(value) if isinstance(value, bytearray)  # Blob 처리
                    else value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime)  # Date 처리
                    else value
                    for value in row
                ))
            # End of key_list while

            print("======================================")
            print(f" {partition + 1}번째 data select {len(insert_data)}건 완료 {time.time() - start_time:.3f}sec 소요")

            start_time = time.time()
            target_cursor.executemany(insert_query, insert_data)
            total_count += len(insert_data)

            print("======================================")
            print(f" {partition + 1}번째 data insert {len(insert_data)}건 완료 {time.time() - start_time:.3f}sec 소요")
            print("======================================")

            if total_count % commit_size == 0:
                target_connection.commit()
                print(f"{owner}.{table} {total_count}건 커밋 완료")
        # End of main while

    except Exception as e:
        print(f"실패 {owner}.{table} total_count: {total_count}")  # TODO 임시용
        print(e)
        result_dict[f"{owner}{table}"] = FAIL
        # TODO 실패 로그 입력 갯수와 메세지까지
    finally:
        complete_queue.put((owner, table))
        if source_pk_cursor:
            source_pk_cursor.close()
        if source_pk_connection:
            source_pk_connection.close()
        if source_data_cursor:
            source_data_cursor.close()
        if source_data_connection:
            source_data_connection.close()
        if target_cursor:
            target_cursor.close()
        if target_connection:
            target_connection.close()


def get_child_process_and_start_migrate(mig_table: tuple, result_dict: dict, complete_queue: multiprocessing.Queue) \
        -> tuple[str, str, multiprocessing.Process]:
    owner = mig_table[0]
    table = mig_table[1]
    is_truncate = True if mig_table[2] == 'Y' else False

    process = Process(target=migrate_table, args=(owner, table, result_dict, complete_queue, is_truncate))
    process.start()

    return owner, table, process


def do_migration(max_processes=10):
    if sys.platform.startswith("win"):
        mig_list = get_mig_list("input\\migration_list.csv")
    else:
        mig_list = get_mig_list("input/migration_list.csv")

    # 실행 중인 프로세스 리스트
    processes = []

    manager = multiprocessing.Manager()
    result_dict = manager.dict()
    complete_queue = Queue()

    while mig_list or processes:

        # 실행 중인 프로세스 수가 최대 프로세스 수보다 작고 남은 테이블이 있는 경우
        while len(processes) < max_processes and mig_list:
            # child 프로세스 생성
            owner, table, process = get_child_process_and_start_migrate(mig_list.pop(), result_dict, complete_queue)
            processes.append((owner, table, process))

        # 완료된 프로세스 확인 및 결과 처리
        while not complete_queue.empty():
            completed_owner, completed_table = complete_queue.get()

            for owner, table, process in processes:
                if owner == completed_owner and table == completed_table:
                    status = result_dict[f"{owner}{table}"]
                    processes.remove((owner, table, process))

                    if status == "SUCC":
                        process.close()
                    else:
                        process.terminate()
                    process.join()

                    # 새로운 프로세스 생성
                    if mig_list:
                        owner, table, process = get_child_process_and_start_migrate(mig_list.pop(), result_dict,
                                                                                    complete_queue)
                        processes.append((owner, table, process))

                    break
            # End of for
        # End of compolete_queue while
    # End of mig_list while

    manager.shutdown()


if __name__ == '__main__':
    start_datetime = datetime.now()
    start = time.time()
    print("D2D Migration start")
    print("시작 시간:", start_datetime.strftime("%Y-%m-%d %H:%M:%S"))

    do_migration(max_processes=10)

    print("D2D Migration end")
    print("=======================================")
    print("시작 시간:", start_datetime.strftime("%Y-%m-%d %H:%M:%S"))
    print("종료 시간:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print(f"총 소요 시간: {time.time() - start:.3f} sec")
    print("=======================================")
