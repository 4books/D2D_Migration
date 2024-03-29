import multiprocessing
import platform
import time
from datetime import datetime
from multiprocessing import Queue, Process
import argparse

from odbc_util import config
from odbc_util import get_db_connect
from odbc_util import has_lob_type
from odbc_util import truncate_table
from odbc_util import get_columns_info
from odbc_util import get_pk_columns_info
from csv_processor import get_mig_list
from csv_processor import create_migration_result
from csv_processor import add_migration_result
from constants import *


def do_migrate_table(
        schema: str,
        table: str,
        result_dict: dict,
        complete_queue: multiprocessing.Queue,
        truncate_flag: bool,
        commit_size: int,
        max_retry: int,
        result_file_path: str
) -> None:
    table_start_time = time.time()

    source_pk_connection = None
    source_pk_cursor = None
    source_data_connection = None
    source_data_cursor = None
    target_connection = None
    target_cursor = None
    partition = 0
    total_count = 0

    try:
        source_pk_connection, source_pk_cursor = get_db_connect(schema, SOURCE)
        source_data_connection, source_data_cursor = get_db_connect(schema, SOURCE)
        target_connection, target_cursor = get_db_connect(schema, TARGET)

        column_names, columns_type = get_columns_info(schema, table, source_data_cursor)
        pk_columns = get_pk_columns_info(schema, table, source_data_cursor)

        # pk가 없는 테이블은 제일 첫번째 컬럼을 가져온다
        if not pk_columns:
            pk_columns.append(column_names[0])

        select_pk_query = f"""
            SELECT {', '.join(pk_columns)} FROM {schema}.{table}
        """

        select_data_query = f"""
            SELECT {', '.join(column_names)} FROM {schema}.{table} 
            WHERE {" AND ".join(f"{column} = ?" for column in pk_columns)}
        """

        insert_query = get_insert_query(schema, table, column_names, columns_type)

        # 테이블 초기화
        if truncate_flag:
            truncate_table(schema, table, target_connection, target_cursor)

        batch_size = 1000
        
        has_lob = has_lob_type(schema, table, target_cursor)
        if has_lob:
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
            print(
                f"{partition + 1}번째 {schema}.{table} pk select {len(key_list)}건 완료 {time.time() - start_time:.3f} sec 소요")

            if not key_list:
                target_connection.commit()
                print(f"{schema}.{table} 남은 데이터 commit 완료")
                print(f"성공 {schema}.{table} total_count: {total_count}")

                result_dict[(schema, table)] = SUCC
                add_migration_result(result_file_path, schema, table, 'S', total_count, SUCC)
                return

            start_time = time.time()
            insert_data = []
            while key_list:
                key_value = key_list.pop()

                retry_count = 0
                row = None

                while retry_count < max_retry:
                    try:
                        source_data_cursor.execute(select_data_query, key_value)
                        row = source_data_cursor.fetchone()
                        break
                    except Exception as e:
                        retry_count += 1
                        if retry_count == max_retry:
                            raise e
                        else:
                            print(f"{partition + 1}번째 {schema}.{table} data select 실패. retry {retry_count}/{max_retry}")
                            time.sleep(10)

                insert_data.append(tuple(
                    bytes(value) if isinstance(value, bytearray)  # Blob 처리
                    else value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime)  # Date 처리
                    else value
                    for value in row
                ))
            # End of key_list while

            print("======================================")
            print(
                f"{partition + 1}번째 {schema}.{table} data select {len(insert_data)}건 완료 {time.time() - start_time:.3f} sec 소요")

            start_time = time.time()
            retry_count = 0
            while retry_count < max_retry:
                try:
                    target_cursor.executemany(insert_query, insert_data)
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count == max_retry:
                        raise e
                    else:
                        print(f"{partition + 1}번째 {schema}.{table} data insert 실패. retry {retry_count}/{max_retry}")
                        time.sleep(10)

            total_count += len(insert_data)

            print("======================================")
            print(
                f"{partition + 1}번째 {schema}.{table} data insert {len(insert_data)}건 완료 {time.time() - start_time:.3f} sec 소요")
            print("======================================")

            if total_count % commit_size == 0:
                target_connection.commit()
                print(f"{schema}.{table} {partition + 1}번째 commit. 총 {total_count}건 커밋 완료")

            partition += 1
        # End of main while

    except Exception as e:
        print(f"실패 {schema}.{table} total_count: {total_count}", e)

        result_dict[(schema, table)] = FAIL
        add_migration_result(result_file_path, schema, table, 'E', total_count, str(e)[:500])
    finally:
        complete_queue.put((schema, table))
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

        print(f"{schema}.{table} 총 소요시간 : {time.time() - table_start_time:.3f} sec")


def get_child_process_and_start_migrate(
        mig_table: tuple,
        result_dict: dict,
        complete_queue: multiprocessing.Queue,
        truncate_flag: bool = True,
        commit_size: int = 1000,
        max_retry: int = 5,
        result_file_path: str = None
) -> tuple[str, str, multiprocessing.Process]:
    schema = mig_table[0]
    table = mig_table[1]

    process = Process(target=do_migrate_table,
                      args=(schema, table, result_dict, complete_queue, truncate_flag, commit_size, max_retry, result_file_path))
    process.start()

    return schema, table, process

def get_insert_query(schema: str, table: str, column_names: list[str], columns_type: list[str]) -> str:
    dbms = config.get('target_db').get('dbms')
    if dbms == 'oracle':
        return f"""
            INSERT INTO {schema}.{table} NOLOGGING ({", ".join(column_names)})
            VALUES ({", ".join("TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')" if column_type == datetime else "?"
                               for column_type in columns_type)})
        """
    elif dbms == 'mysql':
        return f"""
            INSERT INTO {schema}.{table} ({", ".join(column_names)})
            VALUES ({", ".join("STR_TO_DATE(?, '%Y-%m-%d %H:%i:%s')" if column_type == datetime else "?"
                               for column_type in columns_type)})
        """
    elif dbms == 'postgresql':
        return f"""
            INSERT INTO {schema}.{table} ({", ".join(column_names)})
            VALUES ({", ".join("TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS')" if column_type == datetime else "?"
                               for column_type in columns_type)})
        """
    elif dbms == 'sql_server':
        return f"""
            INSERT INTO {schema}.{table} ({", ".join(column_names)})
            VALUES ({", ".join("CONVERT(DATETIME, ?, 120)" if column_type == datetime else "?"
                               for column_type in columns_type)})
        """
    
    return f"""
        INSERT INTO {schema}.{table} ({", ".join(column_names)}) 
        VALUES ({", ".join("?" for _ in range(len(column_names)))})
    """


def do_migration(max_processes: int = 10, truncate_flag: bool = True, commit_size: int = 1000, max_retry: int = 5,
                 file_path: str = "") -> None:
    mig_list = get_mig_list(file_path)

    result_file_path = f"output\\mig_result_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.csv" \
        if platform.system() == "Windows" else f"output/mig_result_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.csv"
    create_migration_result(result_file_path)

    # 실행 중인 프로세스 리스트
    processes = []

    manager = multiprocessing.Manager()
    result_dict = manager.dict()
    complete_queue = Queue()

    while mig_list or processes:

        # 실행 중인 프로세스 수가 최대 프로세스 수보다 작고 남은 테이블이 있는 경우
        while len(processes) < max_processes and mig_list:
            # child 프로세스 생성
            schema, table, process = get_child_process_and_start_migrate(mig_list.pop(), result_dict, complete_queue,
                                                                        truncate_flag, commit_size, max_retry, result_file_path)
            processes.append((schema, table, process))

        # 완료된 프로세스 확인 및 결과 처리
        while not complete_queue.empty():
            completed_schema, completed_table = complete_queue.get()

            for schema, table, process in processes:
                if schema == completed_schema and table == completed_table:
                    status = result_dict[(schema, table)]
                    processes.remove((schema, table, process))

                    if status == SUCC:
                        process.join()
                        process.close()
                    else:
                        process.terminate()
                        process.join()

                    # 새로운 프로세스 생성
                    if mig_list:
                        schema, table, process = get_child_process_and_start_migrate(mig_list.pop(), result_dict,
                                                                                    complete_queue, truncate_flag,
                                                                                    commit_size, max_retry, result_file_path)
                        processes.append((schema, table, process))

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

    default_file_path = "input\\migration_list.csv" if platform.system() == "Windows" else "input/migration_list.csv"

    parser = argparse.ArgumentParser(description='D2D Migration Tool')
    parser.add_argument('-n', '--max-processes', type=int, default=10, help='Maximum number of processes (default: 10)')
    parser.add_argument('-t', '--truncate', action='store_true', help='Truncate target tables before migration')
    parser.add_argument('-c', '--commit-size', type=int, default=1000, help='Commit size (default: 1000)')
    parser.add_argument('-r', '--max-retry', type=int, default=5, help='Set max retry count (default: 5)')
    parser.add_argument('-f', '--file-path', type=str, default=default_file_path,
                        help='Set file path(default: input/migration_list.csv)')
    args = parser.parse_args()

    do_migration(max_processes=args.max_processes, truncate_flag=args.truncate, commit_size=args.commit_size,
                 max_retry=args.max_retry, file_path=args.file_path)

    print("D2D Migration end")
    print("=======================================")
    print("시작 시간:", start_datetime.strftime("%Y-%m-%d %H:%M:%S"))
    print("종료 시간:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print(f"총 소요 시간: {time.time() - start:.3f} sec")
    print("=======================================")
