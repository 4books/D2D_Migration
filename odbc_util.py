import pyodbc

from toml_processor import load_config
from constants import *

config = load_config()

def get_db_connect(schema, db_type) -> tuple[pyodbc.Connection, pyodbc.Cursor]:

    try:
        connection_str = ""

        if db_type == SOURCE:
            dbms = config.get('source_db').get('dbms')
            if dbms == 'oracle':
                connection_str = get_oracle_connection_str(schema, db_type, config)
            else:
                connection_str = get_db_connection_str(schema, db_type, config)

        elif db_type == TARGET:
            dbms = config.get('target_db').get('dbms')
            if dbms == 'oracle':
                connection_str = get_oracle_connection_str(schema, db_type, config)
            else:
                connection_str = get_db_connection_str(schema, db_type, config)
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
    
def get_oracle_connection_str(schema: str, db_type: str, config: dict) -> str:
    dsn = config.get(f'{db_type}_connection_string').get('DSN')
    dbq = config.get(f'{db_type}_connection_string').get('DBQ')
    uid = config.get(schema, {}).get('uid')
    pwd = config.get(schema, {}).get('pwd')

    if not dsn:
        raise ValueError(f'{db_type}_connection_string dsn empty!')
    if not dbq:
        raise ValueError(f'{db_type}_connection_string dbq empty!')
    if not uid:
        raise ValueError(f'{schema} or {schema} uid empty!')
    if not pwd:
        raise ValueError(f'{schema} or {schema} pwd empty!')
    
    return f"DSN={dsn};DBQ={dbq};UID={uid};PWD={pwd};"

def get_db_connection_str(schema: str, db_type: str, config: dict) -> str:
    driver = config.get(f'{db_type}_connection_string').get('DRIVER')
    server = config.get(f'{db_type}_connection_string').get('SERVER')
    database = config.get(f'{db_type}_connection_string').get('DATABASE')
    uid = config.get(schema, {}).get('uid')
    pwd = config.get(schema, {}).get('pwd')

    if not driver:
        raise ValueError(f'{db_type}_connection_string driver empty!')
    if not server:
        raise ValueError(f'{db_type}_connection_string server empty!')
    if not database:
        raise ValueError(f'{db_type}_connection_string database empty!')
    if not uid:
        raise ValueError(f'{schema} or {schema} uid empty!')
    if not pwd:
        raise ValueError(f'{schema} or {schema} pwd empty!')

    return f"DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd};"



def truncate_table(schema: str, table_name: str, connection: pyodbc.Connection, cursor: pyodbc.Cursor) -> None:
    try:
        cursor.execute(f"TRUNCATE TABLE {schema}.{table_name}")
        connection.commit()

        print(schema, table_name, "table truncated")
    except Exception as e:
        print("truncate_table error!", e)
        raise e


def get_pk_columns_info(schema: str, table: str, cursor: pyodbc.Cursor) -> list[str]:

    pk_columns = []
    dbms = config.get('source_db').get('dbms')
    select_query = config.get('pk_select_query').get(dbms)

    select_query = select_query.format(schema=schema, table=table)

    cursor.execute(select_query)
    for column in cursor.fetchall():
        pk_columns.append(column[0])

    return pk_columns


def get_columns_info(schema: str, table_name: str, cursor: pyodbc.Cursor) -> tuple[list[str], list[str]]:
    # 컬럼 정보만 가져오기 위해서 1 = 0
    select_query = f"""
        SELECT *
        FROM {schema}.{table_name}
        WHERE 1 = 0
    """

    cursor.execute(select_query)

    column_names = [column[0] for column in cursor.description]
    columns_type = [column[1] for column in cursor.description]

    return column_names, columns_type


def get_lob_type(schema: str, table_name: str, cursor: pyodbc.Cursor) -> str:
    select_query = f"""
        SELECT DATA_TYPE 
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = '{schema}' 
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
