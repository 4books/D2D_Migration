import csv


def get_mig_list(file_path: str) -> list:
    try:
        with open(file_path, "r") as file:
            reader = csv.reader(file)
            return list(reader)
    except FileNotFoundError as e:
        print(f"File not found: {file_path}", e)
        return []
    except IOError as e:
        print(f"Error reading file: {file_path}", e)
        return []


def create_migration_result(file_path: str):
    with open(file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["OWNER", "TABLE", "STATUS", "COUNT", "MESSAGE"])


def add_migration_result(file_path: str):
    pass
