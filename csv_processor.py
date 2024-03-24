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
