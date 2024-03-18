import csv
from typing import Generator


def get_mig_list(file_path: str) -> Generator[list, None, None]:
    try:
        with open(file_path, "r") as file:
            reader = csv.reader(file)
            for row in reader:
                yield row
    except FileNotFoundError as e:
        print(f"File not found: {file_path}", e)
        yield from []
    except IOError as e:
        print(f"Error reading file: {file_path}", e)
        yield from []
