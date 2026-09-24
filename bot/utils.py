import csv
import os
from typing import List, Any


def ensure_dir(dir_path: str):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)


def append_csv_row(file_path: str, header: List[str], row: List[Any]):
    ensure_dir(os.path.dirname(file_path))
    file_exists = os.path.exists(file_path)

    with open(file_path, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)
