import json
import sqlite3
import os
from datetime import date
import pandas as pd
import hashlib

class MappingConfig:
    def __init__(self, json_path: str):
        with open(json_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)

        self.table_name = self.config.get('table_name')
        self.mappings = self.config.get('mappings', {})

    def get_table_name(self):
        return self.table_name

    def get_content_column(self):
        return self.mappings.get('content')

    def get_value_column(self):
        return self.mappings.get('value')

    def get_date_column(self, row=None, date_for_all_timeseries=None):
        date_type = self.mappings.get('date')

        if row:
            year = int(row.get('Year'))
            month = int(row.get('Month', 1))
            day = int(row.get('Day', 1))
        elif date_for_all_timeseries:
            return date_for_all_timeseries
        else:
            today = date.today()
            year, month, day = today.year, today.month, today.day

        if date_type == 'A':
            return f"{year}-01-01"
        elif date_type == 'M':
            return f"{year}-{month:02d}-01"
        elif date_type == 'D':
            return f"{year}-{month:02d}-{day:02d}"

class DatabaseConfig(MappingConfig):
    def __init__(self, json_path: str):
        super().__init__(json_path)

        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(BASE_DIR, f"{self.get_table_name()}.db")
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def create_table(self):
        self.cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS "{self.get_table_name()}" (
                "{self.get_content_column()}" TEXT,
                "date" TEXT,
                "{self.get_value_column()}" INTEGER
            )
        ''')

    def insert_records(self, records: list):
        for record in records:
            self.cursor.execute(f'''
                INSERT INTO "{self.get_table_name()}" ("{self.get_content_column()}", "date", "{self.get_value_column()}")
                VALUES (?, ?, ?)
            ''', record)

    def close(self):
        self.conn.commit()
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class GetData(DatabaseConfig):
    def __init__(self, data_file, json_path):
        super().__init__(json_path)
        self.data_file = data_file

        if data_file.endswith('.html'):
            self.df = pd.read_html(data_file)[0]
        elif data_file.endswith('.csv'):
            self.df = pd.read_csv(data_file)
        elif data_file.endswith('.xlsx'):
            self.df = pd.read_excel(data_file)
        else:
            raise ValueError("Unsupported file format")

    def calculate_file_hash(self):
        hasher = hashlib.sha256()
        with open(self.data_file, 'rb') as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        return hasher.hexdigest()

    @staticmethod
    def load_last_hash():
        if os.path.exists('hash.txt'):
            with open('hash.txt', 'r') as f:
                return f.read().strip()
        return None

    @staticmethod
    def save_hash(hash_value):
        with open('hash.txt', 'w') as f:
            f.write(hash_value)

    def process_and_store(self):
        current_hash = self.calculate_file_hash()
        last_hash = self.load_last_hash()

        if current_hash == last_hash:
            print('No changes detected in data. Skipping processing.')
            return

        records = []

        for _, row in self.df.iterrows():
            content = row.get(self.get_content_column())

            try:
                value = float(str(row.get(self.get_value_column())).replace(',', '').strip())
            except:
                continue

            date_str = self.get_date_column()
            records.append((content, date_str, value))

        self.create_table()
        self.insert_records(records)
        self.close()
        self.save_hash(current_hash)
        print(f"Inserted {len(records)} records into {self.get_table_name()}")

