import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'AutomationProgram')))
from read_mapping import MappingConfig, DatabaseConfig, GetData

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(BASE_DIR, 'table.html')
mapping_path = os.path.join(BASE_DIR, 'mapping.json')


class Custom(GetData):
    def __init__(self, data_file, mapping_file):
        super().__init__(data_file, mapping_file)

    def get_content_column(self):
        return 'World Population'

    def get_date_column(self, row=None, date_for_all_timeseries=None):
        if row is not None:
            year = int(row['Year (July 1)'])
            return f"{year}-01-01"
        return super().get_date_column(row, date_for_all_timeseries)

    def process_and_store(self):
        current_hash = self.calculate_file_hash()
        last_hash = self.load_last_hash()

        if current_hash == last_hash:
            print('No changes detected in data. Skipping processing.')
            return

        records = []

        for _, row in self.df.iterrows():
            content = 'World Population'

            try:
                value = float(str(row.get(self.get_value_column())).replace(',', '').strip())
            except Exception as e:
                print("Value parsing error:", e)
                continue

            date_str = self.get_date_column(row=row)
            records.append((content, date_str, value))

        self.create_table()
        self.insert_records(records)
        self.close()
        self.save_hash(current_hash)
        print(f"Inserted {len(records)} records into {self.get_table_name()}")


processor = Custom(data_path, mapping_path)
processor.process_and_store()
