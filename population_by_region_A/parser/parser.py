from read_mapping import GetData
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(BASE_DIR, 'table.html')
mapping_path = os.path.join(BASE_DIR, 'mapping.json')



class Custom(GetData):
    def __init__(self, data_file, mapping_file):
        super().__init__(data_file, mapping_file)

        self.df.rename(
            columns={
                "Population (2025)": "Population"
            },
            inplace=True
        )


processor = Custom(data_path, mapping_path)
processor.process_and_store()
