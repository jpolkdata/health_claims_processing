import pandas as pd
import requests
import xml.etree.ElementTree as ET
import sqlite3
from sqlalchemy import create_engine
from datetime import datetime
from abc import ABC, abstractmethod

class ExtractService(ABC):
    @abstractmethod
    def extract(self):
        pass

class CSVExtractService(ExtractService):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        data = pd.read_csv(self.file_path)
        data['source'] = 'CSV'
        return data

class APIJSONExtractService(ExtractService):
    def __init__(self, api_url):
        self.api_url = api_url

    def extract(self):
        response = requests.get(self.api_url)
        data = pd.DataFrame(response.json())
        data['source'] = 'API'
        return data
    
class XMLExtractService(ExtractService):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        tree = ET.parse(self.file_path)
        root = tree.getroot()
        data = []
        for child in root:
            record = child.attrib
            record['source'] = 'XML'
            data.append(record)
        return pd.DataFrame(data)

class SQLExtractService(ExtractService):
    def __init__(self, db_path, query):
        self.db_path = db_path
        self.query = query

    def extract(self):
        conn = sqlite3.connect(self.db_path)
        data = pd.read_sql_query(self.query, conn)
        data['source'] = 'SQL'
        conn.close()
        return data

class TransformService(ABC):
    @abstractmethod
    def transform(self, data):
        pass

class BasicTransformService(TransformService):
    def transform(self, data):
        #Ex remove duplicates, fill missing values, add new column
        data = data.drop_duplicates()
        data = data.fillna(method='ffill')
        data['total'] = data['quantity'].astype(float) * data['price']
        return data

class LoadService(ABC):
    @abstractmethod
    def load(self, data):
        pass

class SQLiteLoadService(LoadService):
    def __init__(self, db_path, table_name):
        self.db_path = db_path
        self.table_name = table_name

    def load(self, data):
        engine = create_engine(f'sqlite:///{self.db_path}')
        data['timestamp'] = datetime.now()
        data.to_sql(self.table_name, con=engine, if_exists='replace', index=False)

class ETLPipeline:
    def __init__(self, extractor: ExtractService, transformer: TransformService, loader: LoadService):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        extracted_data = self.extractor.extract()
        transformed_data = self.transformer.transform(extracted_data)
        self.loader.load(transformed_data)
