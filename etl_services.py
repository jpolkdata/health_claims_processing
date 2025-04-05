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
        conn = sqllite3.connect(self.db_path)
        data = pd.read_sql_query(self.query, conn)
        data['source'] = 'SQL'
        conn.close()
        return data
    