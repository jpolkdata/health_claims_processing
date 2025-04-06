import os
import pandas as pd
import sqlite3
import pytest
from datetime import datetime

from etl_services import CSVExtractService, APIJSONExtractService, XMLExtractService, SQLExtractService
from etl_services import BasicTransformService, SQLiteLoadService, ETLPipeline

@pytest.fixture
def setup_sql_db():
    # source data store
    db_path = 'test_data/test_db.db'
    
    # Make sure any existing db is removed first
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
    except PermissionError:
        # If we can't remove it, use a different filename
        db_path = 'test_data/test_db_new.db'
    
    # Create fresh database
    sql_script = 'test_data/source_data.sql'
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        with open(sql_script, 'r') as f:
            conn.executescript(f.read())
        conn.close()
        
        yield db_path
        
    finally:
        # Make sure connection is closed
        if conn:
            try:
                conn.close()
            except:
                pass
        # Try to clean up
        try:
            if os.path.exists(db_path):
                os.remove(db_path)
        except:
            pass

def test_csv_extraction():
    extractor = CSVExtractService('test_data/source_data.csv')
    data = extractor.extract()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 5
    assert 'source' in data.columns
    assert all(data['source'] == 'CSV')

def test_api_json_extraction(requests_mock):
    api_url = 'https://api.example.com/data'
    requests_mock.get(api_url, json=[
        {"identifier": 1, "date": "2024-07-01", "quantity": 10, "price": 9.99},
        {"identifier": 2, "date": "2024-07-02", "quantity": 15, "price": 19.99},
        {"identifier": 3, "date": "2024-07-03", "quantity": 7, "price": 14.99},
        {"identifier": 4, "date": "2024-07-04", "quantity": None, "price": 29.99},
        {"identifier": 5, "date": "2024-07-05", "quantity": 20, "price": 9.99}
    ])
    extractor = APIJSONExtractService(api_url)
    data = extractor.extract()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 5
    assert 'source' in data.columns
    assert all(data['source'] == 'API')

def test_xml_extraction():
    extractor = XMLExtractService('test_data/source_data.xml')
    data = extractor.extract()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 5
    assert 'source' in data.columns
    assert all(data['source'] == 'XML')

def test_sql_extraction(setup_sql_db):
    db_path = setup_sql_db
    extractor = SQLExtractService(db_path, 'SELECT * FROM sales')
    data = extractor.extract()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 5
    assert 'source' in data.columns
    assert all(data['source'] == 'SQL')

def test_basic_transformation():
    data = pd.DataFrame({
        'identifier': [1, 2, 3, 4, 5],
        'date': ['2024-07-01', '2024-07-02', '2024-07-03', '2024-07-04', '2024-07-05'],
        'quantity': [10, 15, 7, None, 20],
        'price': [9.99, 19.99, 14.99, 29.99, 9.99],
        'source': ['CSV', 'CSV', 'CSV', 'CSV', 'CSV']
    })
    transformer = BasicTransformService()
    transformed_data = transformer.transform(data)
    assert 'total' in transformed_data.columns
    assert len(transformed_data) == 5

def test_etl_pipeline(setup_sql_db):
    db_path = setup_sql_db
    extractor = CSVExtractService('test_data/source_data.csv')
    transformer = BasicTransformService()
    loader = SQLiteLoadService(db_path, 'transformed_sales')

    pipeline = ETLPipeline(extractor, transformer, loader)
    pipeline.run()

    conn = sqlite3.connect(db_path)
    loaded_data = pd.read_sql_query('SELECT * FROM transformed_sales', conn)
    conn.close()

    assert len(loaded_data) == 5
    assert 'total' in loaded_data.columns
    assert 'timestamp' in loaded_data.columns
    assert loaded_data['timestamp'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')).notnull().all()

