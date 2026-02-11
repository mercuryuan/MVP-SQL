
import sys
import os
import time

# Add src to path
sys.path.append(os.getcwd())

from src.utils.dataloder import DataLoader
from src.utils.sql_parser import SQLParser

def check_academic():
    db_name = "academic"
    dataset_name = "spider"
    
    print(f"Loading data for DB: {db_name} from {dataset_name}...")
    
    loader = DataLoader(dataset_name)
    data = loader.filter_data(db_id=db_name, fields=["question", "sql_query"])
    
    if not data:
        print("Data not found in 'spider', trying 'spider_dev'...")
        dataset_name = "spider_dev"
        loader = DataLoader(dataset_name)
        data = loader.filter_data(db_id=db_name, fields=["question", "sql_query"])
        
    if not data:
        print(f"Error: Database '{db_name}' not found in spider or spider_dev.")
        return

    print(f"Found {len(data)} SQL queries. Initializing Parser...")
    
    try:
        parser = SQLParser("spider", db_name) # Always use "spider" for schema path if mapped correctly
    except Exception as e:
        print(f"Failed to initialize parser: {e}")
        return

    success_count = 0
    fail_count = 0
    
    results = []

    print("\nStarting Validation...\n")
    
    for i, item in enumerate(data):
        sql = item['sql_query']
        question = item['question']
        
        try:
            # Test 1: Parse and Qualify
            expression = parser.parse_sql(sql)
            
            # Test 2: Extract Entities
            entities = parser.extract_entities(sql)
            
            # Test 3: Generate Report (Extract Relationships)
            # parser.generate_report(sql) 
            
            success_count += 1
            print(f"[{i+1}/{len(data)}] PASS: {sql[:60]}...")
            
        except Exception as e:
            fail_count += 1
            error_msg = str(e)
            print(f"[{i+1}/{len(data)}] FAIL: {sql}")
            print(f"    Error: {error_msg}")
            results.append({
                "index": i,
                "question": question,
                "sql": sql,
                "error": error_msg
            })

    print(f"\n{'='*30}")
    print(f"Summary for '{db_name}'")
    print(f"Total: {len(data)}")
    print(f"Success: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"{'='*30}")
    
    if fail_count > 0:
        print("\nFailure Details:")
        for res in results:
            print(f"\nID: {res['index']}")
            print(f"SQL: {res['sql']}")
            print(f"Error: {res['error']}")

if __name__ == "__main__":
    check_academic()
