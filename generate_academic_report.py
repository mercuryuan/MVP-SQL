
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

from src.utils.dataloder import DataLoader
from src.utils.sql_parser import SQLParser

def generate_report():
    db_name = "academic"
    # Try spider first
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
        print(f"Error: Database '{db_name}' not found.")
        return

    print(f"Found {len(data)} SQL queries. Initializing Parser...")
    
    try:
        parser = SQLParser("spider", db_name) 
    except Exception as e:
        print(f"Failed to initialize parser: {e}")
        return

    output_file = "academic_detailed_report.txt"
    print(f"Generating detailed report to {output_file} ...")

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"Detailed Parsing Report for Database: {db_name}\n")
        f.write(f"Total Queries: {len(data)}\n")
        f.write("="*60 + "\n\n")
        
        for i, item in enumerate(data):
            sql = item['sql_query']
            question = item['question']
            
            header = f"No.{i+1}\nQuestion: {question}\nSQL: {sql}\n"
            
            # Print to console (progress)
            if (i+1) % 10 == 0:
                print(f"Processing {i+1}/{len(data)}...")
            
            f.write(header)
            
            try:
                report = parser.generate_report(sql)
                f.write("-" * 20 + " Analysis " + "-" * 20 + "\n")
                f.write(report + "\n")
                f.write("-" * 50 + "\n\n")
            except Exception as e:
                f.write("-" * 20 + " ERROR " + "-" * 20 + "\n")
                f.write(f"Parsing Failed: {e}\n")
                f.write("-" * 50 + "\n\n")

    print(f"\nDone! Report saved to {os.path.abspath(output_file)}")
    
    # Print first 3 examples to console as preview
    print("\n--- Preview (First 3 entries) ---")
    with open(output_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        # Find where the 4th entry starts to cut off
        count = 0
        for line in lines:
            if line.startswith("No."):
                count += 1
            if count > 3:
                break
            print(line, end="")

if __name__ == "__main__":
    generate_report()
