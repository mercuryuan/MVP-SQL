import argparse
import os
import sys

# Add the src directory to sys.path to allow imports from graph package
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.graph.pipeline import SchemaPipeline

def main():
    parser = argparse.ArgumentParser(description="Build Schema Graph from SQLite Database")
    parser.add_argument("--db", required=True, help="Path to SQLite DB file")
    parser.add_argument("--out", required=True, help="Output path for the graph pickle file (.pkl)")
    
    args = parser.parse_args()

    # Verify input file exists
    if not os.path.exists(args.db):
        print(f"Error: Database file not found at {args.db}")
        sys.exit(1)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    try:
        pipeline = SchemaPipeline(args.db, args.out)
        pipeline.run()
    except Exception as e:
        print(f"An error occurred during execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    """
    # 在项目根目录下运行
python src/graph/run.py --db "path/to/your.sqlite" --out "path/to/output.pkl"
    """
    main()
