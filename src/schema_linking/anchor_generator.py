import os
import sys
from pathlib import Path
import logging

# Add project root to sys.path to ensure imports work
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)


from src.utils.dataloder import DataLoader



spider_loader = DataLoader("spider_train")
# print(spider_loader.list_dbnames())
data = spider_loader.filter_data(
    db_id="book_2",
    fields=["question", "sql_query"],
    # show_count=True
)
print(data[0])
