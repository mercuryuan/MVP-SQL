import json
import pickle
import os
import networkx as nx
from tqdm import tqdm
import sys

# Ensure we can import from src if needed, though we rely mostly on standard libraries here
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

class GraphRepoConverter:
    def __init__(self, input_root, output_root):
        self.input_root = input_root
        self.output_root = output_root

    def convert_all(self):
        """
        Walk through the input directory and convert all valid graph datasets.
        """
        print(f"Scanning {self.input_root}...")
        
        tasks = []
        for root, dirs, files in os.walk(self.input_root):
            if 'nodes.json' in files and 'relationships.json' in files:
                tasks.append(root)
            elif 'nodes.json' in files:
                # Some folders might only have nodes if there are no relationships (unlikely for valid db, but possible)
                # We can check if relationships.json is missing but nodes.json exists.
                # Based on file listing, most have both.
                pass

        print(f"Found {len(tasks)} datasets to convert.")
        
        for folder_path in tqdm(tasks, desc="Converting"):
            rel_path = os.path.relpath(folder_path, self.input_root)
            output_dir = os.path.join(self.output_root, rel_path)
            dataset_name = os.path.basename(folder_path)
            output_file = os.path.join(output_dir, f"{dataset_name}.pkl")
            
            os.makedirs(output_dir, exist_ok=True)
            
            nodes_file = os.path.join(folder_path, "nodes.json")
            rels_file = os.path.join(folder_path, "relationships.json")
            
            try:
                self.convert_single(nodes_file, rels_file, output_file)
            except Exception as e:
                print(f"Error converting {folder_path}: {e}")

    def convert_single(self, nodes_path, rels_path, output_path):
        G = nx.DiGraph()
        
        with open(nodes_path, 'r', encoding='utf-8') as f:
            nodes_data = json.load(f)
            
        with open(rels_path, 'r', encoding='utf-8') as f:
            rels_data = json.load(f)
            
        old_id_map = {}
        
        # 1. Process Nodes
        for node_item in nodes_data:
            old_id = node_item.get('old_id')
            labels = node_item.get('labels', [])
            props = node_item.get('properties', {})
            
            node_type = labels[0] if labels else "Unknown"
            
            # Determine Node ID based on type
            if "Table" in labels:
                node_id = props.get("name")
                # Ensure type attribute is set
                props["type"] = "Table"
                # Ensure default list attributes exist (consistent with builder.py)
                props.setdefault("reference_to", [])
                props.setdefault("referenced_by", [])
            elif "Column" in labels:
                table_name = props.get("belongs_to")
                col_name = props.get("name")
                if table_name and col_name:
                    node_id = f"{table_name}.{col_name}"
                else:
                    # Fallback if names are missing
                    node_id = str(old_id)
                props["type"] = "Column"
                # Ensure default list attributes exist (consistent with builder.py)
                props.setdefault("referenced_to", [])
                props.setdefault("referenced_by", [])
            else:
                # Other types?
                node_id = str(old_id)
                props["type"] = node_type
                
            if node_id:
                old_id_map[old_id] = node_id
                # Clean up properties if needed, but keeping them all is usually safer
                G.add_node(node_id, **props)

        # 2. Process Relationships
        for rel in rels_data:
            start_old = rel.get("start_old_id")
            end_old = rel.get("end_old_id")
            rel_type = rel.get("type")
            props = rel.get("properties", {})
            
            # Map to new IDs
            start_node = old_id_map.get(start_old)
            end_node = old_id_map.get(end_old)
            
            if start_node and end_node:
                # Add edge with type and properties
                # NetworkX edge attributes are flattened
                edge_attrs = {"type": rel_type}
                edge_attrs.update(props)
                G.add_edge(start_node, end_node, **edge_attrs)
            else:
                # If nodes are missing (filtered out?), skip
                pass
                
        # 3. Save to PKL
        with open(output_path, 'wb') as f:
            pickle.dump(G, f)

if __name__ == "__main__":
    # Default paths based on user request and environment
    input_repo = r"d:\MVP-SQL\graphrepo"
    # User didn't specify output, creating a new directory parallel to graphrepo or inside it
    # Let's create a new top-level directory for the converted pkls
    output_repo = r"d:\MVP-SQL\converted_graph_pkl"
    
    converter = GraphRepoConverter(input_repo, output_repo)
    converter.convert_all()
