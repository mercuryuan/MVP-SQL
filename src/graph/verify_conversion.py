import pickle
import networkx as nx
import os

def verify_graph(pkl_path):
    print(f"Verifying {pkl_path}...")
    if not os.path.exists(pkl_path):
        print("File not found!")
        return

    with open(pkl_path, 'rb') as f:
        G = pickle.load(f)

    print(f"Nodes: {G.number_of_nodes()}")
    print(f"Edges: {G.number_of_edges()}")

    print("\n--- Sample Nodes ---")
    count = 0
    for node, data in G.nodes(data=True):
        print(f"ID: {node}")
        print(f"Data: {data}")
        count += 1
        if count >= 3:
            break

    print("\n--- Sample Edges ---")
    count = 0
    for u, v, data in G.edges(data=True):
        print(f"{u} -> {v}")
        print(f"Data: {data}")
        count += 1
        if count >= 3:
            break
            
    # Check specific nodes if possible (based on previous exploration)
    # phone_1: Table 'phone' and Column 'phone.Company_name'
    if "phone" in G:
        print("\nFound 'phone' table node.")
        print(G.nodes["phone"])
    else:
        print("\n'phone' table node NOT found.")
        
    if "phone.Company_name" in G:
        print("\nFound 'phone.Company_name' column node.")
    
    # Check FK edges
    fk_edges = [ (u,v) for u,v,d in G.edges(data=True) if d.get('type') == 'FOREIGN_KEY']
    print(f"\nFound {len(fk_edges)} FOREIGN_KEY edges.")
    if fk_edges:
        print(f"Sample FK: {fk_edges[0]}")
        print(G.get_edge_data(*fk_edges[0]))

if __name__ == "__main__":
    verify_graph(r"d:\MVP-SQL\schema_graph_repo\spider\phone_1\phone_1.pkl")
