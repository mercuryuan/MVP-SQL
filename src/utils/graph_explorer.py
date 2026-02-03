from ast import main
import networkx as nx
import logging
from typing import List, Dict, Any, Set

import configs.paths

logger = logging.getLogger(__name__)


class GraphExplorer:
    """
    NetworkX implementation of the graph explorer, replacing the old Neo4jExplorer.
    Operates on a NetworkX DiGraph loaded from a pickle file.
    """

    def __init__(self, graph: nx.DiGraph):
        self.graph = graph

    def get_all_nodes(self) -> List[Dict[str, Any]]:
        """
        Get all nodes with their labels and properties.
        """
        nodes = []
        for node_id, data in self.graph.nodes(data=True):
            # Simulate Neo4j structure: labels is a list
            labels = [data.get("type", "Unknown")]
            nodes.append({"labels": labels, "properties": data})
        return nodes

    def get_all_relationships(self) -> List[Dict[str, Any]]:
        """
        Get all relationships (edges) with their type and properties.
        """
        relationships = []
        for u, v, data in self.graph.edges(data=True):
            relationships.append({"type": data.get("type"), "properties": data})
        return relationships

    def get_all_tables(self) -> Dict[str, Any]:
        """
        Get all table nodes.
        Returns: Dict {table_name: properties}
        """
        tables = {}
        for node_id, data in self.graph.nodes(data=True):
            if data.get("type") == "Table":
                tables[data.get("name")] = data
        return tables

    def get_all_columns(self) -> List[Dict[str, Any]]:
        """
        Get all column nodes properties.
        """
        columns = []
        for node_id, data in self.graph.nodes(data=True):
            if data.get("type") == "Column":
                columns.append(data)
        return columns

    def get_all_foreign_keys(self) -> List[Dict[str, Any]]:
        """
        Get all foreign key edges properties.
        """
        foreign_keys = []
        for u, v, data in self.graph.edges(data=True):
            if data.get("type") == "FOREIGN_KEY":
                foreign_keys.append(data)
        return foreign_keys

    def get_columns_for_table(self, table_name: str, max_retries=5, retry_delay=1) -> Dict[str, Any]:
        """
        Get columns for a specific table.
        Retries parameters are kept for API compatibility but are not needed for in-memory graph.
        """
        columns = {}
        if table_name in self.graph:
            # Check outgoing edges for HAS_COLUMN
            for neighbor in self.graph.successors(table_name):
                edge_data = self.graph.get_edge_data(table_name, neighbor)
                if edge_data and edge_data.get("type") == "HAS_COLUMN":
                    node_data = self.graph.nodes[neighbor]
                    if node_data.get("type") == "Column":
                        # Use node properties as column info
                        columns[node_data.get("name")] = node_data

        if not columns:
            # Just a warning, not necessarily an error (table might have no columns or not exist)
            # But following original logic, it might print warning.
            # Original code: print warning if not found.
            pass
            # logger.warning(f"No columns found for table '{table_name}'")

        return columns

    def get_neighbor_tables(self, table_name: str, n_hop: int) -> List[str]:
        """
        Get neighbor tables within n_hop distance via FOREIGN_KEY relationships.
        Treats FOREIGN_KEY as undirected.
        """
        if table_name not in self.graph:
            logger.warning(f"Table {table_name} not found in graph.")
            return []

        visited = {table_name}
        current_layer = {table_name}

        for _ in range(n_hop):
            next_layer = set()
            for node in current_layer:
                # Check outgoing FKs
                for neighbor in self.graph.successors(node):
                    if neighbor == node: continue
                    edge_data = self.graph.get_edge_data(node, neighbor)
                    if edge_data.get("type") == "FOREIGN_KEY":
                        if self.graph.nodes[neighbor].get("type") == "Table":
                            if neighbor not in visited:
                                visited.add(neighbor)
                                next_layer.add(neighbor)

                # Check incoming FKs
                for neighbor in self.graph.predecessors(node):
                    if neighbor == node: continue
                    edge_data = self.graph.get_edge_data(neighbor, node)
                    if edge_data.get("type") == "FOREIGN_KEY":
                        if self.graph.nodes[neighbor].get("type") == "Table":
                            if neighbor not in visited:
                                visited.add(neighbor)
                                next_layer.add(neighbor)

            current_layer = next_layer

        neighbors = list(visited - {table_name})
        if not neighbors:
            pass
            # logger.warning(f"No {n_hop}-hop neighbors found for {table_name}")

        return neighbors

    def is_subgraph_connected(self, selected_tables: List[str]) -> bool:
        """
        Check if the subgraph induced by selected_tables (connected by FKs) is connected.
        """
        if not selected_tables:
            return False

        sub_nodes = set(selected_tables)
        visited = set()
        queue = [selected_tables[0]]

        while queue:
            curr = queue.pop(0)
            if curr in visited:
                continue
            visited.add(curr)

            if curr not in self.graph: continue

            # Check neighbors that are in sub_nodes
            # Outgoing
            for neighbor in self.graph.successors(curr):
                if neighbor in sub_nodes and neighbor not in visited:
                    if self.graph[curr][neighbor].get("type") == "FOREIGN_KEY":
                        queue.append(neighbor)

            # Incoming
            for neighbor in self.graph.predecessors(curr):
                if neighbor in sub_nodes and neighbor not in visited:
                    if self.graph[neighbor][curr].get("type") == "FOREIGN_KEY":
                        queue.append(neighbor)

        return len(visited) == len(selected_tables)

    def bfs_subgraph(self, selected_tables: List[str]) -> List[List[str]]:
        """
        Perform BFS starting from selected_tables.
        Returns layers of visited tables.
        """
        all_tables = set(self.get_all_tables().keys())
        invalid_tables = [t for t in selected_tables if t not in all_tables]
        if invalid_tables:
            # raise RuntimeError(f"[ERROR] Tables not found: {invalid_tables}")
            logger.error(f"[ERROR] Tables not found: {invalid_tables}")
            # Original code raised string? "raise (f...)" -> Actually "raise (f...)" evaluates to raising a TypeError (exception class must be type).
            # Original code line 144: `raise (f"[ERROR] ...")` which is a bug in original code (raise "string").
            # I will fix it to raise RuntimeError or just log and return empty.
            return []

        if not self.is_subgraph_connected(selected_tables):
            logger.error("[ERROR] Selected subgraph is not connected.")
            return []

        visited = set(selected_tables)
        queue = [(t, 0) for t in selected_tables]
        result = []

        # To handle level separation correctly (original logic appends list of tables for each level processed in batch)
        # Original code logic:
        # while queue:
        #    level_size = len(queue)
        #    level_tables = []
        #    for _ in range(level_size): ...
        #    result.append(level_tables)

        while queue:
            level_size = len(queue)
            level_tables = []

            for _ in range(level_size):
                current_table, level = queue.pop(0)
                level_tables.append(current_table)

                # 1-hop neighbors
                neighbors = self.get_neighbor_tables(current_table, 1)
                for neighbor in neighbors:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append((neighbor, level + 1))

            if level_tables:
                result.append(level_tables)

        unvisited_tables = all_tables - visited
        if unvisited_tables:
            logger.debug(f"[DEBUG] Unvisited tables: {unvisited_tables}")

        return result

    def get_foreign_keys_between_tables(self, table1: str, table2: str) -> List[str]:
        """
        Get reference paths for FKs between two tables.
        """
        paths = []

        # t1 -> t2
        if self.graph.has_edge(table1, table2):
            data = self.graph[table1][table2]
            if data.get("type") == "FOREIGN_KEY":
                if data.get("reference_path"):
                    paths.append(data["reference_path"])

        # t2 -> t1
        if self.graph.has_edge(table2, table1):
            data = self.graph[table2][table1]
            if data.get("type") == "FOREIGN_KEY":
                if data.get("reference_path"):
                    paths.append(data["reference_path"])

        return paths


if __name__ == "__main__":
    from src.utils.graph_loader import GraphLoader

    pkl_path = configs.paths.OUTPUT_ROOT / "schema_graph_repo" / "bird" / "books" / "books.pkl"
    G = GraphLoader.load_graph(pkl_path)
    # 全面的图探索
    explorer = GraphExplorer(G)
    logger.info(f"Graph loaded successfully with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")
    # 功能逐一测试
    # 测试连通性
    connected = explorer.is_subgraph_connected(["book", "publisher"])
    logger.info(f"Subgraph connected: {connected}")

    # 测试BFS层序遍历
    layers = explorer.bfs_subgraph(["book", "publisher"])
    logger.info(f"BFS layers: {layers}")

    # 测试外键路径查询
    paths = explorer.get_foreign_keys_between_tables("book", "publisher")
    logger.info(f"Foreign key paths between book and author: {paths}")
