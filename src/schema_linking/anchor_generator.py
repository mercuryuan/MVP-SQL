import os
import sys
from pathlib import Path
import logging

# Add project root to sys.path to ensure imports work
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from configs import paths
from src.utils.graph_loader import GraphLoader

logger = logging.getLogger(__name__)


class DatabaseAnchorGenerator:
    def __init__(self, repo_path=None):
        if repo_path:
            self.repo_path = repo_path
        else:
            # Default to the new schema_graph_repo path
            self.repo_path = os.path.join(paths.OUTPUT_ROOT, "schema_graph_repo")

    def get_first_dataset_first_data(self):
        """
        Reads the first available graph dataset and outputs its first node data.
        Returns:
            tuple: (node_id, node_attributes) of the first node found, or None.
        """
        if not os.path.exists(self.repo_path):
            logger.error(f"Repo path does not exist: {self.repo_path}")
            return None

        logger.info(f"Scanning repository at: {self.repo_path}")

        # Traverse directory to find first .pkl file
        for root, dirs, files in os.walk(self.repo_path):
            for file in files:
                if file.endswith(".pkl"):
                    pkl_path = os.path.join(root, file)
                    logger.info(f"Found dataset: {pkl_path}")

                    G = GraphLoader.load_graph(pkl_path)
                    if G and G.number_of_nodes() > 0:
                        # Get the first node
                        first_node = list(G.nodes(data=True))[0]

                        print("\n" + "=" * 50)
                        print(f"First Data from {file}:")
                        print("-" * 20)
                        print(f"Node ID: {first_node[0]}")
                        print(f"Attributes: {first_node[1]}")
                        print("=" * 50 + "\n")

                        return first_node
                    else:
                        logger.warning(f"Graph loaded from {pkl_path} is empty or invalid.")

        logger.warning("No .pkl files found in repository.")
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    generator = DatabaseAnchorGenerator()
    generator.get_first_dataset_first_data()
