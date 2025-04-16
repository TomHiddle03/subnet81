import time
import asyncio
from typing import Union
import requests

import bittensor as bt

from patrol.protocol import GraphPayload, Node, Edge, TransferEvidence, StakeEvidence

class GraphGenerator:
    
    async def run(self, target_address:str, target_block:int, miner_id: int, dev_mode: bool):

        def convert_evidence(edge_dict: dict) -> Union[TransferEvidence, StakeEvidence]:
            """Convert evidence dictionary to TransferEvidence or StakeEvidence based on edge category."""
            evidence_dict = edge_dict.get('evidence', {})
            category = edge_dict.get('category', '')

            if category == 'balance':
                return TransferEvidence(
                    rao_amount=evidence_dict.get('rao_amount', 0),
                    block_number=evidence_dict.get('block_number', 0)
                )
            elif category == 'staking':
                return StakeEvidence(
                    block_number=evidence_dict.get('block_number', 0),
                    rao_amount=evidence_dict.get('rao_amount', 0),
                    destination_net_uid=evidence_dict.get('destination_net_uid'),
                    source_net_uid=evidence_dict.get('source_net_uid'),
                    alpha_amount=evidence_dict.get('alpha_amount'),
                    delegate_hotkey_source=evidence_dict.get('delegate_hotkey_source'),
                    delegate_hotkey_destination=evidence_dict.get('delegate_hotkey_destination')
                )
            else:
                raise ValueError(f"Unknown edge category: {category}")

        port = 4000 if dev_mode else 3000
        url = f"http://localhost:{port}/transfers?fromAddress={target_address}&blockNumber={target_block}&minerId={miner_id}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            nodes = []
            for node_dict in data['nodes']:
                try:
                    nodes.append(Node(
                        id=node_dict.get('id', ''),
                        type=node_dict.get('type', ''),
                        origin=node_dict.get('origin', '')
                    ))
                except TypeError as e:
                    bt.logging.error(f"Failed to convert node: {node_dict}, error: {e}")
                    raise ValueError(f"Invalid node data: {node_dict}")

            edges = []
            for edge_dict in data['edges']:
                try:
                    evidence = convert_evidence(edge_dict)
                    edges.append(Edge(
                        coldkey_source=edge_dict.get('coldkey_source', ''),
                        coldkey_destination=edge_dict.get('coldkey_destination', ''),
                        category=edge_dict.get('category', ''),
                        type=edge_dict.get('type', ''),
                        evidence=evidence,
                        coldkey_owner=edge_dict.get('coldkey_owner')
                    ))
                except (TypeError, ValueError) as e:
                    bt.logging.error(f"Failed to convert edge: {edge_dict}, error: {e}")
                    raise ValueError(f"Invalid edge data: {edge_dict}")

            subgraph = GraphPayload(nodes=nodes, edges=edges)
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}") 

        return subgraph

if __name__ == "__main__":

    async def example():

        bt.debug()

        target_address = "5E7FS5G6avaFRqaL8gwuzy6BtWQphA8niEEeeqeC4X6eeQ7A"
        target_block = 3898992
        
        start_time = time.time()

        graph_generator = GraphGenerator()
        graph = await graph_generator.run(target_address=target_address, target_block=target_block, miner_id=1, dev_mode=True)
        volume = len(graph.nodes) + len(graph.edges)
        
        bt.logging.info(f"Finished: {time.time() - start_time} with volume: {volume}")

    asyncio.run(example())


