import time
import asyncio
import json
from typing import Union
import requests

import bittensor as bt

from patrol.protocol import GraphPayload, Node, Edge, TransferEvidence, StakeEvidence

class GraphGenerator:
    
    async def run(self, target_address: str, target_block: int, miner_id: int, dev_mode: bool):
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

        port = 4001 if dev_mode else 3000
        url = f"http://localhost:{port}/transfers?fromAddress={target_address}&blockNumber={target_block}&minerId={miner_id}"
        
        graph = None  # Initialize graph to avoid UnboundLocalError
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

            graph = GraphPayload(nodes=nodes, edges=edges)
        except requests.exceptions.RequestException as e:
            bt.logging.error(f"Request failed: {e}")
            raise

        return graph
if __name__ == "__main__":

    async def example():
        bt.debug()

        # Load samples from JSON file
        try:
            with open('sample.json', 'r') as sample_file:
                samples = json.load(sample_file).get('samples', [])
            
            if not samples:
                bt.logging.error("No samples found in sample.json. Using default configuration.")
                samples = [{
                    'target_address': '5DkRocgbM16F41BLGs3UMoqwKdrbmkzQiUHgnzLHXrV9frob',
                    'target_block': 4666564,
                    'miner_id': 1,
                    'dev_mode': True
                }]
        except FileNotFoundError:
            bt.logging.error("sample.json not found. Using default configuration.")
            samples = [{
                'target_address': '5DkRocgbM16F41BLGs3UMoqwKdrbmkzQiUHgnzLHXrV9frob',
                'target_block': 4666564,
                'miner_id': 1,
                'dev_mode': True
            }]
        except json.JSONDecodeError:
            bt.logging.error("Invalid JSON in sample.json. Using default configuration.")
            samples = [{
                'target_address': '5DkRocgbM16F41BLGs3UMoqwKdrbmkzQiUHgnzLHXrV9frob',
                'target_block': 4666564,
                'miner_id': 1,
                'dev_mode': True
            }]

        graph_generator = GraphGenerator()
        results = []

        # Iterate through each configuration
        for idx, config in enumerate(samples, 1):
            try:
                target_address = config.get('target_address')
                target_block = config.get('target_block')
                miner_id = config.get('miner_id')
                dev_mode = config.get('dev_mode')

                # Validate configuration
                if not all([target_address, target_block is not None, miner_id is not None, dev_mode is not None]):
                    bt.logging.error(f"Invalid configuration {idx}: {config}. Skipping.")
                    continue

                bt.logging.info(f"Running configuration {idx}: {config}")
                start_time = time.time()

                graph = await graph_generator.run(
                    target_address=target_address,
                    target_block=target_block,
                    miner_id=miner_id,
                    dev_mode=dev_mode
                )
                volume = len(graph.nodes) + len(graph.edges)
                
                elapsed_time = time.time() - start_time
                bt.logging.info(f"Configuration {idx} finished: {elapsed_time} seconds with volume: {volume}")
                
                results.append({
                    'config_index': idx,
                    'config': config,
                    'volume': volume,
                    'elapsed_time': elapsed_time
                })
            except Exception as e:
                bt.logging.error(f"Error processing configuration {idx}: {config}. Error: {e}")
                results.append({
                    'config_index': idx,
                    'config': config,
                    'error': str(e)
                })

        # Log summary of results
        bt.logging.info("Summary of results:")
        for result in results:
            if 'error' in result:
                bt.logging.info(f"Config {result['config_index']}: Failed with error: {result['error']}")
            else:
                bt.logging.info(f"Config {result['config_index']}: Volume={result['volume']}, Time={result['elapsed_time']:.2f}s")

        return results
    asyncio.run(example())


