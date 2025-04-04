from typing import Dict, Any
import bittensor as bt
import asyncio
import time
import json

from patrol.protocol import GraphPayload, Edge, Node, StakeEvidence, TransferEvidence
from patrol.validation.graph_validation.errors import PayloadValidationError, ErrorPayload
from patrol.chain_data.event_fetcher import EventFetcher
from patrol.chain_data.event_parser import process_event_data
from patrol.chain_data.coldkey_finder import ColdkeyFinder

class BittensorValidationMechanism:

    def __init__(self,  event_fetcher: EventFetcher, coldkey_finder: ColdkeyFinder):
        self.graph_payload = None
        self.event_fetcher = event_fetcher
        self.coldkey_finder = coldkey_finder

    async def validate_payload(self, uid: int, payload: Dict[str, Any] = None, target: str = None) -> Dict[str, Any]:
        start_time = time.time()
        bt.logging.info(f"Starting validation process for uid: {uid}")

        try:
            if not payload:
                raise PayloadValidationError("Empty/Null Payload recieved.")
            self.parse_graph_payload(payload)
            
            self.verify_target_in_graph(target)

            self.verify_graph_connected()

            await self.verify_edge_data()

        except Exception as e: 
            bt.logging.error(f"Validation error for uid {uid}: {e}")
            self.graph_payload = ErrorPayload(message=f"Error: {str(e)}")

        validation_time = time.time() - start_time
        bt.logging.info(f"Validation finished for {uid}. Completed in {validation_time:.2f} seconds")

        return self.return_validated_payload()

    def parse_graph_payload(self, payload: dict) -> None:
        """
        Parses a dictionary into a GraphPayload data structure.
        This will raise an error if required fields are missing, if there are extra fields,
        or if a duplicate edge is found.
        """
        nodes = []
        edges = []
        try:
            seen_nodes = set()
            for node in payload['nodes']:
                node_id = node.get("id")
                if node_id in seen_nodes:
                    raise PayloadValidationError(f"Duplicate node detected: {node_id}")
                seen_nodes.add(node_id)
                nodes.append(Node(**node))
            
            seen_edges = set()  # To track unique edges
            for edge in payload['edges']:
                # Create a key tuple from the edge properties
                evidence = edge.get('evidence')
                if evidence is None:
                    raise PayloadValidationError("Edge is missing the 'evidence' field.")
                
                key = (
                    edge.get('coldkey_source'),
                    edge.get('coldkey_destination'),
                    edge.get('category'),
                    edge.get('type'),
                    evidence.get('rao_amount'),
                    evidence.get('block_number')
                )
                
                # Check for duplicate edge
                if key in seen_edges:
                    raise PayloadValidationError(f"Duplicate edge detected: {key}")
                seen_edges.add(key)

                if edge.get('category') == "balance":                
                    edges.append(
                        Edge(
                            coldkey_source=edge['coldkey_source'],
                            coldkey_destination=edge['coldkey_destination'],
                            category=edge['category'],
                            type=edge['type'],
                            evidence=TransferEvidence(**edge['evidence'])
                        )
                    )
                elif edge.get('category') == "staking":
                    edges.append(
                        Edge(
                            coldkey_source=edge['coldkey_source'],
                            coldkey_destination=edge['coldkey_destination'],
                            coldkey_owner=edge.get('coldkey_owner'),
                            category=edge['category'],
                            type=edge['type'],
                            evidence=StakeEvidence(**edge['evidence'])
                        )
                    )

        except TypeError as e:
            raise PayloadValidationError(f"Payload validation error: {e}")
        
        self.graph_payload = GraphPayload(nodes=nodes, edges=edges)
    
    def verify_target_in_graph(self, target: str) -> None:

        def find_target(target):
            for edge in self.graph_payload.edges:
                if edge.coldkey_destination == target:
                    return True
                elif edge.coldkey_source == target:
                    return True
                elif edge.coldkey_owner == target:
                    return True
            return False
        
        if not find_target(target):
            raise PayloadValidationError("Target not found in payload.")

    def verify_graph_connected(self):
        """
        Checks whether the graph is fully connected using a union-find algorithm.
        Raises a ValueError if the graph is not fully connected.
        """
        # Initialize union-find parent dictionary for each node
        parent = {}

        def find(x: str) -> str:
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(x: str, y: str):
            rootX = find(x)
            rootY = find(y)
            if rootX != rootY:
                parent[rootY] = rootX

        # Initialize each node's parent to itself
        for node in self.graph_payload.nodes:
            parent[node.id] = node.id

        # Process all edges, treating them as undirected connections
        for edge in self.graph_payload.edges:
            src = edge.coldkey_source
            dst = edge.coldkey_destination
            own = edge.coldkey_owner

            if src not in parent or dst not in parent:
                raise ValueError("Edge refers to a node not in the payload")

            union(src, dst)

            if own:
                if own not in parent:
                    raise ValueError("Edge owner refers to a node not in the payload")
                union(src, own)
                union(dst, own)

        # Check that all nodes have the same root
        roots = {find(node.id) for node in self.graph_payload.nodes}
        if len(roots) != 1:
            raise ValueError("Graph is not fully connected.")
    
    async def verify_edge_data(self):

        block_numbers = []

        for edge in self.graph_payload.edges:
            block_numbers.append(edge.evidence.block_number)

        events = await self.event_fetcher.fetch_all_events(block_numbers)

        processed_events = await process_event_data(events, self.coldkey_finder)

        # Create a normalized event key set from on-chain events
        event_keys = {}

        event_keys = set()
        for event in processed_events:
            evidence = event.get('evidence', {})
            event_key = json.dumps({
                "coldkey_source": event.get("coldkey_source"),
                "coldkey_destination": event.get("coldkey_destination"),
                "coldkey_owner": event.get("coldkey_owner"),
                "category": event.get("category"),
                "type": event.get("type"),
                "rao_amount": evidence.get("rao_amount"),
                "block_number": evidence.get("block_number"),
                "destination_net_uid": evidence.get("destination_net_uid"),
                "source_net_uid": evidence.get("source_net_uid"),
                "alpha_amount": evidence.get("alpha_amount"),
                "delegate_hotkey_source": evidence.get("delegate_hotkey_source"),
                "delegate_hotkey_destination": evidence.get("delegate_hotkey_destination"),
            }, sort_keys=True)
            event_keys.add(event_key)

        # Check each graph edge against processed chain events
        missing_edges = []
        for edge in self.graph_payload.edges:
            ev = vars(edge.evidence)
            edge_key = json.dumps({
                "coldkey_source": edge.coldkey_source,
                "coldkey_destination": edge.coldkey_destination,
                "coldkey_owner": edge.coldkey_owner,
                "category": edge.category,
                "type": edge.type,
                "rao_amount": ev.get("rao_amount"),
                "block_number": ev.get("block_number"),
                "destination_net_uid": ev.get("destination_net_uid"),
                "source_net_uid": ev.get("source_net_uid"),
                "alpha_amount": ev.get("alpha_amount"),
                "delegate_hotkey_source": ev.get("delegate_hotkey_source"),
                "delegate_hotkey_destination": ev.get("delegate_hotkey_destination"),
            }, sort_keys=True)

            if edge_key not in event_keys:
                missing_edges.append(edge_key)

        if missing_edges:
            raise PayloadValidationError(f"{len(missing_edges)} edges not found in on-chain events.")

        bt.logging.debug("All edges matched with on-chain events.")

    def return_validated_payload(self):
        return self.graph_payload

# Example usage:
if __name__ == "__main__":

    from async_substrate_interface import AsyncSubstrateInterface
    import json

    bt.debug()

    from patrol.constants import Constants

    file_path = "example_subgraph_output.json"
    with open(file_path, "r") as f:
        payload = json.load(f)

    async def main():

        fetcher = EventFetcher()
        await fetcher.initialize_substrate_connections()

        async with AsyncSubstrateInterface(url=Constants.ARCHIVE_NODE_ADDRESS) as substrate:
            coldkey_finder = ColdkeyFinder(substrate)

            validator = BittensorValidationMechanism(fetcher, coldkey_finder)
            
            # Run the validation
            result = await validator.validate_payload(uid=1, payload=payload, target="5EPdHVcvKSMULhEdkfxtFohWrZbFQtFqwXherScM7B9F6DUD")
            # bt.logging.info("Validated Payload:", result)

    asyncio.run(main())