import pytest
from unittest.mock import AsyncMock
from patrol.validation.graph_validation.bittensor_validation_mechanism import BittensorValidationMechanism
from patrol.validation.graph_validation.errors import PayloadValidationError, ErrorPayload
from patrol.protocol import GraphPayload, Node, Edge, StakeEvidence, TransferEvidence

@pytest.fixture
def valid_payload():
    return {
        "nodes": [
            {"id": "A", "type": "wallet", "origin": "bittensor"},
            {"id": "B", "type": "wallet", "origin": "bittensor"},
        ],
        "edges": [
            {
                "coldkey_source": "A",
                "coldkey_destination": "B",
                "category": "staking",
                "type": "add",
                "evidence": {
                    "rao_amount": 10,
                    "block_number": 1,
                    "delegate_hotkey_destination": "B",
                    "alpha_amount": 1,
                    "destination_net_uid": 1
                }
            }
        ]
    }

@pytest.mark.asyncio
async def test_validate_payload_success(valid_payload):
    # Create AsyncMock instances for event_fetcher and event_processer.
    event_fetcher = AsyncMock()
    # The edge in valid_payload uses block_number = 1, so we simulate an on-chain event matching that edge.
    processed_event = {
        "coldkey_source": "A",
        "coldkey_destination": "B",
        "coldkey_owner": None,
        "category": "staking",
        "type": "add",
        "evidence": {
            "rao_amount": 10,
            "block_number": 1,
            "delegate_hotkey_destination": "B",
            "alpha_amount": 1,
            "destination_net_uid": 1,
            "source_net_uid": None,
            "delegate_hotkey_source": None,
        }
    }
    # The event_fetcher returns events keyed by block number.
    event_fetcher.fetch_all_events.return_value = {1: [processed_event]}
    # The event_processer then processes those events to return a list.
    event_processer = AsyncMock()
    event_processer.process_event_data.return_value = [processed_event]

    validator = BittensorValidationMechanism(event_fetcher, event_processer)
    result = await validator.validate_payload(uid=1, payload=valid_payload, target="B")

    assert isinstance(result, GraphPayload)
    assert len(result.nodes) == 2
    assert len(result.edges) == 1

@pytest.mark.asyncio
async def test_validate_payload_target_missing(valid_payload):
    # Use dummy dependencies; we only care about target verification here.
    validator = BittensorValidationMechanism(AsyncMock(), AsyncMock())
    validator.parse_graph_payload(valid_payload)
    with pytest.raises(PayloadValidationError, match="Target not found"):
        validator.verify_target_in_graph("Z")

def test_parse_graph_payload_duplicate_nodes():
    payload = {
        "nodes": [
            {"id": "A", "type": "wallet", "origin": "bittensor"},
            {"id": "A", "type": "wallet", "origin": "bittensor"},
        ],
        "edges": []
    }
    validator = BittensorValidationMechanism(AsyncMock(), AsyncMock())
    with pytest.raises(PayloadValidationError, match="Duplicate node"):
        validator.parse_graph_payload(payload)

def test_verify_graph_connected_failure():
    # Create a graph payload with two nodes and no edges, which should not be fully connected.
    validator = BittensorValidationMechanism(None, None)
    validator.graph_payload = GraphPayload(
        nodes=[Node(id="A", type="wallet", origin="bittensor"),
               Node(id="B", type="wallet", origin="bittensor")],
        edges=[],
    )
    with pytest.raises(ValueError, match="not fully connected"):
        validator.verify_graph_connected()

@pytest.mark.asyncio
async def test_verify_edge_data_missing_match(valid_payload):
    # Simulate that no matching on-chain events were found for the provided edge.
    event_fetcher = AsyncMock()
    event_fetcher.fetch_all_events.return_value = {1: []}
    event_processer = AsyncMock()
    event_processer.process_event_data.return_value = []

    validator = BittensorValidationMechanism(event_fetcher, event_processer)
    validator.parse_graph_payload(valid_payload)
    with pytest.raises(PayloadValidationError, match="edges not found"):
        await validator.verify_edge_data()