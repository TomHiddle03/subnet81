import asyncio
import logging
import time
from collections import deque
from typing import Any, Dict, Tuple, Deque
from datetime import datetime

from sqlalchemy.ext.asyncio import create_async_engine

from patrol.chain_data.event_fetcher import EventFetcher
from patrol.chain_data.event_processor import EventProcessor
from patrol.chain_data.substrate_client import SubstrateClient
from patrol.chain_data.runtime_groupings import load_versions
from patrol.chain_data.coldkey_finder import ColdkeyFinder
from patrol.validation import hooks
from patrol.validation.config import DB_URL
from patrol.validation.persistence import Base
from patrol.validation.persistence.event_store_repository import DatabaseEventStoreRepository
from patrol.validation.persistence.missed_blocks_repository import MissedBlockReason, MissedBlocksRepository

logger = logging.getLogger(__name__)

class MissedBlocksRetryTask:
    def __init__(
        self,
        event_fetcher: EventFetcher,
        event_processor: EventProcessor,
        event_repository: DatabaseEventStoreRepository,
        missed_blocks_repository: MissedBlocksRepository,
        retry_interval_seconds: int = 300,
        batch_size: int = 25,
        buffer_size: int = 5000  # Number of events to store in buffer before processing
    ):
        self.event_fetcher = event_fetcher
        self.event_processor = event_processor
        self.event_repository = event_repository
        self.missed_blocks_repository = missed_blocks_repository
        self.retry_interval_seconds = retry_interval_seconds
        self.batch_size = batch_size
        self.buffer_size = buffer_size
        self.running = False
        
    def _convert_to_db_format(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert an event from the processor format to the database format.
        """
        evidence = event.get('evidence', {})
                
        db_event = {
            "created_at": datetime.now(),
            "coldkey_source": event.get("coldkey_source"),
            "coldkey_destination": event.get("coldkey_destination"),
            "edge_category": event.get("category"),
            "edge_type": event.get("type"),
            "coldkey_owner": event.get("coldkey_owner"),
            "block_number": evidence.get("block_number"),
            "rao_amount": evidence.get("rao_amount")
        }
        
        if db_event["edge_category"] == "staking":
            db_event.update({
                "destination_net_uid": evidence.get("destination_net_uid"),
                "source_net_uid": evidence.get("source_net_uid"),
                "alpha_amount": evidence.get("alpha_amount"),
                "delegate_hotkey_source": evidence.get("delegate_hotkey_source"),
                "delegate_hotkey_destination": evidence.get("delegate_hotkey_destination")
            })
        
        return db_event
            
    async def _retry_missed_blocks(self, blocks_to_retry: list) -> None:
        """
        Attempt to fetch and process missed blocks using producer-consumer pattern.
        """
        if not blocks_to_retry:
            logger.info("No missed blocks to retry")
            return
            
        logger.info(f"Attempting to retry {len(blocks_to_retry)} missed blocks")
        
        # Create a queue for communication between producer and consumer
        queue = asyncio.Queue()
        
        # List to track successfully processed blocks
        successful_blocks = []
        blocks_without_events = []
        async def process_buffered_events(buffer: Deque[Tuple[int, Any]]) -> None:
            """Process a buffer of events and store them in the database."""
            if not buffer:
                return
            
            to_process = dict(buffer)
            processed_batch = await self.event_processor.process_event_data(to_process)
            logger.info(f"Processed {len(processed_batch)} events from retry!")
            
            event_data_list = []
            blocks_with_events = set()
            
            for event in processed_batch:
                event_data = self._convert_to_db_format(event)
                event_data_list.append(event_data)

                # Add the block number to blocks_with_events. Once its gone through the event processor we
                # can be sure it has at least 1 transfer or staking event.
                if 'block_number' in event_data:
                    blocks_with_events.add(event_data['block_number'])
    
            # Store events to DB
            if event_data_list:
                try:
                    await self.event_repository.add_events(event_data_list)
                    logger.info(f"Stored {len(event_data_list)} events from retried blocks!")
                    
                    # Add successfully processed blocks to list, for removal from missed blocs repo
                    successful_blocks.extend(list(blocks_with_events))
                except Exception as e:
                    logger.error(f"Error storing events in database: {e}")

            if len(to_process.keys()) != len(blocks_with_events):
                blocks_without_events.extend(
                    set(to_process.keys()) - blocks_with_events
                )

        async def consumer_event_queue() -> None:
            """Consumer coroutine that processes events from the queue."""
            buffer: Deque[Tuple[int, Any]] = deque()
            
            while True:
                events = await queue.get()
                if events is None:
                    break
                
                buffer.extend(events.items())
                while len(buffer) >= self.buffer_size:
                    temp_buffer = deque(buffer.popleft() for _ in range(self.buffer_size))
                    await process_buffered_events(temp_buffer)
            
            # Process any remaining events in the buffer
            await process_buffered_events(buffer)
        
        try:
            # Start the producer and consumer tasks
            missed_blocks = []  # To track blocks that fail again
            producer_task = asyncio.create_task(
                self.event_fetcher.stream_all_events(
                    blocks_to_retry, 
                    queue, 
                    missed_blocks,
                    batch_size=self.batch_size
                )
            )
            consumer_task = asyncio.create_task(consumer_event_queue())
            
            # Wait for both tasks to complete
            await asyncio.gather(producer_task, consumer_task)
            
            # Remove the successfully processed blocks from the repository
            if successful_blocks:
                unique_successful_blocks = list(set(successful_blocks))
                await self.missed_blocks_repository.remove_blocks(unique_successful_blocks)
                logger.info(f"Successfully processed and removed {len(unique_successful_blocks)} previously missed blocks")
            
        except Exception as e:
            logger.error(f"Error in producer-consumer pattern: {e}")
        finally:
            # Record any missed blocks
            if missed_blocks:
                logger.warning(f"Recording {len(missed_blocks)} missed blocks in retry task!")
                await self.missed_blocks_repository.add_missed_blocks(
                    missed_blocks,
                    error_message=f"Failed fetching blocks during missed block retry!",
                    reason=MissedBlockReason.FETCH_FAILURE
                )
            if blocks_without_events: 
                logger.warning(f"Recording {len(blocks_without_events)} blocks which don't have events.")
                await self.missed_blocks_repository.add_missed_blocks(
                    blocks_without_events,
                    error_message=f"Block does not contain transfer/staking events.",
                    reason=MissedBlockReason.NO_EVENTS
                )

    async def _retry_loop(self) -> None:
        """Main retry loop."""
        try:
            while self.running:
                start_time = time.time()
                    
                # Get all missed blocks
                all_missed_blocks = await self.missed_blocks_repository.get_all_missed_blocks()
                
                if not all_missed_blocks:
                    logger.info("No missed blocks to retry")
                else:
                    # Convert set to sorted list for predictable processing
                    all_missed_blocks = sorted(list(all_missed_blocks))
                    logger.info(f"Found {len(all_missed_blocks)} missed blocks to retry")
                    
                    block_retry_batch_size = 1000
                    # Process missed blocks in batches
                    for i in range(0, len(all_missed_blocks), block_retry_batch_size):
                        batch = all_missed_blocks[i:i + block_retry_batch_size]
                        logger.info(f"Processing batch of {len(batch)} missed blocks ({i+1}-{i+len(batch)} of {len(all_missed_blocks)})")
                        await self._retry_missed_blocks(batch)
                
                # Calculate time to wait before next retry
                elapsed = time.time() - start_time
                wait_time = max(0, self.retry_interval_seconds - elapsed)
                
                logger.info(f"Retry task completed in {elapsed:.2f}s. Waiting {wait_time:.2f}s before next retry...")
                await asyncio.sleep(wait_time)
            
        except Exception as e:
            logger.error(f"Unexpected error in retry loop: {e}")
            self.running = False

    async def start(self) -> None:
        """Start the retry task."""
        if self.running:
            logger.warning("MissedBlocksRetryTask is already running")
            return
            
        self.running = True
        self._task = asyncio.create_task(self._retry_loop())
        logger.info("Started missed blocks retry task")
        
    async def stop(self) -> None:
        """Stop the retry task."""
        if not self.running:
            return
            
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
            
        logger.info("Stopped missed blocks retry task")

async def create_tables(engine):
    """Create all database tables if they don't exist."""
    logger.info("Creating database tables if they don't exist...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables created or confirmed to exist.")

async def main():
    logging.basicConfig(level=logging.INFO)
    
    # Setup substrate client
    network_url = "wss://archive.chain.opentensor.ai:443/"
    versions = load_versions()
    client = SubstrateClient(runtime_mappings=versions, network_url=network_url, max_retries=3)
    await client.initialize()
    
    # Setup components
    fetcher = EventFetcher(substrate_client=client)
    coldkey_finder = ColdkeyFinder(client)
    processor = EventProcessor(coldkey_finder=coldkey_finder)
    
    # Setup database
    engine = create_async_engine(DB_URL, pool_pre_ping=True)
    hooks.invoke(hooks.HookType.ON_CREATE_DB_ENGINE, engine)

    # Create tables before using them
    await create_tables(engine)

    event_repository = DatabaseEventStoreRepository(engine)
    missed_blocks_repository = MissedBlocksRepository(engine)
    
    # Create and start the retry task
    retry_task = MissedBlocksRetryTask(
        event_fetcher=fetcher,
        event_processor=processor,
        event_repository=event_repository,
        missed_blocks_repository=missed_blocks_repository,
        retry_interval_seconds=300  # Retry every 5 minutes
    )
    
    await retry_task.start()
    
    # Run indefinitely
    try:
        while True:
            await asyncio.sleep(3600)  # Just to keep the main task alive
    finally:
        await retry_task.stop()

if __name__ == "__main__":
    asyncio.run(main())