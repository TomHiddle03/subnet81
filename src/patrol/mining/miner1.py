import time
import asyncio
import argparse
import traceback
from threading import Thread
from asyncio import run_coroutine_threadsafe
from typing import Tuple

import bittensor as bt
from bittensor import AsyncSubtensor
from bittensor.utils.networking import get_external_ip

from patrol.protocol import PatrolSynapse
from patrol.mining.graph_generator import GraphGenerator

def get_event_loop():
    loop = asyncio.new_event_loop()
    thread = Thread(target=loop.run_forever, daemon=True)
    thread.start()
    return loop

class Miner:
    def __init__(self, dev_flag: bool, wallet_path: str, coldkey: str, hotkey: str, port: int, external_ip: str, netuid: int, subtensor: AsyncSubtensor, min_stake_allowed: int, network_url: str, max_future_events: int= 50, max_past_events: int = 50, batch_size: int = 25):
        self.dev_flag = dev_flag
        self.wallet_path = wallet_path
        self.coldkey = coldkey
        self.hotkey = hotkey
        self.port = port
        self.external_ip = external_ip
        self.netuid = netuid
        self.subtensor = subtensor
        self.min_stake_allowed = min_stake_allowed
        self.network_url = network_url
        self.max_future_events = max_future_events
        self.max_past_events = max_past_events
        self.batch_size = batch_size
        self.subgraph_loop = get_event_loop()
        self.graph_generator = None

    async def setup_bittensor_objects(self):
        bt.logging.info("Setting up Bittensor objects.")

        if self.external_ip is None:
            self.external_ip = "0.0.0.0" if self.dev_flag else get_external_ip()

        self.wallet = bt.wallet(self.coldkey, self.hotkey, path=self.wallet_path)
        self.wallet.create_if_non_existent(False, False)
        bt.logging.info(f"Wallet: {self.wallet}")

        if not self.dev_flag:
            self.metagraph = await self.subtensor.metagraph(self.netuid)
            bt.logging.info(f"Metagraph: {self.metagraph}")

            if self.wallet.hotkey.ss58_address not in self.metagraph.hotkeys:
                bt.logging.error(f"\nYour miner: {self.wallet} is not registered. Run 'btcli register'.")
                exit()
            self.my_subnet_uid = self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)

    def blacklist_fn(self, synapse: PatrolSynapse) -> Tuple[bool, str]:
        if self.dev_flag:
            return False, None
        if synapse.dendrite.hotkey not in self.metagraph.hotkeys:
            return True, "Unrecognized hotkey"
        uid = self.metagraph.hotkeys.index(synapse.dendrite.hotkey)
        if not self.metagraph.validator_permit[uid] or self.metagraph.S[uid] < self.min_stake_allowed:
            return True, "Non-validator hotkey"
        return False, None

    async def forward(self, synapse: PatrolSynapse) -> PatrolSynapse:
        bt.logging.info(f"Received request: {synapse.target}, with block number: {synapse.target_block_number}")
        start_time = time.time()
        miner_id = int(''.join(filter(str.isdigit, self.hotkey)))
        future = run_coroutine_threadsafe(
            self.graph_generator.run(synapse.target, synapse.target_block_number, synapse.max_block_number, False),
            self.subgraph_loop
        )
        synapse.subgraph_output = future.result()

        volume = len(synapse.subgraph_output.nodes) + len(synapse.subgraph_output.edges)
        time_consumed = round(time.time() - start_time, 2)
        with open('miner.log', 'a') as f:
            f.write(
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} - "
                f"target={synapse.target}, block_number={synapse.target_block_number}, "
                f"time_consumed={time_consumed}s, volume={volume}, miner_id={miner_id}\n"
            )
        bt.logging.info(f"Returning a graph of {volume} in {time_consumed} seconds.")
        return synapse

    async def setup_axon(self):
        self.axon = bt.axon(wallet=self.wallet, port=self.port, external_ip=self.external_ip)
        self.axon.attach(forward_fn=self.forward, blacklist_fn=self.blacklist_fn)
        if not self.dev_flag:
            await self.subtensor.serve_axon(
                netuid=self.netuid,
                axon=self.axon
            )
        self.axon.start()

    async def setup_miner(self):
        try:
            self.graph_generator = GraphGenerator()
            bt.logging.info("Successfully initialised, waiting for requests...")
            return True
        except Exception as e:
            bt.logging.error(f"Unsuccessfuly attempted to set up miner dependencies. Error: {e}")
            exit()

    async def run(self):
        future = run_coroutine_threadsafe(
            self.setup_miner(),
            self.subgraph_loop
        )
        await self.setup_bittensor_objects()
        await self.setup_axon()

        step = 0
        while True:
            try:
                if step % 60 == 0 and not self.dev_flag:
                    await self.metagraph.sync()
                    bt.logging.info(f"Block: {self.metagraph.block.item()} | Incentive: {self.metagraph.I[self.my_subnet_uid]}")
                step += 1
                time.sleep(1)
            except KeyboardInterrupt:
                self.axon.stop()
                break
            except Exception:
                bt.logging.debug(traceback.format_exc())
                continue

async def boot():
    parser = argparse.ArgumentParser()
    parser.add_argument('--netuid', type=int, default=81)
    parser.add_argument('--wallet_path', type=str, default="~/.bittensor/wallets/")
    parser.add_argument('--coldkey', type=str, default="miners")
    parser.add_argument('--hotkey', type=str, default="miner_1")
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--external_ip', type=str, default=None)
    parser.add_argument('--dev_flag', type=bool, default=False)
    parser.add_argument('--min_stake_allowed', type=int, default=30000)
    parser.add_argument('--subtensor_address', type=str, default="finney")
    parser.add_argument('--archive_node_address', type=str, default="wss://archive.chain.opentensor.ai:443/")
    parser.add_argument('--max_future_events', type=int, default=50)
    parser.add_argument('--max_past_events', type=int, default=50)
    parser.add_argument('--event_batch_size', type=int, default=25)
    args = parser.parse_args()

    async with AsyncSubtensor(network=args.subtensor_address) as subtensor:
        miner = Miner(
            dev_flag=args.dev_flag,
            wallet_path=args.wallet_path,
            coldkey=args.coldkey,
            hotkey=args.hotkey,
            port=args.port,
            external_ip=args.external_ip,
            netuid=args.netuid,
            subtensor=subtensor,
            min_stake_allowed=args.min_stake_allowed,
            network_url=args.archive_node_address,
            max_future_events=args.max_future_events,
            max_past_events=args.max_past_events,
            batch_size=args.event_batch_size
        )
        await miner.run()

if __name__ == "__main__":
    asyncio.run(boot())
