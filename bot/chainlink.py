import time
from web3 import AsyncWeb3
from .config import settings
from typing import Dict, Optional

AGGREGATOR_ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"}
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"internalType": "uint80", "name": "_roundId", "type": "uint80"}],
        "name": "getRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"}
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function"
    }
]

class ChainlinkFetcher:
    def __init__(self):
        self.cached_decimals = None
        self.cached_result = {"price": None, "updatedAt": None, "source": "chainlink"}
        self.cached_fetched_at_ms = 0
        self.min_fetch_interval_ms = 2000
        self.preferred_rpc_url = None

    def get_ordered_rpcs(self):
        from_list = settings.POLYGON_RPC_URLS
        single = [settings.POLYGON_RPC_URL] if settings.POLYGON_RPC_URL else []
        defaults = [
            "https://polygon-bor-rpc.publicnode.com",
            "https://polygon.drpc.org",
            "https://1rpc.io/matic"
        ]
        all_rpcs = list(dict.fromkeys(from_list + single + defaults))
        if self.preferred_rpc_url and self.preferred_rpc_url in all_rpcs:
            all_rpcs.remove(self.preferred_rpc_url)
            return [self.preferred_rpc_url] + all_rpcs
        return all_rpcs

    async def fetch_chainlink_btc_usd(self) -> Dict:
        now = time.time() * 1000
        if self.cached_fetched_at_ms and now - self.cached_fetched_at_ms < self.min_fetch_interval_ms:
            return self.cached_result

        rpcs = self.get_ordered_rpcs()
        if not rpcs:
            return {"price": None, "updatedAt": None, "source": "missing_config"}

        aggregator_address = settings.CHAINLINK_BTC_USD_AGGREGATOR
        for rpc in rpcs:
            try:
                w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc, request_kwargs={'timeout': 3.5, 'ssl': False}))
                contract = w3.eth.contract(address=AsyncWeb3.to_checksum_address(aggregator_address), abi=AGGREGATOR_ABI)

                if self.cached_decimals is None:
                    self.cached_decimals = await contract.functions.decimals().call()

                round_data = await contract.functions.latestRoundData().call()
                answer = round_data[1]
                updated_at = round_data[3]

                price = answer / (10 ** self.cached_decimals)
                self.cached_result = {
                    "price": price,
                    "updatedAt": updated_at * 1000,
                    "source": "chainlink"
                }
                self.cached_fetched_at_ms = now
                self.preferred_rpc_url = rpc
                return self.cached_result
            except Exception:
                self.cached_decimals = None
                continue

        return self.cached_result

    async def fetch_round_at_timestamp(self, target_ts: int, symbol: str = "BTC") -> Optional[Dict]:
        """Binary search historical Chainlink aggregator rounds on Polygon to pinpoint
        the exact round and price that was active at target_ts (seconds or ms).
        Polymarket resolves against the latest Chainlink price where updatedAt <= eventStartTime."""
        if target_ts > 1e11:
            target_ts = int(target_ts / 1000)

        rpcs = self.get_ordered_rpcs()
        if not rpcs:
            return None

        aggregator_address = settings.get_aggregator(symbol)
        for rpc in rpcs:
            try:
                w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc, request_kwargs={'timeout': 4.0, 'ssl': False}))
                contract = w3.eth.contract(address=AsyncWeb3.to_checksum_address(aggregator_address), abi=AGGREGATOR_ABI)

                if self.cached_decimals is None:
                    self.cached_decimals = await contract.functions.decimals().call()

                latest_round = await contract.functions.latestRoundData().call()
                latest_id, answer, started_at, updated_at, answered_in = latest_round

                # If the target is at or after latest round, latest is the answer
                if updated_at <= target_ts:
                    price = answer / (10 ** self.cached_decimals)
                    return {
                        "price": price,
                        "updatedAt": updated_at * 1000,
                        "roundId": latest_id,
                        "source": "chainlink_latest"
                    }

                phase_id = latest_id >> 64
                latest_offset = latest_id & 0xFFFFFFFFFFFFFFFF

                dt = updated_at - target_ts
                # Polygon Chainlink BTC updates roughly every ~20-30s
                est_rounds = max(5, int(dt / 25))
                low = max(1, latest_offset - est_rounds * 3 - 30)
                high = min(latest_offset, latest_offset - max(0, int(est_rounds * 0.2) - 10))

                try:
                    low_id = (phase_id << 64) | low
                    low_rd = await contract.functions.getRoundData(low_id).call()
                    if low_rd[3] > target_ts:
                        low = max(1, latest_offset - est_rounds * 8 - 100)
                except Exception:
                    low = max(1, latest_offset - 2000)

                best = None
                while low <= high:
                    mid = (low + high) // 2
                    mid_id = (phase_id << 64) | mid
                    try:
                        rd = await contract.functions.getRoundData(mid_id).call()
                        r_ts = rd[3]
                        if r_ts <= target_ts:
                            best = rd
                            low = mid + 1
                        else:
                            high = mid - 1
                    except Exception:
                        high = mid - 1

                if best and best[3] > 0:
                    price = best[1] / (10 ** self.cached_decimals)
                    self.preferred_rpc_url = rpc
                    return {
                        "price": price,
                        "updatedAt": best[3] * 1000,
                        "roundId": best[0],
                        "source": "chainlink_historical"
                    }
            except Exception:
                continue

        return None


chainlink_fetcher = ChainlinkFetcher()
