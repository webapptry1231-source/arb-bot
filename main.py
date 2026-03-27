import asyncio
import aiohttp
import time
import math
import random
import os
import logging
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from dotenv import load_dotenv
import aiosqlite
import json
from web3 import AsyncWeb3, AsyncHTTPProvider

load_dotenv()

# =============================================================================
# CONFIGURATION
# CONFIGURATION (RELAXED)
# =============================================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

CONCURRENCY_LIMIT = 20
RATE_LIMIT_SLEEP = 0.05              # DexScreener ~10 req/s per IP
SPREAD_HISTORY_LEN = 60              # not used, kept for compatibility

V3_DEX_IDS = {
    "uniswap-v3", "pancakeswap-v3", "camelot-v3", "sushiswap-v3",
    "aerodrome-cl", "velodrome-v2", "quickswap-v3", "zyberswap",
    "ramses-v2", "thena-fusion", "algebra",
}

EVM_CHAINS = {
    "base":     {"rpcs": ["https://mainnet.base.org"],         "gecko": "base"},
    "arbitrum": {"rpcs": ["https://arb1.arbitrum.io/rpc"],     "gecko": "arbitrum"},
    "bsc":      {"rpcs": ["https://bsc-dataseed.binance.org"], "gecko": "bsc"},
    "polygon":  {"rpcs": ["https://polygon-rpc.com"],          "gecko": "polygon_pos"},
    "ethereum": {"rpcs": ["https://eth.llamarpc.com"],         "gecko": "ethereum"},
    "optimism": {"rpcs": ["https://mainnet.optimism.io"],      "gecko": "optimism"},
}
GECKO_TO_CHAIN = {v["gecko"]: k for k, v in EVM_CHAINS.items()}
GECKO_TO_CHAIN["polygon_pos"] = "polygon"

MULTICALL3_ADDR = "0xcA11bde05977b3631167028862bE2a173976CA11"

DEX_FEES = {
    "uniswap": 30, "pancakeswap": 25, "sushiswap": 30, "camelot": 30,
    "aerodrome": 5, "velodrome": 5, "quickswap": 30, "thena": 5,
    "ramses": 5, "zyberswap": 30, "algebra": 30, "gecko": 30, "unknown": 30,
}

GAS_COST_CHAIN = {
    "ethereum": 8.0, "arbitrum": 0.7, "base": 0.15, "polygon": 0.2,
    "bsc": 0.25, "optimism": 0.5,
}
GAS_MULTIPLIER = 1.2

# Chain latency (seconds) – fixed, not random
CHAIN_LATENCY = {
    "ethereum": 1.0, "arbitrum": 0.3, "base": 0.2, "polygon": 0.4,
    "bsc": 0.4, "optimism": 0.5,
}

STABLECOIN_SYMBOLS = {"USDT", "USDC", "DAI", "BUSD", "TUSD", "FDUSD",
                      "FRAX", "LUSD", "USDD", "GUSD", "USDP"}
MAJOR_QUOTE_SYMBOLS = {"WETH", "ETH", "WBTC", "BTC", "WBNB", "BNB", "WMATIC", "MATIC"}
ALLOWED_QUOTE_SYMBOLS = STABLECOIN_SYMBOLS | MAJOR_QUOTE_SYMBOLS
QUOTE_USD_MIN = 0.0001
QUOTE_USD_MAX = 200_000
# Precision mode (user chose option 2: fewer but higher-confidence candidates)
NON_STABLE_LIQUIDITY_MULTIPLIER = 2.0
NON_STABLE_VOLUME_MULTIPLIER = 2.0

MIN_LIQUIDITY             = 10_000
MAX_LIQUIDITY             = 20_000_000
MIN_VOLUME_LIQ_RATIO      = 0.1
MIN_POOLS                 = 2
MAX_POOLS                 = 10
MIN_OPPORTUNITY_COUNT     = 1
MIN_AVG_SPREAD            = 0.003
MIN_AVG_DURATION          = 1.5
MIN_LIQUIDITY_TARGET      = 30_000
TOP_N_PAIRS               = 8
MIN_PROFIT_POTENTIAL_USD  = 1.0
MIN_POOLS                 = 2
MAX_POOLS                 = 10
MIN_OPPORTUNITY_COUNT     = 1
MIN_AVG_SPREAD            = 0.003
MIN_AVG_DURATION          = 1.5
MIN_LIQUIDITY_TARGET      = 30_000
TOP_N_PAIRS               = 8
MIN_PROFIT_POTENTIAL_USD  = 1.0

# --- RELAXED FILTERS ---
MIN_LIQUIDITY             = 10_000        # was 20k
MAX_LIQUIDITY             = 2_000_000     # was 500k
MIN_VOLUME_LIQ_RATIO      = 0.5           # was 2.0
MIN_POOLS                 = 2
MAX_POOLS                 = 10            # was 4
MIN_OPPORTUNITY_COUNT     = 1             # was 3
MIN_AVG_SPREAD            = 0.005         # 0.5% – lower to catch small opportunities
MIN_AVG_DURATION          = 1.5           # was 2.5
MIN_LIQUIDITY_TARGET      = 30_000
TOP_N_PAIRS               = 8
MIN_PROFIT_POTENTIAL_USD  = 1.0           # was 8.0 – we'll let observation decide later
MAX_SLIPPAGE_IMPACT       = 0.018
FLASHLOAN_FEE_BPS         = 9
MIN_CONFIDENCE_SCORE      = 0.55

DISCOVERY_MIN_VOLUME      = 5_000
DISCOVERY_MIN_LIQUIDITY   = 20_000
MIN_POOL_LIQUIDITY        = 20_000

# Reserve freshness: fetch every 5 seconds (aligns with observation interval)
RESERVE_TTL = 5

MAX_POOL_AGE              = 60
MAX_PRICE_JUMP_PCT        = 0.05

CHAIN_DIFFICULTY = {
    "base": 1, "polygon": 1, "optimism": 2, "arbitrum": 3, "bsc": 3, "ethereum": 4,
}
LATENCY_THRESHOLD = 2.0
OPPORTUNITY_DECAY_THRESHOLD = 1.0


# Reserve freshness: fetch every 5 seconds (aligns with observation interval)
RESERVE_TTL = 5

MAX_POOL_AGE              = 60
MAX_PRICE_JUMP_PCT        = 0.05

CHAIN_DIFFICULTY = {
    "base": 1, "polygon": 1, "optimism": 2, "arbitrum": 3, "bsc": 3, "ethereum": 4,
}
LATENCY_THRESHOLD = 2.0
OPPORTUNITY_DECAY_THRESHOLD = 1.0

NORMAL_INTERVAL = 2.0
HOT_INTERVAL = 0.3
BURST_DETECTION_WINDOW = 5

DEXSCREENER_TOKEN_URL  = "https://api.dexscreener.com/latest/dex/tokens/{}"
DEXSCREENER_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search?q={}"
GECKO_BASE             = "https://api.geckoterminal.com/api/v2"

DEXSCREENER_QUERIES = ["new", "trending", "hot", "base", "sol", "bsc", "arb", "eth", "polygon"]


def chain_rpcs(chain: str) -> List[str]:
    cfg = EVM_CHAINS.get(chain, {})
    defaults = cfg.get("rpcs", [])
    env_key = f"RPCS_{chain.upper()}"
    env_val = os.getenv(env_key, "").strip()
    if not env_val:
        return defaults
    parsed = [u.strip() for u in env_val.split(",") if u.strip()]
    return parsed or defaults

# =============================================================================
# AMM Helpers
# =============================================================================

def get_amount_out(amount_in: float, reserve_in: float, reserve_out: float, fee_bps: int = 30) -> float:
    if reserve_in == 0 or reserve_out == 0:
        return 0.0
    amount_in_with_fee = amount_in * (10000 - fee_bps)
    numerator = amount_in_with_fee * reserve_out
    denominator = reserve_in * 10000 + amount_in_with_fee
    return numerator / denominator if denominator > 0 else 0.0

def is_healthy_pool(p: Dict) -> bool:
    if p["r_base"] <= 0 or p["r_quote"] <= 0:
        return False
    price = p["r_quote"] / p["r_base"]
    base_value = p["r_base"] * price
    quote_value = p["r_quote"]
    ratio = base_value / quote_value if quote_value > 0 else 0
    if ratio > 10 or ratio < 0.1:
        return False
    if base_value < 5000:
        return False
    return True

# =============================================================================
# DATA MODELS
# =============================================================================

class TokenMemory:
    def __init__(self, token_key: str):
        self.key = token_key
        self.opportunity_count = 0
        self.avg_spread = 0.0
        self.max_spread = 0.0
        self.avg_duration = 0.0
        self.spread_std = 0.0
        self.successful_cycles = 0
        self.failed_cycles = 0
        self.competition_score = 0
        self.behavior_type = "NEW"
        self.last_seen = 0.0
        self.last_spike_time = 0.0
        self.spike_frequency = 0
        self.avg_spike_interval = 0.0
        self.recovery_time_avg = 0.0
        self.spread_decay_rate = 0.0
        self.time_of_day_histogram = {}
        self.fake_breakouts = 0
        self.real_breakouts = 0
        self.best_trade_size = 0.0
        self.profit_per_trade_avg = 0.0
        self.last_5_spreads = deque(maxlen=5)
        self.last_trade_time = 0.0
        self._prev_spread = 0.0
        self.last_liquidity = 0.0
        self.last_update_time = 0.0
        self.missed_opportunities = 0
        self.decay_avg = 0.0
        self.alpha_token = False

    def update(self, spread, duration, trade_usd=0, net_profit=0):
        self.opportunity_count += 1; n = self.opportunity_count
        self.avg_spread = (self.avg_spread*(n-1) + spread)/n
        self.max_spread = max(self.max_spread, spread)
        self.avg_duration = (self.avg_duration*(n-1) + duration)/n
        if n>1: self.spread_std = math.sqrt(((self.spread_std**2)*(n-1) + (spread - self.avg_spread)**2)/n)
        self.last_seen = time.time()
        if spread > self.avg_spread*2:
            now = time.time()
            if self.last_spike_time: self.avg_spike_interval = (self.avg_spike_interval*self.spike_frequency + (now - self.last_spike_time))/(self.spike_frequency+1)
            self.last_spike_time = now; self.spike_frequency += 1
        if self._prev_spread and (decay:=self._prev_spread - spread)>0:
            self.spread_decay_rate = (self.spread_decay_rate*(n-1) + decay)/n
            self.decay_avg = (self.decay_avg*(n-1) + decay)/n
        self._prev_spread = spread
        hour = int(time.time()//3600%24)
        self.time_of_day_histogram[hour] = self.time_of_day_histogram.get(hour,0)+1
        if trade_usd:
            self.best_trade_size = self.best_trade_size*0.7 + trade_usd*0.3 if self.best_trade_size else trade_usd
        if net_profit:
            self.profit_per_trade_avg = self.profit_per_trade_avg*0.7 + net_profit*0.3 if self.profit_per_trade_avg else net_profit
        self.last_5_spreads.append(spread)
        self.last_update_time = time.time()

    def compute_repeatability_score(self) -> float:
        if self.opportunity_count < MIN_OPPORTUNITY_COUNT: return 0.0
        consistency = 1/(1+self.spread_std)
        success_rate = self.successful_cycles / max(1,self.successful_cycles+self.failed_cycles)
        hour = int(time.time()//3600%24)
        time_boost = self.time_of_day_histogram.get(hour,0)/max(1,self.opportunity_count)
        fake_penalty = self.fake_breakouts/(self.real_breakouts+1) if self.real_breakouts else 0
        decay = max(0.5, 1 - (time.time()-self.last_seen)/3600)
        score = (self.avg_spread*0.3 + min(self.opportunity_count/50,1)*0.2 + min(self.avg_duration/10,1)*0.15 + consistency*0.15 + time_boost*0.1 + success_rate*0.1 - fake_penalty*0.1 - self.competition_score*0.05)*decay
        return max(0.0, min(1.0, score))


class TokenState:
    def __init__(self, token_key: str, chain: str, contract: str, symbol: str,
                 pools: List[Dict], liquidity_total: float, volume_24h: float):
        self.key = token_key
        self.chain = chain
        self.contract = contract
        self.symbol = symbol
        self.pools = pools
        self.liquidity_total = liquidity_total
        self.volume_24h = volume_24h
        self.last_update = time.time()
        self.active_opportunity = None
        self.memory = TokenMemory(token_key)
        self.cex_price = 0.0
        self.dex_spread = 0.0
        self.max_real_profit = 0.0
        # Backward-compat alias used by older logic/logging paths
        self.profit_potential = 0.0
        self.optimal_trade_size = 0.0
        self.pool_timestamps: Dict[str, float] = {}
        self.price_history: Dict[str, deque] = {}
        self.base_decimals = 18
        self.quote_decimals = 6
        self.last_reserve_fetch = 0.0
        self.blacklisted = False
        self.confidence_score = 0.0

    def compute_spreads_and_profit(self):
        # Always compute from current reserves; if none, result will be zero
        now = time.time()
        valid_pools = []
        for p in self.pools:
            if p.get("pool_type") != "v2":
                continue
            quote = p.get("quote_symbol", "").upper()
            if quote not in {"USDC", "USDT"}:
                continue

            quote = p.get("quote_symbol", "").upper()
            if quote not in ALLOWED_QUOTE_SYMBOLS:
                continue

            quote_usd = p.get("quote_usd", 0.0)
            if quote in STABLECOIN_SYMBOLS:
                quote_usd = 1.0
            elif not (QUOTE_USD_MIN <= quote_usd <= QUOTE_USD_MAX):
                continue

            if "timestamp" in p and now - p["timestamp"] > MAX_POOL_AGE:
                continue
            if p.get("price", 0) <= 0:
                continue
            addr = p.get("pool_address", "")
            if addr:
                if addr not in self.price_history:
                    self.price_history[addr] = deque(maxlen=2)
                self.price_history[addr].append(p["price"])
                if len(self.price_history[addr]) == 2:
                    old, new = self.price_history[addr]
                    jump = (new - old) / old if old > 0 else 0
                    if abs(jump) > MAX_PRICE_JUMP_PCT:
                        continue
            if "r_base" not in p or "r_quote" not in p:
                continue
            if not is_healthy_pool(p):
                continue
            valid_pools.append(p)

        if len(valid_pools) < 2:
            self.dex_spread = 0.0
            self.max_real_profit = 0.0
            self.profit_potential = 0.0
            self.confidence_score = 0.0
            return

        prices = []
        for p in valid_pools:
            price = p["r_quote"] / p["r_base"] if p["r_base"] > 0 else 0.0
            if price <= 0:
                continue
            prices.append(price)
        if not prices:
            self.dex_spread = 0.0
            self.confidence_score = 0.0
            return
        mn, mx = min(prices), max(prices)
        if mn == 0:
            self.dex_spread = 0.0
            self.confidence_score = 0.0
            return
        raw_spread = (mx - mn) / mn
        if raw_spread > 0.03:
            self.dex_spread = 0.0
            self.confidence_score = 0.0
            return
        if raw_spread < 0.001:
            self.dex_spread = 0.0
            self.confidence_score = 0.0
            return
        self.dex_spread = raw_spread

        # Confidence score for downstream execution routing
        avg_age = sum(max(0.0, now - p.get("timestamp", now)) for p in valid_pools) / len(valid_pools)
        freshness = max(0.0, min(1.0, 1 - (avg_age / MAX_POOL_AGE)))
        pool_depth = min(1.0, len(valid_pools) / 4)
        liquidity = min(1.0, self.liquidity_total / 100_000)
        spread_quality = max(0.0, min(1.0, (raw_spread - 0.001) / 0.02))
        quote_quality_count = 0
        for p in valid_pools:
            qsym = p.get("quote_symbol", "").upper()
            qusd = p.get("quote_usd", 0.0)
            if qsym in STABLECOIN_SYMBOLS:
                quote_quality_count += 1
            elif QUOTE_USD_MIN <= qusd <= QUOTE_USD_MAX:
                quote_quality_count += 1
        quote_quality = quote_quality_count / len(valid_pools)
        self.confidence_score = max(0.0, min(1.0, (
            freshness * 0.3
            + pool_depth * 0.2
            + liquidity * 0.2
            + spread_quality * 0.15
            + quote_quality * 0.15
        )))

        cheapest = min(valid_pools, key=lambda x: x["r_quote"] / x["r_base"])
        most_expensive = max(valid_pools, key=lambda x: x["r_quote"] / x["r_base"])

        min_pool_liq_usd = min(cheapest["r_quote"], most_expensive["r_quote"])
        max_size_usd = min_pool_liq_usd * 0.02
        test_sizes = [max_size_usd * f for f in [0.1, 0.25, 0.5, 0.75, 1.0]]

        best_profit = 0.0
        best_size = 0
        gas_cost = GAS_COST_CHAIN.get(self.chain, 0.5) * GAS_MULTIPLIER
        competition_penalty = 1 - min(0.5, self.memory.competition_score * 0.05)
        latency_factor = CHAIN_LATENCY.get(self.chain, 0.5)
        latency_penalty = 1 - min(0.5, latency_factor * 0.8)
        difficulty = CHAIN_DIFFICULTY.get(self.chain, 2)
        difficulty_penalty = 1 - min(0.5, difficulty * 0.1)

        # Fixed latency decay (chain-based, not random)
        execution_delay = latency_factor
        decay_factor = max(0.6, 1 - execution_delay * 0.3)

        for size_usd in test_sizes:
            if size_usd > min_pool_liq_usd * 0.02:
                continue

            amount_base = get_amount_out(
                size_usd,
                cheapest["r_quote"],
                cheapest["r_base"],
                cheapest["fee_bps"]
            )
            if amount_base <= 0:
                continue

            amount_quote_out = get_amount_out(
                amount_base,
                most_expensive["r_base"],
                most_expensive["r_quote"],
                most_expensive["fee_bps"]
            )
            if amount_quote_out <= 0:
                continue

            effective_price = size_usd / amount_base if amount_base > 0 else 0
            market_price = cheapest["r_quote"] / cheapest["r_base"] if cheapest["r_base"] > 0 else 0
            if market_price > 0:
                buy_impact = abs(effective_price - market_price) / market_price
                if buy_impact > MAX_SLIPPAGE_IMPACT:
                    continue

            sell_price = amount_quote_out / amount_base if amount_base > 0 else 0
            expected_sell_price = most_expensive["r_quote"] / most_expensive["r_base"] if most_expensive["r_base"] > 0 else 0
            if expected_sell_price > 0:
                sell_impact = abs(sell_price - expected_sell_price) / expected_sell_price
                if sell_impact > MAX_SLIPPAGE_IMPACT:
                    continue

            profit_usd = amount_quote_out - size_usd
            flash_fee = size_usd * FLASHLOAN_FEE_BPS / 10000
            net_profit = profit_usd - gas_cost - flash_fee
            net_profit *= competition_penalty * latency_penalty * difficulty_penalty * decay_factor
            net_profit *= 0.75   # MEV penalty

            if net_profit > best_profit:
                best_profit = net_profit
                best_size = size_usd

        if best_profit < 0.5:
            self.max_real_profit = 0.0
            self.profit_potential = 0.0
            self.optimal_trade_size = 0.0
            return

        self.max_real_profit = best_profit
        self.profit_potential = best_profit
        self.optimal_trade_size = best_size

            profit_usd = amount_quote_out - size_usd
            flash_fee = size_usd * FLASHLOAN_FEE_BPS / 10000
            net_profit = profit_usd - gas_cost - flash_fee
            net_profit *= competition_penalty * latency_penalty * difficulty_penalty * decay_factor
            net_profit *= 0.75   # MEV penalty

            if net_profit > best_profit:
                best_profit = net_profit
                best_size = size_usd

        if best_profit < 0.5:
            self.max_real_profit = 0.0
            self.optimal_trade_size = 0.0
            return

        self.max_real_profit = best_profit
        self.optimal_trade_size = best_size
        # Use only DEX spread for profit (CEX is optional)
        total_spread = self.dex_spread

        # Realistic trade size: 0.3% of liquidity (not 2%)
        usable_liquidity = self.liquidity_total * 0.003
        repeatability = self.memory.compute_repeatability_score() if self.memory.opportunity_count > 0 else 0.3
        slippage_adjust = 1.0 - MAX_SLIPPAGE_IMPACT
        gross = total_spread * usable_liquidity * slippage_adjust
        net = gross - GAS_COST_USD - (gross * FLASHLOAN_FEE_BPS / 10000)
        self.profit_potential = max(0.0, net)

    def update_from_pools(self, new_pools: List[Dict]):
        self.pools = new_pools
        self.liquidity_total = sum(p["liquidity"] for p in new_pools)
        self.volume_24h = sum(p["volume_24h"] for p in new_pools)
        self.last_update = time.time()
        for p in new_pools:
            addr = p.get("pool_address", "")
            if addr:
                self.pool_timestamps[addr] = p.get("timestamp", time.time())


# =============================================================================
# ON-CHAIN VALIDATOR (Multicall3)
# =============================================================================

MULTICALL3_ABI = [{"inputs":[{"components":[{"name":"target","type":"address"},{"name":"callData","type":"bytes"}],"name":"calls","type":"tuple[]"}],"name":"aggregate","outputs":[{"name":"blockNumber","type":"uint256"},{"name":"returnData","type":"bytes[]"}],"stateMutability":"payable","type":"function"}]
V2_RESERVES_ABI = [{"inputs":[],"name":"getReserves","outputs":[{"name":"_reserve0","type":"uint112"},{"name":"_reserve1","type":"uint112"},{"name":"_blockTimestampLast","type":"uint32"}],"stateMutability":"view","type":"function"}]
V2_TOKEN0_ABI = [{"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"}]
V2_TOKEN1_ABI = [{"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"}]
ERC20_DECIMALS_ABI = [{"inputs":[],"name":"decimals","outputs":[{"type":"uint8"}],"stateMutability":"view","type":"function"}]

class OnChainValidator:
    def __init__(self):
        self._w3 = {}
        self._mc = {}
        self._decimals_cache = {}
        self._sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        self._rpc_index = defaultdict(int)
        self._active_rpc = {}
        self._rpc_stats = defaultdict(lambda: defaultdict(lambda: {"ok": 0, "fail": 0, "lat_ms": 500.0, "last_fail": 0.0}))

    def _pick_best_rpc(self, chain: str, rpcs: List[str]) -> str:
        now = time.time()
        best_rpc = rpcs[0]
        best_score = float("-inf")
        for rpc in rpcs:
            st = self._rpc_stats[chain][rpc]
            success = st["ok"]
            fail = st["fail"]
            total = success + fail
            success_rate = success / total if total else 0.7
            recent_fail_penalty = 0.2 if (now - st["last_fail"]) < 30 else 0.0
            latency_score = max(0.0, 1 - min(st["lat_ms"], 2000.0) / 2000.0)
            score = (success_rate * 0.65) + (latency_score * 0.35) - recent_fail_penalty
            if score > best_score:
                best_score = score
                best_rpc = rpc
        return best_rpc

    def _record_rpc_result(self, chain: str, rpc: str, ok: bool, latency_ms: float = None):
        st = self._rpc_stats[chain][rpc]
        if ok:
            st["ok"] += 1
            if latency_ms is not None:
                st["lat_ms"] = st["lat_ms"] * 0.7 + latency_ms * 0.3
        else:
            st["fail"] += 1
            st["last_fail"] = time.time()

    def _setup(self, chain, rotate: bool = False):
        rpcs = chain_rpcs(chain)
        if not rpcs:
            return
        if chain in self._w3 and not rotate:
            return
        if rotate and chain in self._active_rpc and len(rpcs) > 1:
            current = self._active_rpc[chain]
            self._rpc_index[chain] = (rpcs.index(current) + 1) % len(rpcs) if current in rpcs else 0
            rpc = rpcs[self._rpc_index[chain]]
        else:
            rpc = self._pick_best_rpc(chain, rpcs)
        w3 = AsyncWeb3(AsyncHTTPProvider(rpc, request_kwargs={"timeout":5}))
        self._w3[chain] = w3
        self._mc[chain] = w3.eth.contract(address=AsyncWeb3.to_checksum_address(MULTICALL3_ADDR), abi=MULTICALL3_ABI)
        self._active_rpc[chain] = rpc

    async def _batch_call(self, chain, targets, fn_name, abi, decode_types):
        async with self._sem:
            self._setup(chain)
            w3, mc = self._w3.get(chain), self._mc.get(chain)
            if not w3 or not mc:
                return {}
            calls = []
            valid_targets = []
            for target in targets:
                if not target:
                    continue
                try:
                    c = w3.eth.contract(address=AsyncWeb3.to_checksum_address(target), abi=abi)
                    calls.append((AsyncWeb3.to_checksum_address(target), c.encode_abi(fn_name)))
                    valid_targets.append(target)
                except Exception:
                    continue
            if not calls:
                return {}
            from eth_abi import decode as abi_decode
            results = {}
            BATCH = 25
            for i in range(0, len(calls), BATCH):
                b_calls, b_targets = calls[i:i+BATCH], valid_targets[i:i+BATCH]
                try:
                    t0 = time.time()
                    _, rd = await mc.functions.aggregate(b_calls).call()
                    active_rpc = self._active_rpc.get(chain, "")
                    if active_rpc:
                        self._record_rpc_result(chain, active_rpc, ok=True, latency_ms=(time.time() - t0) * 1000)
                    for j, data in enumerate(rd):
                        try:
                            decoded = abi_decode(decode_types, data)
                            results[b_targets[j]] = decoded
                        except Exception:
                            continue
                except Exception:
                    active_rpc = self._active_rpc.get(chain, "")
                    if active_rpc:
                        self._record_rpc_result(chain, active_rpc, ok=False)
                    # RPC may be stale/rate-limited; rotate provider and retry this batch once
                    self._setup(chain, rotate=True)
                    w3, mc = self._w3.get(chain), self._mc.get(chain)
                    if not w3 or not mc:
                        continue
                    for j, (target, _) in enumerate(b_calls):
                        try:
                            c = w3.eth.contract(address=target, abi=abi)
                            val = await getattr(c.functions, fn_name)().call()
                            results[b_targets[j]] = val if isinstance(val, tuple) else (val,)
                        except Exception:
                            continue
            return results

    async def _get_decimals(self, chain, token_addr):
        key = f"{chain}:{token_addr}"
        if key in self._decimals_cache:
            return self._decimals_cache[key]
        self._setup(chain)
        w3 = self._w3.get(chain)
        if not w3:
            return 18
        try:
            t0 = time.time()
            contract = w3.eth.contract(address=AsyncWeb3.to_checksum_address(token_addr), abi=ERC20_DECIMALS_ABI)
            dec = await contract.functions.decimals().call()
            active_rpc = self._active_rpc.get(chain, "")
            if active_rpc:
                self._record_rpc_result(chain, active_rpc, ok=True, latency_ms=(time.time() - t0) * 1000)
            self._decimals_cache[key] = dec
            return dec
        except Exception:
            active_rpc = self._active_rpc.get(chain, "")
            if active_rpc:
                self._record_rpc_result(chain, active_rpc, ok=False)
            # Retry once on next configured RPC for this chain
            try:
                self._setup(chain, rotate=True)
                w3 = self._w3.get(chain)
                if not w3:
                    return 18
                t0 = time.time()
                contract = w3.eth.contract(address=AsyncWeb3.to_checksum_address(token_addr), abi=ERC20_DECIMALS_ABI)
                dec = await contract.functions.decimals().call()
                active_rpc = self._active_rpc.get(chain, "")
                if active_rpc:
                    self._record_rpc_result(chain, active_rpc, ok=True, latency_ms=(time.time() - t0) * 1000)
                self._decimals_cache[key] = dec
                return dec
            except Exception:
                active_rpc = self._active_rpc.get(chain, "")
                if active_rpc:
                    self._record_rpc_result(chain, active_rpc, ok=False)
                return 18

    async def fetch_reserves_for_token(self, token: TokenState):
        if time.time() - token.last_reserve_fetch < RESERVE_TTL:
            return
        v2_pools = [p for p in token.pools if p["pool_type"] == "v2"]
        if not v2_pools:
            return
        chain = token.chain
        pool_addresses = [p["pool_address"] for p in v2_pools if p["pool_address"]]

        res_map = await self._batch_call(chain, pool_addresses, "getReserves", V2_RESERVES_ABI, ["uint112","uint112","uint32"])
        t0_map = await self._batch_call(chain, pool_addresses, "token0", V2_TOKEN0_ABI, ["address"])
        t1_map = await self._batch_call(chain, pool_addresses, "token1", V2_TOKEN1_ABI, ["address"])

        # Determine quote token address (the token that is not the base)
        quote_token_addr = None
        for p in v2_pools:
            addr = p["pool_address"]
            if addr in t0_map:
                t0_raw = t0_map[addr]
                if isinstance(t0_raw, (tuple, list)) and len(t0_raw) > 0:
                    t0 = t0_raw[0]
                    if t0.lower() != token.contract.lower():
                        quote_token_addr = t0
                        break
            if addr in t1_map and not quote_token_addr:
                t1_raw = t1_map[addr]
                if isinstance(t1_raw, (tuple, list)) and len(t1_raw) > 0:
                    t1 = t1_raw[0]
                    if t1.lower() != token.contract.lower():
                        quote_token_addr = t1
                        break

        base_dec = await self._get_decimals(chain, token.contract)
        quote_dec = 6
        if quote_token_addr:
            quote_dec = await self._get_decimals(chain, quote_token_addr)
        token.base_decimals = base_dec
        token.quote_decimals = quote_dec

        for p in v2_pools:
            addr = p["pool_address"]
            # Validate that we have data for this pool
            if addr not in res_map or addr not in t0_map or addr not in t1_map:
                continue
            # Guard against malformed reserve data
            reserves = res_map[addr]
            if not isinstance(reserves, (tuple, list)) or len(reserves) < 3:
                continue
            r0, r1, _ = reserves
            # Guard against malformed token0/token1 data
            t0_raw = t0_map[addr]
            t1_raw = t1_map[addr]
            if not isinstance(t0_raw, (tuple, list)) or not isinstance(t1_raw, (tuple, list)):
                continue
            if len(t0_raw) < 1 or len(t1_raw) < 1:
                continue
            token0 = t0_raw[0]
            token1 = t1_raw[0]

            p["token0"] = token0
            p["token1"] = token1
            p["reserve0"] = r0
            p["reserve1"] = r1

            if token0.lower() == token.contract.lower():
                p["r_base"] = r0 / (10**base_dec)
                p["r_quote"] = (r1 / (10**quote_dec)) * max(p.get("quote_usd", 0), 0)
            elif token1.lower() == token.contract.lower():
                p["r_base"] = r1 / (10**base_dec)
                p["r_quote"] = (r0 / (10**quote_dec)) * max(p.get("quote_usd", 0), 0)
            else:
                continue

            dex_name = p["dex"].split("-")[0].lower()
            p["fee_bps"] = DEX_FEES.get(dex_name, 30)

        token.last_reserve_fetch = time.time()


# =============================================================================


# =============================================================================
# ON-CHAIN VALIDATOR (Multicall3)
# =============================================================================

MULTICALL3_ABI = [{"inputs":[{"components":[{"name":"target","type":"address"},{"name":"callData","type":"bytes"}],"name":"calls","type":"tuple[]"}],"name":"aggregate","outputs":[{"name":"blockNumber","type":"uint256"},{"name":"returnData","type":"bytes[]"}],"stateMutability":"payable","type":"function"}]
V2_RESERVES_ABI = [{"inputs":[],"name":"getReserves","outputs":[{"name":"_reserve0","type":"uint112"},{"name":"_reserve1","type":"uint112"},{"name":"_blockTimestampLast","type":"uint32"}],"stateMutability":"view","type":"function"}]
V2_TOKEN0_ABI = [{"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"}]
V2_TOKEN1_ABI = [{"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"}]
ERC20_DECIMALS_ABI = [{"inputs":[],"name":"decimals","outputs":[{"type":"uint8"}],"stateMutability":"view","type":"function"}]

class OnChainValidator:
    def __init__(self):
        self._w3 = {}
        self._mc = {}
        self._decimals_cache = {}
        self._sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        self._rpc_index = defaultdict(int)
        self._active_rpc = {}
        self._rpc_stats = defaultdict(lambda: defaultdict(lambda: {"ok": 0, "fail": 0, "lat_ms": 500.0, "last_fail": 0.0}))

    def _pick_best_rpc(self, chain: str, rpcs: List[str]) -> str:
        now = time.time()
        best_rpc = rpcs[0]
        best_score = float("-inf")
        for rpc in rpcs:
            st = self._rpc_stats[chain][rpc]
            success = st["ok"]
            fail = st["fail"]
            total = success + fail
            success_rate = success / total if total else 0.7
            recent_fail_penalty = 0.2 if (now - st["last_fail"]) < 30 else 0.0
            latency_score = max(0.0, 1 - min(st["lat_ms"], 2000.0) / 2000.0)
            score = (success_rate * 0.65) + (latency_score * 0.35) - recent_fail_penalty
            if score > best_score:
                best_score = score
                best_rpc = rpc
        return best_rpc

    def _record_rpc_result(self, chain: str, rpc: str, ok: bool, latency_ms: float = None):
        st = self._rpc_stats[chain][rpc]
        if ok:
            st["ok"] += 1
            if latency_ms is not None:
                st["lat_ms"] = st["lat_ms"] * 0.7 + latency_ms * 0.3
        else:
            st["fail"] += 1
            st["last_fail"] = time.time()

    def _setup(self, chain, rotate: bool = False):
        rpcs = chain_rpcs(chain)
        if not rpcs:
            return
        if chain in self._w3 and not rotate:
            return
        if rotate and chain in self._active_rpc and len(rpcs) > 1:
            current = self._active_rpc[chain]
            self._rpc_index[chain] = (rpcs.index(current) + 1) % len(rpcs) if current in rpcs else 0
            rpc = rpcs[self._rpc_index[chain]]
        else:
            rpc = self._pick_best_rpc(chain, rpcs)
        w3 = AsyncWeb3(AsyncHTTPProvider(rpc, request_kwargs={"timeout":5}))
        self._w3[chain] = w3
        self._mc[chain] = w3.eth.contract(address=AsyncWeb3.to_checksum_address(MULTICALL3_ADDR), abi=MULTICALL3_ABI)
        self._active_rpc[chain] = rpc

    async def _batch_call(self, chain, targets, fn_name, abi, decode_types):
        async with self._sem:
            self._setup(chain)
            w3, mc = self._w3.get(chain), self._mc.get(chain)
            if not w3 or not mc:
                return {}
            calls = []
            valid_targets = []
            for target in targets:
                if not target:
                    continue
                try:
                    c = w3.eth.contract(address=AsyncWeb3.to_checksum_address(target), abi=abi)
                    calls.append((AsyncWeb3.to_checksum_address(target), c.encode_abi(fn_name)))
                    valid_targets.append(target)
                except Exception:
                    continue
            if not calls:
                return {}
            from eth_abi import decode as abi_decode
            results = {}
            BATCH = 25
            for i in range(0, len(calls), BATCH):
                b_calls, b_targets = calls[i:i+BATCH], valid_targets[i:i+BATCH]
                try:
                    t0 = time.time()
                    _, rd = await mc.functions.aggregate(b_calls).call()
                    active_rpc = self._active_rpc.get(chain, "")
                    if active_rpc:
                        self._record_rpc_result(chain, active_rpc, ok=True, latency_ms=(time.time() - t0) * 1000)
                    for j, data in enumerate(rd):
                        try:
                            decoded = abi_decode(decode_types, data)
                            results[b_targets[j]] = decoded
                        except Exception:
                            continue
                except Exception:
                    active_rpc = self._active_rpc.get(chain, "")
                    if active_rpc:
                        self._record_rpc_result(chain, active_rpc, ok=False)
                    # RPC may be stale/rate-limited; rotate provider and retry this batch once
                    self._setup(chain, rotate=True)
                    w3, mc = self._w3.get(chain), self._mc.get(chain)
                    if not w3 or not mc:
                        continue
                    for j, (target, _) in enumerate(b_calls):
                        try:
                            c = w3.eth.contract(address=target, abi=abi)
                            val = await getattr(c.functions, fn_name)().call()
                            results[b_targets[j]] = val if isinstance(val, tuple) else (val,)
                        except Exception:
                            continue
            return results

    async def _get_decimals(self, chain, token_addr):
        key = f"{chain}:{token_addr}"
        if key in self._decimals_cache:
            return self._decimals_cache[key]
        self._setup(chain)
        w3 = self._w3.get(chain)
        if not w3:
            return 18
        try:
            t0 = time.time()
            contract = w3.eth.contract(address=AsyncWeb3.to_checksum_address(token_addr), abi=ERC20_DECIMALS_ABI)
            dec = await contract.functions.decimals().call()
            active_rpc = self._active_rpc.get(chain, "")
            if active_rpc:
                self._record_rpc_result(chain, active_rpc, ok=True, latency_ms=(time.time() - t0) * 1000)
            self._decimals_cache[key] = dec
            return dec
        except Exception:
            active_rpc = self._active_rpc.get(chain, "")
            if active_rpc:
                self._record_rpc_result(chain, active_rpc, ok=False)
            # Retry once on next configured RPC for this chain
            try:
                self._setup(chain, rotate=True)
                w3 = self._w3.get(chain)
                if not w3:
                    return 18
                t0 = time.time()
                contract = w3.eth.contract(address=AsyncWeb3.to_checksum_address(token_addr), abi=ERC20_DECIMALS_ABI)
                dec = await contract.functions.decimals().call()
                active_rpc = self._active_rpc.get(chain, "")
                if active_rpc:
                    self._record_rpc_result(chain, active_rpc, ok=True, latency_ms=(time.time() - t0) * 1000)
                self._decimals_cache[key] = dec
                return dec
            except Exception:
                active_rpc = self._active_rpc.get(chain, "")
                if active_rpc:
                    self._record_rpc_result(chain, active_rpc, ok=False)
                return 18

    async def fetch_reserves_for_token(self, token: TokenState):
        if time.time() - token.last_reserve_fetch < RESERVE_TTL:
            return
        v2_pools = [p for p in token.pools if p["pool_type"] == "v2"]
        if not v2_pools:
            return
        chain = token.chain
        pool_addresses = [p["pool_address"] for p in v2_pools if p["pool_address"]]

        res_map = await self._batch_call(chain, pool_addresses, "getReserves", V2_RESERVES_ABI, ["uint112","uint112","uint32"])
        t0_map = await self._batch_call(chain, pool_addresses, "token0", V2_TOKEN0_ABI, ["address"])
        t1_map = await self._batch_call(chain, pool_addresses, "token1", V2_TOKEN1_ABI, ["address"])

        # Determine quote token address (the token that is not the base)
        quote_token_addr = None
        for p in v2_pools:
            addr = p["pool_address"]
            if addr in t0_map:
                t0_raw = t0_map[addr]
                if isinstance(t0_raw, (tuple, list)) and len(t0_raw) > 0:
                    t0 = t0_raw[0]
                    if t0.lower() != token.contract.lower():
                        quote_token_addr = t0
                        break
            if addr in t1_map and not quote_token_addr:
                t1_raw = t1_map[addr]
                if isinstance(t1_raw, (tuple, list)) and len(t1_raw) > 0:
                    t1 = t1_raw[0]
                    if t1.lower() != token.contract.lower():
                        quote_token_addr = t1
                        break

        base_dec = await self._get_decimals(chain, token.contract)
        quote_dec = 6
        if quote_token_addr:
            quote_dec = await self._get_decimals(chain, quote_token_addr)
        token.base_decimals = base_dec
        token.quote_decimals = quote_dec

        for p in v2_pools:
            addr = p["pool_address"]
            # Validate that we have data for this pool
            if addr not in res_map or addr not in t0_map or addr not in t1_map:
                continue
            # Guard against malformed reserve data
            reserves = res_map[addr]
            if not isinstance(reserves, (tuple, list)) or len(reserves) < 3:
                continue
            r0, r1, _ = reserves
            # Guard against malformed token0/token1 data
            t0_raw = t0_map[addr]
            t1_raw = t1_map[addr]
            if not isinstance(t0_raw, (tuple, list)) or not isinstance(t1_raw, (tuple, list)):
                continue
            if len(t0_raw) < 1 or len(t1_raw) < 1:
                continue
            token0 = t0_raw[0]
            token1 = t1_raw[0]

            p["token0"] = token0
            p["token1"] = token1
            p["reserve0"] = r0
            p["reserve1"] = r1

            if token0.lower() == token.contract.lower():
                p["r_base"] = r0 / (10**base_dec)
                p["r_quote"] = (r1 / (10**quote_dec)) * max(p.get("quote_usd", 0), 0)
            elif token1.lower() == token.contract.lower():
                p["r_base"] = r1 / (10**base_dec)
                p["r_quote"] = (r0 / (10**quote_dec)) * max(p.get("quote_usd", 0), 0)
            else:
                continue

            dex_name = p["dex"].split("-")[0].lower()
            p["fee_bps"] = DEX_FEES.get(dex_name, 30)

        token.last_reserve_fetch = time.time()


# =============================================================================
# SAFE HTTP SESSION (with per-domain lock)
# =============================================================================

class SafeSession:
    def __init__(self):
        self._session = None
        self._last_req = defaultdict(float)
        self._domain_locks: Dict[str, asyncio.Lock] = {}

    async def start(self):
        conn = aiohttp.TCPConnector(limit=200, ttl_dns_cache=300)
        self._session = aiohttp.ClientSession(connector=conn)

    async def stop(self):
        if self._session:
            await self._session.close()

    async def get(self, url, domain="default"):
        if not self._session:
            return None
        # Ensure lock exists for this domain
        if domain not in self._domain_locks:
            self._domain_locks[domain] = asyncio.Lock()
        # Rate limit using per-domain lock
        async with self._domain_locks[domain]:
            gap = time.time() - self._last_req[domain]
            if gap < RATE_LIMIT_SLEEP:
                await asyncio.sleep(RATE_LIMIT_SLEEP - gap)
            self._last_req[domain] = time.time()
        # Actual request outside the lock
        try:
            timeout = aiohttp.ClientTimeout(total=8)
            async with self._session.get(url, timeout=timeout) as r:
                return await r.json() if r.status == 200 else None
        except Exception:
            return None

    async def post(self, url, json_data):
        if not self._session:
            return None
        try:
            timeout = aiohttp.ClientTimeout(total=8)
            async with self._session.post(url, json=json_data, timeout=timeout) as r:
                return await r.json() if r.status == 200 else None
        except Exception:
            return None


# =============================================================================
# TELEGRAM NOTIFIER
# =============================================================================

class TelegramNotifier:
    def __init__(self, token, chat_id, session):
        self.token = token
        self.chat_id = chat_id
        self.session = session
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.last_sent = {}

    @staticmethod
    def _escape_markdown(text: str) -> str:
        escape_chars = r"_*[]()~`>#+-=|{}.!"
        return "".join(f"\\{c}" if c in escape_chars else c for c in str(text))

    async def send(self, text, key=None, cooldown=30):
        if not self.token or not self.chat_id:
            return
        now = time.time()
        if key:
            if key in self.last_sent and now - self.last_sent[key] < cooldown:
                return
            self.last_sent[key] = now
        await self.session.post(self.url, {
            "chat_id": self.chat_id,
            "text": self._escape_markdown(text),
            "parse_mode": "MarkdownV2"
        })


# =============================================================================
# DISCOVERY ENGINE (relaxed, with debug prints)
# =============================================================================

class DiscoveryEngine:
    def __init__(self, session: SafeSession, oc: OnChainValidator):
        self.session = session
        self.oc = oc

    @staticmethod
    def _pf(val, default=0.0):
        try:
            return float(val or default)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_div(num: float, den: float, default: float = 0.0) -> float:
        if den == 0:
            return default
        return num / den

    def _quote_usd_from_pair(self, pair: Dict) -> float:
        qt = pair.get("quoteToken", {})
        qsym = (qt.get("symbol") or "").upper()
        if qsym in STABLECOIN_SYMBOLS:
            return 1.0
        price_usd = self._pf(pair.get("priceUsd"))
        price_native = self._pf(pair.get("priceNative"))
        inferred = self._safe_div(price_usd, price_native)
        return inferred if QUOTE_USD_MIN <= inferred <= QUOTE_USD_MAX else 0.0

    @staticmethod
    def _is_valid_quote_usd(quote_usd: float) -> bool:
        return QUOTE_USD_MIN <= quote_usd <= QUOTE_USD_MAX

    async def _dexscreener_search(self, q: str) -> List[Dict]:
        d = await self.session.get(DEXSCREENER_SEARCH_URL.format(q), "dexscreener")
        if d is None:
            print(f"Dexscreener returned None for query '{q}'")
            return []
        pairs = d.get("pairs", [])
        print(f"Dexscreener query '{q}' -> {len(pairs)} pairs")
        return pairs

    async def _gecko_pools(self, gnet: str, page: int = 1) -> List[Dict]:
        url = f"{GECKO_BASE}/networks/{gnet}/pools?page={page}&include=base_token,quote_token,dex"
        data = await self.session.get(url, "gecko")
        if data is None:
            print(f"Gecko returned None for {gnet}")
            return []
        parsed = self._parse_gecko(data or {}, gnet)
        print(f"Gecko {gnet} page {page} -> {len(parsed)} pools")
        return parsed

    async def _gecko_trending(self, gnet: str) -> List[Dict]:
        url = f"{GECKO_BASE}/networks/{gnet}/trending_pools?include=base_token,quote_token,dex"
        data = await self.session.get(url, "gecko")
        if data is None:
            print(f"Gecko trending returned None for {gnet}")
            return []
        parsed = self._parse_gecko(data or {}, gnet)
        print(f"Gecko trending {gnet} -> {len(parsed)} pools")
        return parsed

    def _parse_gecko(self, data: Dict, gnet: str) -> List[Dict]:
        chain = GECKO_TO_CHAIN.get(gnet)
        if not chain:
            return []
        tok_map = {}
        for item in data.get("included", []):
            if item.get("type") == "token":
                tid = item["id"]
                attrs = item.get("attributes", {})
                addr = attrs.get("address", "")
                if "_" in addr and not addr.startswith("0x"):
                    addr = addr.split("_", 1)[1]
                tok_map[tid] = {"address": addr.lower(), "symbol": attrs.get("symbol", "")}
        results = []
        for p in data.get("data", []):
            attrs = p.get("attributes", {})
            rels = p.get("relationships", {})
            base_id = rels.get("base_token", {}).get("data", {}).get("id", "")
            quot_id = rels.get("quote_token", {}).get("data", {}).get("id", "")
            dex_id = rels.get("dex", {}).get("data", {}).get("id", "gecko")
            bt = tok_map.get(base_id, {})
            qt = tok_map.get(quot_id, {})
            if not bt.get("address") or not qt.get("address"):
                continue
            qsym = qt.get("symbol", "").upper()
            if qsym not in ALLOWED_QUOTE_SYMBOLS:
                continue
            quote_usd = 1.0 if qsym in STABLECOIN_SYMBOLS else self._safe_div(
                self._pf(attrs.get("base_token_price_usd")),
                self._pf(attrs.get("base_token_price_quote_token"))
            )
            if not self._is_valid_quote_usd(quote_usd):
                continue
            pool_type = "v3" if dex_id in V3_DEX_IDS else "v2"
            if pool_type != "v2":
                continue
            results.append({
                "chainId": chain,
                "baseToken": {"address": bt["address"], "symbol": bt.get("symbol", "")},
                "quoteToken": {"address": qt["address"], "symbol": qt.get("symbol", "")},
                "priceUsd": self._pf(attrs.get("base_token_price_usd")),
                "priceNative": self._pf(attrs.get("base_token_price_quote_token")),
                "liquidity": {"usd": self._pf(attrs.get("reserve_in_usd"))},
                "volume": {"h24": self._pf((attrs.get("volume_usd") or {}).get("h24"))},
                "dexId": dex_id,
                "pairAddress": attrs.get("address", ""),
                "timestamp": time.time(),
                "quoteUsd": quote_usd,
                "reserve0": 0, "reserve1": 0, "token0": "", "token1": "",
                "pool_type": pool_type,
            })
        return results

    async def discover_tokens(self) -> List[TokenState]:
        print("\n🔍 Phase 1: Low-competition pair discovery (all chains)...")
        tasks = [self._dexscreener_search(q) for q in DEXSCREENER_QUERIES]
        for cfg in EVM_CHAINS.values():
            gnet = cfg["gecko"]
            tasks += [self._gecko_pools(gnet, 1), self._gecko_pools(gnet, 2), self._gecko_trending(gnet)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_pairs = []
        for r in results:
            if isinstance(r, list):
                all_pairs.extend(r)
            elif isinstance(r, Exception):
                print(f"Error in fetch: {r}")
        print(f"  Raw pairs collected: {len(all_pairs)}")

        token_map = {}
        for p in all_pairs:
            if not isinstance(p, dict):
                continue
            chain = p.get("chainId", "")
            if chain not in EVM_CHAINS:
                continue
            bt = p.get("baseToken", {})
            qt = p.get("quoteToken", {})
            baddr = (bt.get("address") or "").lower().strip()
            qaddr = (qt.get("address") or "").lower().strip()
            qsym = (qt.get("symbol") or "").upper()
            if "_" in baddr and not baddr.startswith("0x"):
                baddr = baddr.split("_", 1)[1]
            if "_" in qaddr and not qaddr.startswith("0x"):
                qaddr = qaddr.split("_", 1)[1]
            if not baddr or not qaddr or baddr == "0x0000000000000000000000000000000000000000":
                continue
            if qsym not in ALLOWED_QUOTE_SYMBOLS:
                continue
            quote_usd = self._pf(p.get("quoteUsd")) or self._quote_usd_from_pair(p)
            if not self._is_valid_quote_usd(quote_usd):
                continue
            liq_raw = p.get("liquidity", {})
            liq = self._pf(liq_raw.get("usd") if isinstance(liq_raw, dict) else liq_raw)
            vol_raw = p.get("volume", {})
            vol = self._pf(vol_raw.get("h24") if isinstance(vol_raw, dict) else vol_raw)
            min_liq = DISCOVERY_MIN_LIQUIDITY * (NON_STABLE_LIQUIDITY_MULTIPLIER if qsym not in STABLECOIN_SYMBOLS else 1.0)
            min_vol = DISCOVERY_MIN_VOLUME * (NON_STABLE_VOLUME_MULTIPLIER if qsym not in STABLECOIN_SYMBOLS else 1.0)
            if liq < min_liq:
                continue
            if vol < min_vol:
                continue
            if vol / liq < MIN_VOLUME_LIQ_RATIO:
                continue
            key = f"{chain}:{baddr}"
            if key not in token_map:
                token_map[key] = {
                    "chain": chain,
                    "address": baddr,
                    "symbol": bt.get("symbol", ""),
                    "pools": [],
                    "total_liquidity": 0.0,
                    "total_volume_24h": 0.0
                }
            ptype = p.get("pool_type", "v3" if p.get("dexId", "") in V3_DEX_IDS else "v2")
            if ptype != "v2":
                continue
            token_map[key]["pools"].append({
                "dex": p.get("dexId", "unknown"),
                "price": self._pf(p.get("priceUsd")),
                "liquidity": liq,
                "volume_24h": vol,
                "pool_address": (p.get("pairAddress") or "").lower(),
                "pool_type": ptype,
                "quote_symbol": qsym,
                "quote_usd": quote_usd,
                "timestamp": p.get("timestamp", time.time()),
                "reserve0": p.get("reserve0", 0),
                "reserve1": p.get("reserve1", 0),
                "token0": p.get("token0", ""),
                "token1": p.get("token1", ""),
            })
            token_map[key]["total_liquidity"] += liq
            token_map[key]["total_volume_24h"] += vol

        active_tokens = []
        for key, info in token_map.items():
            if len(info["pools"]) < MIN_POOLS or len(info["pools"]) > MAX_POOLS:
                print(f"Filtered (pools): {info['symbol']} | pools={len(info['pools'])}")
                continue
            if info["total_liquidity"] < MIN_LIQUIDITY or info["total_liquidity"] > MAX_LIQUIDITY:
                print(f"Filtered (liq): {info['symbol']} | liq={info['total_liquidity']:.0f}")
                continue
            vol_liq = info["total_volume_24h"] / max(1, info["total_liquidity"])
            if vol_liq < MIN_VOLUME_LIQ_RATIO:
                print(f"Filtered (vol/liq): {info['symbol']} | ratio={vol_liq:.2f}")
                continue
            if any(p["price"] <= 0 for p in info["pools"]):
                print(f"Filtered (price): {info['symbol']} | zero price")
                continue

            sym = info["symbol"].upper()
            if sym in STABLECOIN_SYMBOLS or sym in {"WETH", "USDC", "USDT", "SOL", "UNI", "WBTC"}:
                print(f"Filtered (stable/top): {sym}")
                continue

            state = TokenState(
                token_key=key,
                chain=info["chain"],
                contract=info["address"],
                symbol=info["symbol"],
                pools=info["pools"],
                liquidity_total=info["total_liquidity"],
                volume_24h=info["total_volume_24h"]
            )
            await self.oc.fetch_reserves_for_token(state)
            state.compute_spreads_and_profit()
            print(f"Accepted: {state.symbol} | liq={state.liquidity_total:.0f} | spread={state.dex_spread*100:.2f}% | profit=${state.max_real_profit:.2f}")
            active_tokens.append(state)

            state.compute_spreads_and_profit()
            print(f"Accepted: {state.symbol} | liq={state.liquidity_total:.0f} | spread={state.dex_spread*100:.2f}% | profit=${state.max_real_profit:.2f}")
            active_tokens.append(state)


            # Optional CEX price (Binance) – not critical, so we don't filter if missing
            cex = await self.session.get(f"https://api.binance.com/api/v3/ticker/price?symbol={sym}USDT", "cex")
            if cex and "price" in cex:
                state.cex_price = float(cex["price"])

            # Compute profit potential for debugging, but do NOT filter on it
            state.compute_spreads_and_profit()
            print(f"Accepted: {state.symbol} | liq={state.liquidity_total:.0f} | spread={state.dex_spread*100:.2f}% | profit=${state.profit_potential:.2f}")

            active_tokens.append(state)

        print(f"  FINAL accepted pairs (will be observed): {len(active_tokens)}")
        return active_tokens


# =============================================================================
# STATE ENGINE
# =============================================================================

class StateEngine:
    def __init__(self, oc: OnChainValidator, db_path: str = "arbitrage.db"):
        self.oc = oc
        self.tokens: Dict[str, TokenState] = {}
        self.lock = asyncio.Lock()
        self.db_path = db_path

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS token_memory (
                    token_key TEXT PRIMARY KEY,
                    opportunity_count INTEGER,
                    avg_spread REAL,
                    max_spread REAL,
                    avg_duration REAL,
                    spread_std REAL,
                    successful_cycles INTEGER,
                    failed_cycles INTEGER,
                    competition_score INTEGER,
                    behavior_type TEXT,
                    last_seen REAL,
                    last_spike_time REAL,
                    spike_frequency INTEGER,
                    avg_spike_interval REAL,
                    recovery_time_avg REAL,
                    spread_decay_rate REAL,
                    fake_breakouts INTEGER,
                    real_breakouts INTEGER,
                    best_trade_size REAL,
                    profit_per_trade_avg REAL,
                    time_of_day_histogram TEXT,
                    missed_opportunities INTEGER,
                    decay_avg REAL,
                    alpha_token INTEGER
                )""")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS opportunity_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_key TEXT,
                    timestamp REAL,
                    spread REAL,
                    duration REAL,
                    liquidity REAL,
                    profit REAL,
                    trade_size REAL,
                    successful INTEGER
                )""")
            await db.commit()

    async def save_memory(self, mem: TokenMemory):
        async with aiosqlite.connect(self.db_path) as db:
            import json
            hist_json = json.dumps(mem.time_of_day_histogram)
            await db.execute("""
                INSERT OR REPLACE INTO token_memory VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (mem.key, mem.opportunity_count, mem.avg_spread, mem.max_spread,
                  mem.avg_duration, mem.spread_std, mem.successful_cycles,
                  mem.failed_cycles, mem.competition_score, mem.behavior_type,
                  mem.last_seen, mem.last_spike_time, mem.spike_frequency,
                  mem.avg_spike_interval, mem.recovery_time_avg, mem.spread_decay_rate,
                  mem.fake_breakouts, mem.real_breakouts, mem.best_trade_size,
                  mem.profit_per_trade_avg, hist_json, mem.missed_opportunities,
                  mem.decay_avg, 1 if mem.alpha_token else 0))
            await db.commit()

    async def log_opportunity(self, token_key: str, spread: float, duration: float,
                               liquidity: float, profit: float, trade_size: float,
                               successful: bool):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO opportunity_log
                (token_key, timestamp, spread, duration, liquidity, profit, trade_size, successful)
                VALUES (?,?,?,?,?,?,?,?)
            """, (token_key, time.time(), spread, duration, liquidity, profit, trade_size, 1 if successful else 0))
            await db.commit()

    async def update_token(self, token_key: str, chain: str, contract: str, pools: List[Dict], symbol: str = "", cex_price: float = None):
        # Step 1: update the state inside the lock
        async with self.lock:
            if token_key not in self.tokens:
                self.tokens[token_key] = TokenState(token_key, chain, contract, symbol, pools, 0, 0)
            state = self.tokens[token_key]
            state.pools = pools
            state.liquidity_total = sum(p["liquidity"] for p in pools)
            state.volume_24h = sum(p["volume_24h"] for p in pools)
            if cex_price is not None:
                state.cex_price = cex_price
            state.last_update = time.time()
        # Step 2: fetch reserves and recompute profit OUTSIDE the lock
        await self.oc.fetch_reserves_for_token(state)
        state.compute_spreads_and_profit()

    async def get_all_tokens(self) -> List[TokenState]:
        async with self.lock:
            return list(self.tokens.values())


# =============================================================================
# RANKING ENGINE (now allows tokens with minimal history)
# =============================================================================

class RankingEngine:
    def __init__(self, state_engine: StateEngine):
        self.state_engine = state_engine

    async def rank_tokens(self) -> List[TokenState]:
        tokens = await self.state_engine.get_all_tokens()
        scored = []
        for token in tokens:
            if token.blacklisted:
                continue
            mem = token.memory
            # Include tokens with no history but current profit > threshold (bootstrap)
            if mem.opportunity_count < MIN_OPPORTUNITY_COUNT:
                if token.max_real_profit >= MIN_PROFIT_POTENTIAL_USD and token.confidence_score >= MIN_CONFIDENCE_SCORE:
                    scored.append((token.max_real_profit * 0.08 + token.confidence_score * 0.12, token))
                continue
            liq_score = min(1.0, token.liquidity_total / 200000)
            volatility_penalty = min(1.0, mem.spread_std * 10)
            score = (token.max_real_profit * 0.4
                     + token.confidence_score * 0.15
                     + mem.compute_repeatability_score() * 0.2
                     + min(mem.avg_duration / 10, 1) * 0.15
                     + liq_score * 0.15
                     - volatility_penalty * 0.1)
            scored.append((score, token))
        scored.sort(key=lambda x: x[0], reverse=True)
        top_20 = scored[:max(1, len(scored)//5)]
        for _, token in top_20:
            token.memory.alpha_token = True
            await self.state_engine.save_memory(token.memory)
        scored.sort(key=lambda x: x[0], reverse=True)
        top_20 = scored[:max(1, len(scored)//5)]
        for _, token in top_20:
            token.memory.alpha_token = True
            await self.state_engine.save_memory(token.memory)
            # Only require at least 1 opportunity (not 3)
            if token.memory.opportunity_count < MIN_OPPORTUNITY_COUNT:
                continue
            # Profit potential threshold is now only used for ranking, not for filtering
            # We still sort by it, but we don't drop below threshold.
            # However, we keep the threshold as a lower bound for Telegram reports.
            scored.append((token.profit_potential, token))
        scored.sort(key=lambda x: x[0], reverse=True)
        # Return all tokens (not just TOP_N_PAIRS) for observation, but limit ranking output
        return [token for _, token in scored[:TOP_N_PAIRS]]


# =============================================================================
# OBSERVATION ENGINE
# =============================================================================

class ObservationEngine:
    def __init__(self, session: SafeSession, state_engine: StateEngine, tg: TelegramNotifier, oc: OnChainValidator):
        self.session = session
        self.state_engine = state_engine
        self.tg = tg
        self.oc = oc
        self.running = False
        self.active_opps = {}
        self._sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        self.last_loop_time = time.time()
        self.decay_tracker: Dict[str, deque] = defaultdict(lambda: deque(maxlen=5))

    async def start(self):
        self.running = True
        asyncio.create_task(self._poll_loop())

    async def _poll_loop(self):
        while self.running:
            start = time.time()
            tokens = await self.state_engine.get_all_tokens()
            tasks = [self._update_token(t) for t in tokens if not t.blacklisted]
            await asyncio.gather(*tasks, return_exceptions=True)
            elapsed = time.time() - start
            if elapsed > LATENCY_THRESHOLD:
                print(f"⚠️ Loop took {elapsed:.2f}s – we are late")
            await asyncio.sleep(max(0.0, NORMAL_INTERVAL - elapsed))

    async def _update_token(self, token: TokenState):
        async with self._sem:
            raw = await self.session.get(DEXSCREENER_TOKEN_URL.format(token.contract), "dexscreener")
            if not raw:
                return

            old_pool_map = {p["pool_address"]: p for p in token.pools if "pool_address" in p}
        # Optional CEX refresh (non‑critical)
        sym = token.symbol.upper().split()[0].split("/")[0]
        cex_data = await self.session.get(f"https://api.binance.com/api/v3/ticker/price?symbol={sym}USDT", "cex")
        if cex_data and "price" in cex_data:
            token.cex_price = float(cex_data["price"])

            new_pools = []
            for p in raw.get("pairs", []):
                if p.get("chainId") != token.chain:
                    continue
                qt = p.get("quoteToken", {})
                qsym = (qt.get("symbol") or "").upper()
                if qsym not in ALLOWED_QUOTE_SYMBOLS:
                    continue
                quote_usd = 1.0 if qsym in STABLECOIN_SYMBOLS else 0.0
                price = float(p.get("priceUsd", 0))
                price_native = float(p.get("priceNative", 0) or 0)
                if quote_usd == 0.0 and price > 0 and price_native > 0:
                    quote_usd = price / price_native
                if not DiscoveryEngine._is_valid_quote_usd(quote_usd):
                    continue
                liq_raw = p.get("liquidity", {})
                liquidity = float(liq_raw.get("usd", 0)) if isinstance(liq_raw, dict) else float(liq_raw) if liq_raw else 0
                min_pool_liquidity = MIN_POOL_LIQUIDITY * (NON_STABLE_LIQUIDITY_MULTIPLIER if qsym not in STABLECOIN_SYMBOLS else 1.0)
                if liquidity < min_pool_liquidity:
                    continue
                dex = p.get("dexId", "unknown")
                pool_addr = p.get("pairAddress", "").lower()
                pool_type = "v3" if dex in V3_DEX_IDS else "v2"
                if pool_type != "v2":
                    continue
                new_pool = {
                    "dex": dex,
                    "price": price,
                    "liquidity": liquidity,
                    "volume_24h": 0,
                    "pool_address": pool_addr,
                    "pool_type": pool_type,
                    "quote_symbol": qsym,
                    "quote_usd": quote_usd,
                    "timestamp": time.time(),
                    "reserve0": 0,
                    "reserve1": 0,
                    "token0": "",
                    "token1": "",
                }
                if pool_addr in old_pool_map:
                    old = old_pool_map[pool_addr]
                    for key in ["r_base", "r_quote", "token0", "token1", "fee_bps"]:
                        if key in old:
                            new_pool[key] = old[key]
                new_pools.append(new_pool)

            token.update_from_pools(new_pools)
            await self.oc.fetch_reserves_for_token(token)
            token.compute_spreads_and_profit()
            spread = token.dex_spread
            profit = token.max_real_profit
            if spread > 0.001:
                print(f"  OBSERVE {token.symbol} ({token.chain}) | spread={spread*100:.2f}% | profit=${profit:.2f} | conf={token.confidence_score:.2f} | alpha={token.memory.alpha_token}")

            token.memory.last_5_spreads.append(spread)

            # Slow decay of competition score
            token.memory.competition_score = max(0, token.memory.competition_score * 0.99)

            if (spread > MIN_AVG_SPREAD and token.liquidity_total > MIN_LIQUIDITY_TARGET
                and token.confidence_score >= MIN_CONFIDENCE_SCORE):
                if token.key not in self.active_opps:
                    opp = {"start": time.time(), "max_spread": spread}
                    self.active_opps[token.key] = opp
                    token.active_opportunity = opp
                else:
                    opp = self.active_opps[token.key]
                    opp["max_spread"] = max(opp["max_spread"], spread)
            else:
                if token.key in self.active_opps:
                    opp = self.active_opps.pop(token.key)
                    duration = time.time() - opp["start"]
                    if duration < 3.0:
                        token.memory.competition_score = min(20, token.memory.competition_score + 3)
                    was_successful = profit >= MIN_PROFIT_POTENTIAL_USD
                    if was_successful:
                        token.memory.successful_cycles += 1
                    else:
                        token.memory.failed_cycles += 1
                    token.memory.update(opp["max_spread"], duration, trade_usd=0, net_profit=profit)
                    token.memory.real_breakouts += 1
                    await self.state_engine.log_opportunity(token.key, opp["max_spread"], duration,
                                                              token.liquidity_total, profit, token.optimal_trade_size, was_successful)
                    self.decay_tracker[token.key].append(profit)
                    if len(self.decay_tracker[token.key]) == 5:
                        avg_decay = sum(self.decay_tracker[token.key]) / 5
                        if avg_decay < 0.1:
                            token.memory.decay_avg = (token.memory.decay_avg * (token.memory.opportunity_count-1) + avg_decay) / token.memory.opportunity_count
                            if token.memory.decay_avg < OPPORTUNITY_DECAY_THRESHOLD:
                                token.memory.behavior_type = "FAST_DECAY"
                                token.memory.alpha_token = False
                                await self.state_engine.save_memory(token.memory)
                    await self.state_engine.save_memory(token.memory)
                    token.active_opportunity = None

                    if token.memory.failed_cycles > 5 and token.memory.successful_cycles < 2:
                        token.blacklisted = True
                        await self.tg.send(f"💀 *Blacklisted* {token.symbol} ({token.chain})", key=f"blacklist_{token.key}", cooldown=3600)


# =============================================================================
# MICRO-DOMINATION
# =============================================================================

class MicroDomination:
    def __init__(self, state_engine: StateEngine, tg: TelegramNotifier):
        self.state_engine = state_engine
        self.tg = tg
        self.running = False
        self.tasks = {}
        self.burst_threshold = 0.005

    async def start(self):
        self.running = True

    async def watch_token(self, token_key: str):
        last_spread = 0.0
        last_time = time.time()
        last_state_update = 0.0
        while self.running:
            token = self.state_engine.tokens.get(token_key)
            if (not token or token.blacklisted
                or token.max_real_profit < MIN_PROFIT_POTENTIAL_USD
                or token.confidence_score < MIN_CONFIDENCE_SCORE):
                break

            # Skip if state hasn't changed since last check
            if token.last_update == last_state_update:
                await asyncio.sleep(HOT_INTERVAL)
                continue
            last_state_update = token.last_update

            spread = token.dex_spread
            profit = token.max_real_profit

            now = time.time()
            if spread > last_spread + self.burst_threshold and (now - last_time) < BURST_DETECTION_WINDOW:
                print(f"🚀 BURST DETECTED: {token.symbol} spread jumped {spread*100:.2f}% in {(now-last_time):.1f}s")
                if profit >= 2.0 and spread > MIN_AVG_SPREAD and token.confidence_score >= MIN_CONFIDENCE_SCORE:
                    await self.tg.send(f"⚠️ *BURST* {token.symbol} spread {spread*100:.1f}%", key=f"burst_{token.key}", cooldown=30)
            last_spread = spread
            last_time = now

            if (profit >= 2.0 and spread >= MIN_AVG_SPREAD and token.liquidity_total > 50000 and
                token.confidence_score >= MIN_CONFIDENCE_SCORE and
                token.active_opportunity and (time.time() - token.active_opportunity["start"]) > 2.0):
                msg = (f"⚡ *ARBITRAGE OPPORTUNITY* ({token.chain})\n"
                       f"{token.symbol}\n"
                       f"Spread: {spread*100:.2f}%\n"
                       f"Profit: ${profit:.2f}\n"
                       f"Size: ${token.optimal_trade_size:.0f}\n"
                       f"Liq: ${token.liquidity_total:,.0f}")
                await self.tg.send(msg, key=token.key, cooldown=60)
                await self.state_engine.log_opportunity(token.key, spread,
                                                          time.time() - token.active_opportunity["start"],
                                                          token.liquidity_total, profit, token.optimal_trade_size,
                                                          profit >= MIN_PROFIT_POTENTIAL_USD)

            if token.memory.alpha_token:
                await asyncio.sleep(HOT_INTERVAL)
            else:
                await asyncio.sleep(NORMAL_INTERVAL)

    async def update_targets(self, top_tokens: List[TokenState]):
        top_keys = {t.key for t in top_tokens}
        # Do not filter again; we keep the same list as ranking output
        for key in list(self.tasks.keys()):
            task = self.tasks[key]
            if key not in top_keys or task.done():
                task.cancel()
                del self.tasks[key]
        for token in top_tokens:
            if token.key not in self.tasks:
                self.tasks[token.key] = asyncio.create_task(self.watch_token(token.key))


# =============================================================================
# MAIN SCANNER
# =============================================================================

class ArbitrageScanner:
    def __init__(self):
        self.session = SafeSession()
        self.oc = OnChainValidator()
        self.state_engine = StateEngine(self.oc)
        self.tg = None
        self.obs = None
        self.micro = None
        self.ranking = None
        self.running = False

    async def start(self):
        self.running = True
        await self.session.start()
        self.tg = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, self.session)
        await self.state_engine.init_db()

        discovery = DiscoveryEngine(self.session, self.oc)
        tokens = await discovery.discover_tokens()
        for t in tokens:
            await self.state_engine.update_token(t.key, t.chain, t.contract, t.pools, t.symbol)

        sorted_tokens = sorted(tokens, key=lambda x: x.max_real_profit, reverse=True)
        print(f"\n📋 DISCOVERED — {len(sorted_tokens)} tokens will be observed")
        for i, t in enumerate(sorted_tokens[:15], 1):
            print(f"{i:2d}. {t.symbol:12} ({t.chain}) | liq=${t.liquidity_total:,.0f} | "
                  f"spread={t.dex_spread*100:.2f}% | profit=${t.max_real_profit:.2f}")
        for i, t in enumerate(sorted_tokens[:15], 1):
            print(f"{i:2d}. {t.symbol:12} ({t.chain}) | liq=${t.liquidity_total:,.0f} | "
                  f"spread={t.dex_spread*100:.2f}% | profit=${t.max_real_profit:.2f}")
        sorted_tokens = sorted(tokens, key=lambda x: x.profit_potential, reverse=True)
        print(f"\n📋 DISCOVERED — {len(sorted_tokens)} tokens will be observed")
        for i, t in enumerate(sorted_tokens[:15], 1):
            print(f"{i:2d}. {t.symbol:12} ({t.chain}) | liq=${t.liquidity_total:,.0f} | "
                  f"spread={t.dex_spread*100:.2f}% | profit_pot=${t.profit_potential:.1f}")
        await self.tg.send(f"Discovered {len(tokens)} tokens (observation started)", key="discovery", cooldown=3600)

        self.obs = ObservationEngine(self.session, self.state_engine, self.tg, self.oc)
        await self.obs.start()
        print("👁️  Observation started.")

        self.micro = MicroDomination(self.state_engine, self.tg)
        await self.micro.start()
        print("🚀 Micro‑domination ready.")

        self.ranking = RankingEngine(self.state_engine)
        asyncio.create_task(self._ranking_loop())
        asyncio.create_task(self._heartbeat())
        asyncio.create_task(self._rediscovery_loop())

    async def _rediscovery_loop(self, interval=600):
        while self.running:
            await asyncio.sleep(interval)
            print("\n🔄 Re-discovering new tokens...")
            discovery = DiscoveryEngine(self.session, self.oc)
            new_tokens = await discovery.discover_tokens()
            added = 0
            for token in new_tokens:
                if token.key not in self.state_engine.tokens:
                    await self.state_engine.update_token(token.key, token.chain, token.contract,
                                                          token.pools, token.symbol)
                    added += 1
            if added:
                print(f"✅ Added {added} new tokens.")
                await self.tg.send(f"Rediscovered {added} new tokens", key="rediscovery", cooldown=3600)

    async def _ranking_loop(self, interval=30):
        while self.running:
            top = await self.ranking.rank_tokens()
            if top:
                print(f"\n🏆 TOP {len(top)} PAIRS BY PROFIT POTENTIAL:")
                for i, t in enumerate(top, 1):
                    print(f"{i:2d}. {t.symbol:12} ({t.chain}) | profit=${t.max_real_profit:.1f} | spread={t.dex_spread*100:.2f}% | conf={t.confidence_score:.2f} | liq=${t.liquidity_total:,.0f}")
                await self.micro.update_targets(top)
                top_text = "🏆 *TOP PROFIT POTENTIAL*\n"
                for t in top[:5]:
                    top_text += f"{t.symbol} ({t.chain}) | ${t.max_real_profit:.1f} | {t.dex_spread*100:.2f}% | C:{t.confidence_score:.2f}\n"
                await self.tg.send(top_text, key="top_pairs", cooldown=300)
                summary = f"📊 *SUMMARY*\nPairs tracked: {len(await self.state_engine.get_all_tokens())}\nActive watchers: {len(self.micro.tasks)}"
                await self.tg.send(summary, key="summary", cooldown=600)
            else:
                print("⚠️  No tokens with sufficient history yet.")
            await asyncio.sleep(interval)

    async def _heartbeat(self, interval=15):
        while self.running:
            total = len(await self.state_engine.get_all_tokens())
            watchers = len(self.micro.tasks)
            print(f"\n💓 {time.strftime('%H:%M:%S')} | Watching: {total} | Watchers: {watchers}")
            await asyncio.sleep(interval)

    async def stop(self):
        self.running = False
        await self.session.stop()


# =============================================================================
# ENTRY POINT
# =============================================================================

async def main():
    while True:
        scanner = ArbitrageScanner()
        try:
            await scanner.start()
            await asyncio.Event().wait()
        except Exception as e:
            print(f"💥 Scanner crashed: {e} — restarting in 30s")
            await scanner.stop()
            await asyncio.sleep(30)
    scanner = ArbitrageScanner()
    try:
        await scanner.start()
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await scanner.stop()

if __name__ == "__main__":
    asyncio.run(main())
