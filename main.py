# !pip install nest_asyncio aiosqlite web3 aiohttp eth-abi -q

import os
import asyncio
import aiohttp
import time
import math
import random
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import logging
import nest_asyncio
import aiosqlite
from web3 import AsyncWeb3, AsyncHTTPProvider

nest_asyncio.apply()
logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s')

# =============================================================================
# CONFIGURATION
# =============================================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TARGET_CHAINS = {"base", "polygon", "bsc"}  # low competition

EVM_CHAINS = {
    "base":     {"rpc": "https://mainnet.base.org",         "gecko": "base"},
    "polygon":  {"rpc": "https://polygon-rpc.com",          "gecko": "polygon_pos"},
    "bsc":      {"rpc": "https://bsc-dataseed.binance.org", "gecko": "bsc"},
}
GECKO_TO_CHAIN = {v["gecko"]: k for k, v in EVM_CHAINS.items()}
GECKO_TO_CHAIN["polygon_pos"] = "polygon"

MULTICALL3_ADDR = "0xcA11bde05977b3631167028862bE2a173976CA11"

V3_DEX_IDS = {
    "uniswap-v3", "pancakeswap-v3", "camelot-v3", "sushiswap-v3",
    "aerodrome-cl", "velodrome-v2", "quickswap-v3", "zyberswap",
    "ramses-v2", "thena-fusion", "algebra",
}

STABLECOIN_SYMBOLS = {"USDT", "USDC", "DAI"}

MIN_POOL_LIQUIDITY    = 20_000
MIN_GROUP_LIQUIDITY   = 50_000
MIN_LIQUIDITY_TARGET  = 200_000
MAX_SPREAD            = 0.05
MIN_SPREAD            = 0.001
MAX_STABLE_SPREAD     = 0.01
FAKE_SPREAD_THRESHOLD = 0.10
MAX_PRICE_IMPACT      = 0.01
FLASHLOAN_FEE_BPS     = 9

OBSERVE_INTERVAL      = 0.8        # faster polling
SPREAD_HISTORY_LEN    = 60
MIN_OPPORTUNITY_COUNT = 1
TOP_N_PAIRS           = 50
RATE_LIMIT_SLEEP      = 0.2
CONCURRENCY_LIMIT     = 20          # RPC semaphore

TOP_TOKENS = {"WETH", "USDC", "USDT", "WBTC", "ETH", "BNB"}

GAS_COST_USD = {"base": 0.5, "polygon": 0.5, "bsc": 0.3}
CHAIN_LATENCY = {"base": (0.3, 0.7), "polygon": (0.4, 0.8), "bsc": (0.4, 0.9)}
CHAIN_PRIORITY = {"base": 0.9, "polygon": 0.7, "bsc": 0.6}

PRE_SIGNAL_WINDOW = 5

SAFE_DEXES = {
    "uniswap", "sushiswap", "pancakeswap",
    "aerodrome", "quickswap"
}

DEXSCREENER_TOKEN_URL  = "https://api.dexscreener.com/latest/dex/tokens/{}"
DEXSCREENER_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search?q={}"
GECKO_BASE             = "https://api.geckoterminal.com/api/v2"

DEXSCREENER_QUERIES = ["trending", "hot", "new", "usdc", "usdt", "weth", "base", "polygon", "bnb"]

TOKEN_PRICE_CACHE: Dict[str, float] = {}

# =============================================================================
# TELEGRAM NOTIFIER (fixed formatting)
# =============================================================================

class TelegramNotifier:
    def __init__(self, token, chat_id, session):
        self.token = token
        self.chat_id = chat_id
        self.session = session
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.last_sent = {}

    async def send(self, text, key=None, cooldown=30):
        now = time.time()
        if key:
            if key in self.last_sent and now - self.last_sent[key] < cooldown:
                return
            self.last_sent[key] = now
        # Use session.post for reliability
        await self.session.post(self.url, {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown"
        })

# =============================================================================
# SAFE HTTP SESSION (with post method)
# =============================================================================

class SafeSession:
    def __init__(self):
        self._session = None
        self._last_req = defaultdict(float)

    async def start(self):
        conn = aiohttp.TCPConnector(limit=200, ttl_dns_cache=300)
        self._session = aiohttp.ClientSession(connector=conn)

    async def stop(self):
        if self._session:
            await self._session.close()

    async def get(self, url, domain="default"):
        if not self._session:
            return None
        gap = time.time() - self._last_req[domain]
        if gap < RATE_LIMIT_SLEEP:
            await asyncio.sleep(RATE_LIMIT_SLEEP - gap)
        self._last_req[domain] = time.time()
        try:
            async with self._session.get(url, timeout=8) as r:
                return await r.json() if r.status == 200 else None
        except Exception:
            return None

    async def post(self, url, json_data):
        if not self._session:
            return None
        try:
            async with self._session.post(url, json=json_data, timeout=8) as r:
                return await r.json() if r.status == 200 else None
        except Exception:
            return None

# =============================================================================
# DATA MODELS (unchanged)
# =============================================================================

@dataclass
class Pool:
    dex:           str
    pool_address:  str
    price_usd:     float
    liquidity:     float
    volume_24h:    float
    pool_type:     str = "v2"
    reserve0:      int = 0
    reserve1:      int = 0
    token0_is_base:bool = True
    verified:      bool = False
    base_dec:      int = 18
    quote_dec:     int = 18

@dataclass
class PairGroup:
    chain:         str
    base_address:  str
    quote_address: str
    base_symbol:   str
    quote_symbol:  str
    pools:         List[Pool] = field(default_factory=list)
    spread:        float = 0.0
    buy_pool:      Optional[Pool] = None
    sell_pool:     Optional[Pool] = None
    blacklisted:   bool = False
    last_seen:     float = field(default_factory=time.time)

    @property
    def key(self) -> str: return f"{self.chain}:{self.base_address}:{self.quote_address}"
    @property
    def total_liquidity(self) -> float: return sum(p.liquidity for p in self.pools)
    @property
    def symbol_pair(self) -> str: return f"{self.base_symbol}/{self.quote_symbol}"

    def compute_spread(self):
        valid = [p for p in self.pools if p.price_usd > 0]
        if len(valid) < 2: self.spread = 0.0; return
        prices = sorted(valid, key=lambda p: p.price_usd)
        lo, hi = prices[0].price_usd, prices[-1].price_usd
        self.buy_pool, self.sell_pool = prices[0], prices[-1]
        self.spread = (hi - lo) / lo if lo > 0 else 0.0

@dataclass
class PairMemory:
    key: str
    opportunity_count: int = 0
    avg_spread: float = 0.0
    max_spread: float = 0.0
    avg_duration: float = 0.0
    spread_std: float = 0.0
    successful_cycles: int = 0
    failed_cycles: int = 0
    competition_score: int = 0
    behavior_type: str = "NEW"
    last_seen: float = 0.0
    last_spike_time: float = 0.0
    spike_frequency: int = 0
    avg_spike_interval: float = 0.0
    recovery_time_avg: float = 0.0
    spread_decay_rate: float = 0.0
    time_of_day_histogram: Dict[int, int] = field(default_factory=dict)
    fake_breakouts: int = 0
    real_breakouts: int = 0
    best_trade_size: float = 0.0
    profit_per_trade_avg: float = 0.0
    last_5_spreads: deque = field(default_factory=lambda: deque(maxlen=5))
    last_trade_time: float = 0.0
    _prev_spread: float = 0.0
    last_liquidity: float = 0.0
    last_update_time: float = 0.0
    missed_opportunities: int = 0

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
        self._prev_spread = spread
        hour = int(time.time()//3600%24)
        self.time_of_day_histogram[hour] = self.time_of_day_histogram.get(hour,0)+1
        if trade_usd:
            self.best_trade_size = self.best_trade_size*0.7 + trade_usd*0.3 if self.best_trade_size else trade_usd
        if net_profit:
            self.profit_per_trade_avg = self.profit_per_trade_avg*0.7 + net_profit*0.3 if self.profit_per_trade_avg else net_profit
        self.last_5_spreads.append(spread)
        self.last_update_time = time.time()

    def classify_behavior(self):
        if self.opportunity_count < 3: self.behavior_type = "NEW"; return
        if self.spike_frequency>3 and self.spread_decay_rate<0.0005 and self.avg_duration>1.0: self.behavior_type = "SNIPABLE"
        elif self.spike_frequency>3 and self.avg_duration<0.5 and self.fake_breakouts>self.real_breakouts: self.behavior_type = "MEV_TRAP"
        elif self.avg_spread<0.003 and self.spread_std<0.001 and self.avg_duration>3.0: self.behavior_type = "MARKET_MAKER_DRIFT"
        elif self.spread_std>0.01: self.behavior_type = "CHAOTIC"
        elif self.opportunity_count>5 and max(self.time_of_day_histogram.values())/self.opportunity_count>0.3: self.behavior_type = "SCHEDULED"
        else: self.behavior_type = "NORMAL"

    def compute_repeatability_score(self):
        if self.opportunity_count < MIN_OPPORTUNITY_COUNT: return 0.0
        consistency = 1/(1+self.spread_std)
        success_rate = self.successful_cycles / max(1,self.successful_cycles+self.failed_cycles)
        hour = int(time.time()//3600%24)
        time_boost = self.time_of_day_histogram.get(hour,0)/max(1,self.opportunity_count)
        fake_penalty = self.fake_breakouts/(self.real_breakouts+1) if self.real_breakouts else 0
        decay = max(0.5, 1 - (time.time()-self.last_seen)/3600)
        score = (self.avg_spread*0.3 + min(self.opportunity_count/50,1)*0.2 + min(self.avg_duration/10,1)*0.15 + consistency*0.15 + time_boost*0.1 + success_rate*0.1 - fake_penalty*0.1 - self.competition_score*0.05)*decay
        return max(0.0, min(1.0, score))

    def dynamic_threshold(self): return max(0.002, self.avg_spread*0.7, self.spread_std*1.5)

    def should_skip(self):
        if self.competition_score > 20: return True
        if self.fake_breakouts > self.real_breakouts: return True
        if self.spread_std > 0.02: return True
        if self.recovery_time_avg < 0.5 and self.opportunity_count > 3: return True
        if self.avg_spike_interval and self.avg_spike_interval < 2: return True
        if self.opportunity_count < 3: return True
        if self.failed_cycles > 5 and self.successful_cycles == 0: return True
        return False

    def detect_pre_signal(self, current_spread, current_liquidity):
        if len(self.last_5_spreads) < PRE_SIGNAL_WINDOW: return False
        spreads = list(self.last_5_spreads)
        trend = sum(spreads[i+1]-spreads[i] for i in range(len(spreads)-1))
        volatility = max(spreads) - min(spreads)
        liq_change = current_liquidity - self.last_liquidity
        self.last_liquidity = current_liquidity
        momentum = spreads[-1] > spreads[-2]*1.1
        return (trend > 0 and volatility < 0.0015 and liq_change > 0 and momentum)

# =============================================================================
# ON-CHAIN VALIDATOR (unchanged)
# =============================================================================

MULTICALL3_ABI = [{"inputs":[{"components":[{"name":"target","type":"address"},{"name":"callData","type":"bytes"}],"name":"calls","type":"tuple[]"}],"name":"aggregate","outputs":[{"name":"blockNumber","type":"uint256"},{"name":"returnData","type":"bytes[]"}],"stateMutability":"payable","type":"function"}]
V2_RESERVES_ABI = [{"inputs":[],"name":"getReserves","outputs":[{"name":"_reserve0","type":"uint112"},{"name":"_reserve1","type":"uint112"},{"name":"_blockTimestampLast","type":"uint32"}],"stateMutability":"view","type":"function"}]
V2_TOKEN0_ABI = [{"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"}]
ERC20_DECIMALS_ABI = [{"inputs":[],"name":"decimals","outputs":[{"type":"uint8"}],"stateMutability":"view","type":"function"}]

class OnChainValidator:
    def __init__(self):
        self._w3 = {}
        self._mc = {}
        self._decimals_cache = {}
        self._sem = asyncio.Semaphore(CONCURRENCY_LIMIT)

    def _setup(self, chain):
        if chain in self._w3: return
        rpc = EVM_CHAINS[chain]["rpc"]
        w3 = AsyncWeb3(AsyncHTTPProvider(rpc, request_kwargs={"timeout":5}))
        self._w3[chain] = w3
        self._mc[chain] = w3.eth.contract(address=AsyncWeb3.to_checksum_address(MULTICALL3_ADDR), abi=MULTICALL3_ABI)

    async def _batch_call(self, chain, pools, fn_name, abi, decode_types):
        async with self._sem:
            self._setup(chain)
            w3, mc = self._w3[chain], self._mc[chain]
            if not w3 or not mc: return {}
            calls, valid = [], []
            for p in pools:
                if not p.pool_address: continue
                try:
                    c = w3.eth.contract(address=AsyncWeb3.to_checksum_address(p.pool_address), abi=abi)
                    calls.append((AsyncWeb3.to_checksum_address(p.pool_address), c.encode_abi(fn_name)))
                    valid.append(p)
                except: continue
            if not calls: return {}
            from eth_abi import decode as abi_decode
            results = {}
            BATCH=25
            for i in range(0,len(calls),BATCH):
                b_calls, b_pools = calls[i:i+BATCH], valid[i:i+BATCH]
                try:
                    _, rd = await mc.functions.aggregate(b_calls).call()
                    for j,data in enumerate(rd):
                        try: results[b_pools[j].pool_address] = abi_decode(decode_types, data)
                        except: continue
                except:
                    for j,(target,_) in enumerate(b_calls):
                        try:
                            c = w3.eth.contract(address=target, abi=abi)
                            val = await getattr(c.functions, fn_name)().call()
                            results[b_pools[j].pool_address] = val if isinstance(val,tuple) else (val,)
                        except: continue
            return results

    async def _get_decimals(self, chain, token_addr):
        key = f"{chain}:{token_addr}"
        if key in self._decimals_cache: return self._decimals_cache[key]
        self._setup(chain)
        w3 = self._w3[chain]
        if not w3: return 18
        try:
            contract = w3.eth.contract(address=AsyncWeb3.to_checksum_address(token_addr), abi=ERC20_DECIMALS_ABI)
            dec = await contract.functions.decimals().call()
            self._decimals_cache[key] = dec
            return dec
        except: return 18

    async def get_reserves_for_group(self, group):
        v2_pools = [p for p in group.pools if p.pool_type == "v2"]
        if not v2_pools: return False
        chain = group.chain
        t0_map = await self._batch_call(chain, v2_pools, "token0", V2_TOKEN0_ABI, ["address"])
        res_map = await self._batch_call(chain, v2_pools, "getReserves", V2_RESERVES_ABI, ["uint112","uint112","uint32"])
        if not res_map: return False
        base_dec = await self._get_decimals(chain, group.base_address)
        quote_dec = await self._get_decimals(chain, group.quote_address)
        valid = 0
        for p in v2_pools:
            res = res_map.get(p.pool_address)
            if not res: continue
            r0,r1 = int(res[0]), int(res[1])
            if r0<1000 or r1<1000: continue
            t0 = t0_map.get(p.pool_address)
            if t0: p.token0_is_base = (t0[0].lower() == group.base_address.lower()) if isinstance(t0,tuple) else (str(t0).lower() == group.base_address.lower())
            p.reserve0, p.reserve1 = r0, r1
            p.verified = True
            p.base_dec, p.quote_dec = base_dec, quote_dec
            if p.token0_is_base: price = (r1/10**quote_dec) / (r0/10**base_dec) if r0 else 0
            else: price = (r0/10**quote_dec) / (r1/10**base_dec) if r1 else 0
            p.price_usd = price
            valid += 1
        return valid >= 1

    async def validate_group(self, group): return await self.get_reserves_for_group(group)

# =============================================================================
# PROFIT SIMULATOR (unchanged)
# =============================================================================

class ProfitSimulator:
    @staticmethod
    def simulate_v2_swap(amount_in, r_in, r_out, fee_bps=30):
        if r_in == 0 or r_out == 0: return 0.0
        k = 10_000 - fee_bps
        amount_in_with_fee = amount_in * k
        num = amount_in_with_fee * r_out
        den = r_in * 10_000 + amount_in_with_fee
        return num / den if den > 0 else 0.0

    @classmethod
    def compute_profit(cls, group, trade_usd):
        bp, sp = group.buy_pool, group.sell_pool
        if not bp or not sp or not bp.verified or not sp.verified: return None

        if abs(bp.price_usd - sp.price_usd) / bp.price_usd > 0.05:
            return None

        quote_price = 1.0
        amount_quote = trade_usd / quote_price

        if bp.token0_is_base:
            r_quote_in = bp.reserve1 / (10**bp.quote_dec)
            r_base_out = bp.reserve0 / (10**bp.base_dec)
        else:
            r_quote_in = bp.reserve0 / (10**bp.quote_dec)
            r_base_out = bp.reserve1 / (10**bp.base_dec)

        base_received = cls.simulate_v2_swap(amount_quote, r_quote_in, r_base_out)
        if base_received <= 0: return None

        if sp.token0_is_base:
            r_base_in = sp.reserve0 / (10**sp.base_dec)
            r_quote_out = sp.reserve1 / (10**sp.quote_dec)
        else:
            r_base_in = sp.reserve1 / (10**sp.base_dec)
            r_quote_out = sp.reserve0 / (10**sp.quote_dec)

        quote_received = cls.simulate_v2_swap(base_received, r_base_in, r_quote_out)
        if quote_received <= 0: return None

        proceeds_usd = quote_received * 1.0

        gross = proceeds_usd - trade_usd
        flash_fee = trade_usd * FLASHLOAN_FEE_BPS / 10000
        gas_cost = GAS_COST_USD.get(group.chain, 0.5) * random.uniform(0.8, 1.5)
        net_profit = (gross - gas_cost - flash_fee) * 0.98

        if net_profit <= 0: return None

        effective_price = trade_usd / base_received if base_received else 0
        slippage = abs(effective_price - bp.price_usd) / bp.price_usd if bp.price_usd else 0
        if slippage > 0.01: return None

        impact = trade_usd / group.total_liquidity
        if impact > 0.01: return None

        return {"gross": gross, "net_profit": net_profit, "flash_fee": flash_fee,
                "trade_usd": trade_usd, "on_chain": True, "slippage": slippage, "impact": impact}

    @classmethod
    def find_optimal_trade(cls, group, memory=None):
        sizes = [200,500,1000,2000,5000,10000]
        if memory and memory.best_trade_size: sizes.insert(0, memory.best_trade_size)
        best = None
        best_profit = 0
        for s in sizes:
            p = cls.compute_profit(group, s)
            if p and p["net_profit"] > best_profit:
                best_profit = p["net_profit"]
                best = (s, p)
        return best

# =============================================================================
# VALIDATION PIPELINE
# =============================================================================

class ValidationPipeline:
    def __init__(self, oc, sim, memory_store):
        self.oc = oc
        self.sim = sim
        self.memory = memory_store
        self.blacklist = set()

    async def run(self, group, mem, latency=0.0):
        if group.key in self.blacklist: return None
        if group.quote_symbol not in STABLECOIN_SYMBOLS: return None
        if group.base_symbol not in TOP_TOKENS: return None
        if group.total_liquidity < MIN_LIQUIDITY_TARGET: return None
        if min(p.liquidity for p in group.pools) < 50000: return None
        if group.buy_pool and group.buy_pool.dex not in SAFE_DEXES: return None
        if group.sell_pool and group.sell_pool.dex not in SAFE_DEXES: return None

        if not await self.oc.validate_group(group): return None

        group.compute_spread()
        if group.spread <= MIN_SPREAD or group.spread > MAX_SPREAD: return None

        if group.spread < mem.dynamic_threshold(): return None
        if mem.should_skip(): return None

        if group.buy_pool and group.sell_pool:
            raw_slippage = abs(group.sell_pool.price_usd - group.buy_pool.price_usd) / group.buy_pool.price_usd
            if raw_slippage > 0.02: return None

        opt = self.sim.find_optimal_trade(group, mem)
        if not opt: return None
        trade_usd, profit = opt

        if profit["net_profit"] < 3.0: return None

        failure_risk = mem.competition_score * 0.02 + mem.spread_std * 5 + (1 if mem.behavior_type == "MEV_TRAP" else 0.5)
        success_prob = max(0.0, 1.0 - failure_risk)
        success_prob *= (1 - min(latency/1.0, 0.5))
        expected_profit = profit["net_profit"] * success_prob
        if expected_profit < 2.0: return None

        hour = int(time.time() // 3600 % 24)
        time_score = mem.time_of_day_histogram.get(hour,0)/max(1,mem.opportunity_count)
        age = time.time() - mem.last_spike_time if mem.last_spike_time else 30
        freshness = max(0.0, 1 - age/30)
        confidence = (group.spread*0.3 + (1/(1+mem.spread_std))*0.2 + min(mem.opportunity_count/50,1)*0.2 + time_score*0.2 + (1 - mem.fake_breakouts/(mem.real_breakouts+1))*0.1)
        confidence *= CHAIN_PRIORITY.get(group.chain,0.5) * (0.7+0.3*freshness)
        miss_penalty = mem.missed_opportunities / max(1, mem.opportunity_count)
        confidence *= (1 - min(miss_penalty, 0.5))
        if confidence < 0.3: return None

        tier = "HIGH" if confidence>0.7 else "MEDIUM" if confidence>0.5 else "LOW"
        if confidence>0.7: size = min(trade_usd*1.5,10000)
        elif confidence>0.5: size = trade_usd
        else: size = trade_usd*0.5
        size = max(200, min(size,10000))

        profit2 = self.sim.compute_profit(group, size)
        if not profit2: return None

        return {"key": group.key, "chain": group.chain, "pair": group.symbol_pair,
                "spread": group.spread, "spread_pct": group.spread*100,
                "liquidity": group.total_liquidity, "buy_dex": group.buy_pool.dex if group.buy_pool else "",
                "sell_dex": group.sell_pool.dex if group.sell_pool else "",
                "buy_price": group.buy_pool.price_usd if group.buy_pool else 0,
                "sell_price": group.sell_pool.price_usd if group.sell_pool else 0,
                "trade_usd": profit2["trade_usd"], "net_profit": profit2["net_profit"],
                "expected_profit": expected_profit, "success_prob": success_prob,
                "confidence": confidence, "tier": tier, "pools": len(group.pools),
                "timestamp": time.time()}

# =============================================================================
# PAIR MEMORY STORE (SQLite)
# =============================================================================

class PairMemoryStore:
    def __init__(self, db_path="arb_pairs.db"):
        self.db = db_path
        self.cache = {}

    async def init(self):
        async with aiosqlite.connect(self.db) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS pair_memory (
                    key TEXT PRIMARY KEY,
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
                    missed_opportunities INTEGER DEFAULT 0
                )""")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS opportunity_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT, timestamp REAL, spread REAL,
                    duration REAL, liquidity REAL,
                    net_profit REAL, buy_dex TEXT, sell_dex TEXT
                )""")
            await db.commit()

    def get(self, key):
        if key not in self.cache: self.cache[key] = PairMemory(key=key)
        return self.cache[key]

    async def save(self, mem):
        self.cache[mem.key] = mem
        async with aiosqlite.connect(self.db) as db:
            import json
            hist_json = json.dumps(mem.time_of_day_histogram)
            await db.execute("""
                INSERT OR REPLACE INTO pair_memory VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (mem.key, mem.opportunity_count, mem.avg_spread, mem.max_spread,
                  mem.avg_duration, mem.spread_std, mem.successful_cycles,
                  mem.failed_cycles, mem.competition_score, mem.behavior_type,
                  mem.last_seen, mem.last_spike_time, mem.spike_frequency,
                  mem.avg_spike_interval, mem.recovery_time_avg, mem.spread_decay_rate,
                  mem.fake_breakouts, mem.real_breakouts, mem.best_trade_size,
                  mem.profit_per_trade_avg, hist_json, mem.missed_opportunities))
            await db.commit()

    async def log_opp(self, opp):
        async with aiosqlite.connect(self.db) as db:
            await db.execute("""
                INSERT INTO opportunity_log
                (key, timestamp, spread, duration, liquidity,
                 net_profit, buy_dex, sell_dex)
                VALUES (?,?,?,?,?,?,?,?)
            """, (opp["key"], opp["timestamp"], opp["spread"],
                  opp.get("duration",0), opp["liquidity"],
                  opp["net_profit"], opp["buy_dex"], opp["sell_dex"]))
            await db.commit()

    async def load_all(self):
        async with aiosqlite.connect(self.db) as db:
            async with db.execute("SELECT * FROM pair_memory") as cur:
                for row in await cur.fetchall():
                    key = row[0]
                    m = PairMemory(key=key)
                    (m.opportunity_count, m.avg_spread, m.max_spread,
                     m.avg_duration, m.spread_std, m.successful_cycles,
                     m.failed_cycles, m.competition_score, m.behavior_type,
                     m.last_seen, m.last_spike_time, m.spike_frequency,
                     m.avg_spike_interval, m.recovery_time_avg, m.spread_decay_rate,
                     m.fake_breakouts, m.real_breakouts, m.best_trade_size,
                     m.profit_per_trade_avg, hist_json, m.missed_opportunities) = row[1:]
                    import json
                    m.time_of_day_histogram = json.loads(hist_json)
                    self.cache[key] = m

# =============================================================================
# DISCOVERY ENGINE (unchanged)
# =============================================================================

class DiscoveryEngine:
    def __init__(self, session):
        self.session = session

    @staticmethod
    def _pf(val, default=0.0):
        try: return float(val or default)
        except: return default

    async def _dex_search(self, q):
        d = await self.session.get(DEXSCREENER_SEARCH_URL.format(q), "dexscreener")
        return (d or {}).get("pairs", [])

    async def _gecko_pools(self, gnet, page=1):
        url = f"{GECKO_BASE}/networks/{gnet}/pools?page={page}&include=base_token,quote_token,dex"
        data = await self.session.get(url, "gecko")
        return self._parse_gecko(data or {}, gnet)

    async def _gecko_trending(self, gnet):
        url = f"{GECKO_BASE}/networks/{gnet}/trending_pools?include=base_token,quote_token,dex"
        data = await self.session.get(url, "gecko")
        return self._parse_gecko(data or {}, gnet)

    def _parse_gecko(self, data, gnet):
        chain = GECKO_TO_CHAIN.get(gnet)
        if not chain or chain not in TARGET_CHAINS: return []
        tok_map = {}
        for item in data.get("included", []):
            if item.get("type") == "token":
                tid = item["id"]
                attrs = item.get("attributes", {})
                addr = attrs.get("address","")
                if "_" in addr and not addr.startswith("0x"): addr = addr.split("_",1)[1]
                tok_map[tid] = {"address": addr.lower(), "symbol": attrs.get("symbol","")}
        results = []
        for p in data.get("data", []):
            attrs = p.get("attributes", {})
            rels = p.get("relationships", {})
            base_id = rels.get("base_token", {}).get("data", {}).get("id","")
            quot_id = rels.get("quote_token", {}).get("data", {}).get("id","")
            dex_id = rels.get("dex", {}).get("data", {}).get("id","gecko")
            bt = tok_map.get(base_id, {})
            qt = tok_map.get(quot_id, {})
            if not bt.get("address") or not qt.get("address"): continue
            if qt.get("symbol") not in STABLECOIN_SYMBOLS: continue
            results.append({
                "chainId": chain,
                "baseToken": {"address": bt["address"], "symbol": bt.get("symbol","")},
                "quoteToken": {"address": qt["address"], "symbol": qt.get("symbol","")},
                "priceUsd": self._pf(attrs.get("base_token_price_usd")),
                "liquidity": {"usd": self._pf(attrs.get("reserve_in_usd"))},
                "volume": {"h24": self._pf((attrs.get("volume_usd") or {}).get("h24"))},
                "dexId": dex_id,
                "pairAddress": attrs.get("address",""),
            })
        return results

    async def discover_pairs(self):
        print("\n🔍 Phase 1: Discovering pairs...")
        tasks = [self._dex_search(q) for q in DEXSCREENER_QUERIES]
        for cfg in EVM_CHAINS.values():
            gnet = cfg["gecko"]
            tasks += [self._gecko_pools(gnet, 1), self._gecko_pools(gnet, 2), self._gecko_trending(gnet)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_pairs = []
        for r in results:
            if isinstance(r, list): all_pairs.extend(r)
        print(f"  Raw pairs: {len(all_pairs)}")
        groups = {}
        for p in all_pairs:
            if not isinstance(p, dict): continue
            chain = p.get("chainId","")
            if chain not in TARGET_CHAINS: continue
            bt = p.get("baseToken", {})
            qt = p.get("quoteToken", {})
            baddr = (bt.get("address") or "").lower().strip()
            qaddr = (qt.get("address") or "").lower().strip()
            if "_" in baddr and not baddr.startswith("0x"): baddr = baddr.split("_",1)[1]
            if "_" in qaddr and not qaddr.startswith("0x"): qaddr = qaddr.split("_",1)[1]
            if not baddr or not qaddr or baddr == "0x0000000000000000000000000000000000000000": continue
            liq_raw = p.get("liquidity", {})
            liq = self._pf(liq_raw.get("usd") if isinstance(liq_raw,dict) else liq_raw)
            vol_raw = p.get("volume", {})
            vol = self._pf(vol_raw.get("h24") if isinstance(vol_raw,dict) else vol_raw)
            if liq < MIN_POOL_LIQUIDITY: continue
            key = f"{chain}:{baddr}:{qaddr}"
            if key not in groups:
                groups[key] = PairGroup(chain=chain, base_address=baddr, quote_address=qaddr,
                                        base_symbol=bt.get("symbol",""), quote_symbol=qt.get("symbol",""))
            ptype = "v3" if p.get("dexId","") in V3_DEX_IDS else "v2"
            groups[key].pools.append(Pool(dex=p.get("dexId","unknown"),
                                          pool_address=(p.get("pairAddress") or "").lower(),
                                          price_usd=self._pf(p.get("priceUsd")),
                                          liquidity=liq, volume_24h=vol, pool_type=ptype))
        groups = {k:g for k,g in groups.items() if all(p.pool_type=="v2" for p in g.pools) and len(g.pools)>=2}
        print(f"  V2-only groups (≥2 pools): {len(groups)}")
        return groups

# =============================================================================
# OBSERVATION ENGINE
# =============================================================================

class ObservationEngine:
    def __init__(self, session, pipeline, memory, oc, tg):
        self.session, self.pipeline, self.memory, self.oc, self.tg = session, pipeline, memory, oc, tg
        self.groups = {}
        self.active_opps = {}
        self.spread_hist = defaultdict(lambda: deque(maxlen=SPREAD_HISTORY_LEN))
        self.watchlist = set()
        self.running = False
        self._sem = asyncio.Semaphore(CONCURRENCY_LIMIT)

    def add_groups(self, groups):
        for k,g in groups.items():
            if k not in self.groups: self.groups[k] = g

    async def start(self):
        self.running = True
        await self._update_watchlist()
        asyncio.create_task(self._poll_loop())
        asyncio.create_task(self._watchlist_loop())

    async def _update_watchlist(self):
        scored = []
        for g in self.groups.values():
            if g.blacklisted: continue
            score = self.memory.get(g.key).compute_repeatability_score()
            if score>0: scored.append((score,g))
        scored.sort(key=lambda x:x[0], reverse=True)
        self.watchlist = {g.key for _,g in scored[:TOP_N_PAIRS]}

    async def _watchlist_loop(self, interval=300):
        while self.running:
            await asyncio.sleep(interval)
            await self._update_watchlist()

    async def _poll_loop(self):
        while self.running:
            start = time.time()
            targets = [g for g in self.groups.values() if g.key in self.watchlist]
            async with self._sem:
                await asyncio.gather(*[self._update_group(g) for g in targets], return_exceptions=True)
            elapsed = time.time() - start
            await asyncio.sleep(max(0.0, OBSERVE_INTERVAL - elapsed))

    async def _update_group(self, group):
        if group.blacklisted: return
        if not await self.oc.get_reserves_for_group(group): return
        group.compute_spread()
        spread = group.spread
        self.spread_hist[group.key].append(spread)
        mem = self.memory.get(group.key)
        mem.last_5_spreads.append(spread)
        mem.last_liquidity = group.total_liquidity
        hist = self.spread_hist[group.key]
        if len(hist)>=2:
            prev = list(hist)[-2]
            if prev > MIN_SPREAD and spread < prev*0.5:
                time_diff = time.time() - mem.last_update_time
                mem.competition_score += 20 if time_diff<0.5 else 10
                mem.classify_behavior()
        if spread > MIN_SPREAD:
            if group.key not in self.active_opps:
                self.active_opps[group.key] = {"start": time.time(), "max_spread": spread}
            else:
                self.active_opps[group.key]["max_spread"] = max(self.active_opps[group.key]["max_spread"], spread)
        else:
            if group.key in self.active_opps:
                ao = self.active_opps.pop(group.key)
                duration = time.time() - ao["start"]
                mem.update(ao["max_spread"], duration)
                mem.real_breakouts += 1
                mem.classify_behavior()
                await self.memory.save(mem)
        if time.time() - mem.last_seen > 3600: group.blacklisted = True
        if spread>0.001:
            print(f"  [{group.chain:10s}] {group.symbol_pair:25s} spread={spread*100:6.3f}%  liq=${group.total_liquidity:>9,.0f}  pools={len(group.pools)}  opps={mem.opportunity_count}")

    async def run_pipeline(self, group, latency=0.0):
        return await self.pipeline.run(group, self.memory.get(group.key), latency)

    def get_top_pairs(self):
        scored = []
        for g in self.groups.values():
            if g.blacklisted: continue
            score = self.memory.get(g.key).compute_repeatability_score()
            if score>0: scored.append((score,g))
        scored.sort(key=lambda x:x[0], reverse=True)
        return scored

# =============================================================================
# LIVE ARBITRAGE ENGINE (with all upgrades)
# =============================================================================

class LiveArbitrageEngine:
    def __init__(self, obs, pipeline, memory, oc, tg):
        self.obs, self.pipeline, self.memory, self.oc, self.tg = obs, pipeline, memory, oc, tg
        self.running = False
        self.tasks = {}
        self.last_emit = {}

    async def start(self):
        self.running = True
        asyncio.create_task(self._manager_loop())

    async def _manager_loop(self):
        while self.running:
            selected = []
            for g in self.obs.groups.values():
                if g.blacklisted or g.key not in self.obs.watchlist: continue
                mem = self.memory.get(g.key)
                if mem.behavior_type in ["SNIPABLE","SCHEDULED"] and mem.compute_repeatability_score()>0.4:
                    selected.append(g)
            selected.sort(key=lambda g: self.memory.get(g.key).compute_repeatability_score(), reverse=True)
            selected = selected[:15]
            current = {g.key for g in selected}
            for k in list(self.tasks.keys()):
                if k not in current: self.tasks[k].cancel(); del self.tasks[k]
            for g in selected:
                if g.key not in self.tasks:
                    self.tasks[g.key] = asyncio.create_task(self._watch_pair(g))
            await asyncio.sleep(2.0)

    async def _watch_pair(self, group):
        while self.running:
            try:
                if not await self.oc.get_reserves_for_group(group):
                    await asyncio.sleep(0.2); continue
                group.compute_spread()
                mem = self.memory.get(group.key)
                spread = group.spread
                early = False
                if mem.detect_pre_signal(spread, group.total_liquidity):
                    early_threshold = mem.dynamic_threshold() * 0.7
                    if spread > early_threshold:
                        early = True
                        # Pre-signal alert (cooldown 60s)
                        await self.tg.send(
                            f"🧠 *PRE-SIGNAL*\n{group.symbol_pair} ({group.chain})\nSpread building...",
                            key=f"pre_{group.key}", cooldown=60
                        )
                threshold = early_threshold if early else mem.dynamic_threshold()
                if spread < threshold:
                    await asyncio.sleep(0.2); continue
                latency_range = CHAIN_LATENCY.get(group.chain, (0.3,0.7))
                latency = random.uniform(*latency_range)
                await asyncio.sleep(latency)
                if not await self.oc.get_reserves_for_group(group):
                    await asyncio.sleep(0.2); continue
                group.compute_spread()
                if group.spread < threshold:
                    mem.missed_opportunities += 1
                    await self.memory.save(mem)
                    continue
                now = time.time()
                if now - mem.last_trade_time < mem.avg_spike_interval:
                    await asyncio.sleep(0.2); continue
                result = await self.pipeline.run(group, mem, latency)
                if result:
                    mem.update(result["spread"], result.get("duration",0),
                               trade_usd=result["trade_usd"], net_profit=result["net_profit"])
                    mem.last_trade_time = now
                    mem.successful_cycles += 1
                    mem.classify_behavior()
                    await self.memory.save(mem)
                    await self.memory.log_opp({**result, "duration": result.get("duration",0)})
                    self.last_emit[group.key] = now
                    tier = result["tier"]

                    # Send only if net profit >= 15 (realistic edge)
                    if result["net_profit"] >= 15:
                        msg = (
                            f"⚡ *ARB SIGNAL* ({tier})\n"
                            f"{group.symbol_pair} | {group.chain}\n"
                            f"Spread: {result['spread_pct']:.2f}%\n"
                            f"Profit: ${result['net_profit']:.2f}\n"
                            f"Size: ${result['trade_usd']:.0f}\n"
                            f"Liq: ${group.total_liquidity:,.0f}\n"
                            f"Success: {result['success_prob']*100:.1f}%\n"
                            f"{result['buy_dex']} → {result['sell_dex']}"
                        )
                        await self.tg.send(msg, key=group.key, cooldown=90)

                    print(f"\n⚡ LIVE ARB SIGNAL [{tier} confidence]")
                    print(f"{group.symbol_pair} ({group.chain})")
                    print(f"Spread: {result['spread_pct']:.3f}%")
                    print(f"Profit: ${result['net_profit']:.2f}")
                    print(f"Expected profit: ${result['expected_profit']:.2f}")
                    print(f"Success prob: {result['success_prob']*100:.1f}%")
                    print(f"Route: {result['buy_dex']} → {result['sell_dex']}")
                    print("-"*50)
                else:
                    mem.failed_cycles += 1
                    await self.memory.save(mem)
                    # Log failure to Telegram (cooldown 120s)
                    await self.tg.send(
                        f"❌ *Failed*: {group.symbol_pair}\n"
                        f"spread {group.spread*100:.2f}%",
                        key=f"fail_{group.key}", cooldown=120
                    )
                    # Auto-kill after 10 failures with 0 successes
                    if mem.failed_cycles > 10 and mem.successful_cycles == 0:
                        group.blacklisted = True
                        await self.tg.send(
                            f"💀 *Auto-killed*: {group.symbol_pair} (10 failures, 0 success)",
                            key=f"kill_{group.key}", cooldown=3600
                        )
            except Exception as e:
                await self.tg.send(f"⚠️ Error: {str(e)[:200]}", key="error", cooldown=60)
            await asyncio.sleep(0.2)

# =============================================================================
# MAIN SCANNER
# =============================================================================

class ArbitrageScanner:
    def __init__(self):
        self.session = SafeSession()
        self.oc = OnChainValidator()
        self.sim = ProfitSimulator()
        self.memory = PairMemoryStore()
        self.pipeline = None
        self.obs = None
        self.live = None
        self.tg = None
        self.running = False

    async def start(self):
        self.running = True
        await self.session.start()
        await self.memory.init()
        await self.memory.load_all()
        self.pipeline = ValidationPipeline(self.oc, self.sim, self.memory)
        self.tg = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, self.session)

        discovery = DiscoveryEngine(self.session)
        groups = await discovery.discover_pairs()
        sorted_g = sorted(groups.values(), key=lambda g: g.total_liquidity, reverse=True)
        for g in sorted_g: g.compute_spread()
        print(f"\n📋 TOP PAIRS BY LIQUIDITY (sample):")
        print(f"{'#':3} {'Chain':10} {'Pair':28} {'Liq':>12} {'Spread%':>8} {'Pools':>6} {'DEXes'}")
        print("-"*90)
        for i,g in enumerate(sorted_g[:25],1):
            dexes = list({p.dex for p in g.pools})[:3]
            print(f"{i:3d} {g.chain:10s} {g.symbol_pair:28s} ${g.total_liquidity:>11,.0f} {g.spread*100:>7.3f}% {len(g.pools):>6}  {dexes}")
        print("-"*90)

        await self.tg.send(f"Discovered {len(groups)} valid V2 pairs", key="discovery", cooldown=3600)

        self.obs = ObservationEngine(self.session, self.pipeline, self.memory, self.oc, self.tg)
        self.obs.add_groups(groups)
        await self.obs.start()
        print(f"\n👁️  Observing {len(groups)} V2-only groups on {', '.join(TARGET_CHAINS)}...")

        self.live = LiveArbitrageEngine(self.obs, self.pipeline, self.memory, self.oc, self.tg)
        await self.live.start()
        print("🚀 LiveArbitrageEngine started")

        asyncio.create_task(self._ranking_loop())
        asyncio.create_task(self._heartbeat())
        asyncio.create_task(self._rediscovery_loop())

    async def _ranking_loop(self, interval=30):
        while self.running:
            top = self.obs.get_top_pairs()
            if top:
                print(f"\n🏆 TOP {len(top)} PAIRS BY REPEATABILITY SCORE:")
                print(f"{'Pair':30} {'Chain':10} {'Score':6} {'Opps':5} {'Avg%':7} {'Max%':7} {'Type':12} {'Liquidity':>12}")
                print("-"*100)
                for score,g in top[:5]:
                    mem = self.memory.get(g.key)
                    print(f"{g.symbol_pair:30s} {g.chain:10s} {score:6.4f} {mem.opportunity_count:5d} {mem.avg_spread*100:6.3f}% {mem.max_spread*100:6.3f}% {mem.behavior_type:12s} ${g.total_liquidity:>11,.0f}")
                print("-"*100)

                # Telegram top pairs report (cooldown 5 min)
                top_text = "🏆 *TOP PAIRS*\n"
                for score,g in top[:5]:
                    mem = self.memory.get(g.key)
                    top_text += f"{g.symbol_pair} ({g.chain}) | {score:.3f} | {mem.behavior_type}\n"
                await self.tg.send(top_text, key="top_pairs", cooldown=300)

                # Daily summary (every 10 min)
                summary = (
                    f"📊 *SUMMARY*\n"
                    f"Pairs tracked: {len(self.obs.groups)}\n"
                    f"Watchlist: {len(self.obs.watchlist)}\n"
                    f"Active opps: {len(self.obs.active_opps)}"
                )
                await self.tg.send(summary, key="summary", cooldown=600)
            else:
                print("⚠️  No pairs with sufficient history yet.")
            await asyncio.sleep(interval)

    async def _heartbeat(self, interval=15):
        while self.running:
            watching = len(self.obs.groups)
            active = len(self.obs.active_opps)
            blacklisted = sum(1 for g in self.obs.groups.values() if g.blacklisted)
            watchlist = len(self.obs.watchlist)
            print(f"\n💓 {time.strftime('%H:%M:%S')} | Watching: {watching} | Active spreads: {active} | Blacklisted: {blacklisted} | Watchlist: {watchlist}")
            await asyncio.sleep(interval)

    async def _rediscovery_loop(self, interval=180):
        while self.running:
            await asyncio.sleep(interval)
            print("\n🔄 Re-discovering...")
            discovery = DiscoveryEngine(self.session)
            new_groups = await discovery.discover_pairs()
            added = 0
            for k,g in new_groups.items():
                if k not in self.obs.groups:
                    self.obs.groups[k] = g; added+=1
            if added: print(f"  ✅ Added {added} new V2 groups.")

    async def stop(self):
        self.running = False
        await self.session.stop()

# =============================================================================
# RUN
# =============================================================================

scanner = ArbitrageScanner()
loop = asyncio.get_event_loop()
loop.create_task(scanner.start())
try:
    loop.run_forever()
except KeyboardInterrupt:
    print("\nStopping...")
    loop.run_until_complete(scanner.stop())