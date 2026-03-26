import asyncio
import aiohttp
import time
import math
import random
import os
import logging
from collections import defaultdict, deque
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# CONFIGURATION (PROFIT-FOCUSED)
# =============================================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TARGET_CHAINS = None
EVM_CHAINS = {
    "base":     {"rpc": "https://mainnet.base.org",         "gecko": "base"},
    "arbitrum": {"rpc": "https://arb1.arbitrum.io/rpc",     "gecko": "arbitrum"},
    "bsc":      {"rpc": "https://bsc-dataseed.binance.org", "gecko": "bsc"},
    "polygon":  {"rpc": "https://polygon-rpc.com",          "gecko": "polygon_pos"},
    "ethereum": {"rpc": "https://eth.llamarpc.com",         "gecko": "ethereum"},
    "optimism": {"rpc": "https://mainnet.optimism.io",      "gecko": "optimism"},
}
GECKO_TO_CHAIN = {v["gecko"]: k for k, v in EVM_CHAINS.items()}
GECKO_TO_CHAIN["polygon_pos"] = "polygon"

MULTICALL3_ADDR = "0xcA11bde05977b3631167028862bE2a173976CA11"

V3_DEX_IDS = {
    "uniswap-v3", "pancakeswap-v3", "camelot-v3", "sushiswap-v3",
    "aerodrome-cl", "velodrome-v2", "quickswap-v3", "zyberswap",
    "ramses-v2", "thena-fusion", "algebra",
}

STABLECOIN_SYMBOLS = {"USDT", "USDC", "DAI", "BUSD", "TUSD", "FDUSD",
                      "FRAX", "LUSD", "USDD", "GUSD", "USDP"}

MIN_LIQUIDITY             = 20_000
MAX_LIQUIDITY             = 500_000
MIN_VOLUME_LIQ_RATIO      = 2.0
MIN_POOLS                 = 2
MAX_POOLS                 = 4
MIN_OPPORTUNITY_COUNT     = 3
MIN_AVG_SPREAD            = 0.008
MIN_AVG_DURATION          = 2.5
MIN_LIQUIDITY_TARGET      = 30_000
TOP_N_PAIRS               = 8
MIN_PROFIT_POTENTIAL_USD  = 8.0
MAX_SLIPPAGE_IMPACT       = 0.018
FLASHLOAN_FEE_BPS         = 9
GAS_COST_USD              = 0.5

OBSERVE_INTERVAL          = 2.0
SPREAD_HISTORY_LEN        = 60
RATE_LIMIT_SLEEP          = 0.25
CONCURRENCY_LIMIT         = 20

DEXSCREENER_TOKEN_URL  = "https://api.dexscreener.com/latest/dex/tokens/{}"
DEXSCREENER_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search?q={}"
GECKO_BASE             = "https://api.geckoterminal.com/api/v2"

DEXSCREENER_QUERIES = ["new", "trending", "hot", "base", "sol", "bsc", "arb", "eth", "polygon"]

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
        self.time_of_day_histogram: Dict[int, int] = field(default_factory=dict)
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
        self.spread_history = deque(maxlen=SPREAD_HISTORY_LEN)
        self.active_opportunity = None
        self.memory = TokenMemory(token_key)
        self.cex_price = 0.0
        self.dex_spread = 0.0
        self.profit_potential = 0.0

    def compute_spreads_and_profit(self):
        prices = [p["price"] for p in self.pools if p["price"] > 0]
        if len(prices) >= 2:
            mn, mx = min(prices), max(prices)
            self.dex_spread = (mx - mn) / mn if mn > 0 else 0.0
        else:
            self.dex_spread = 0.0

        if self.cex_price > 0 and prices:
            all_p = prices + [self.cex_price]
            mn, mx = min(all_p), max(all_p)
            total_spread = (mx - mn) / mn if mn > 0 else 0.0
        else:
            total_spread = self.dex_spread

        repeatability = self.memory.compute_repeatability_score() if self.memory.opportunity_count > 0 else 0.3
        slippage_adjust = 1.0 - MAX_SLIPPAGE_IMPACT
        gross = total_spread * self.liquidity_total * 0.02 * slippage_adjust
        net = gross - GAS_COST_USD - (gross * FLASHLOAN_FEE_BPS / 10000)
        self.profit_potential = max(0.0, net)

    def update_from_pools(self, new_pools: List[Dict]):
        self.pools = new_pools
        self.liquidity_total = sum(p["liquidity"] for p in new_pools)
        self.volume_24h = sum(p["volume_24h"] for p in new_pools)
        self.last_update = time.time()


# =============================================================================
# SAFE HTTP SESSION
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
# TELEGRAM NOTIFIER
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
        await self.session.post(self.url, {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown"
        })


# =============================================================================
# DISCOVERY ENGINE
# =============================================================================

class DiscoveryEngine:
    def __init__(self, session: SafeSession):
        self.session = session

    @staticmethod
    def _pf(val, default=0.0):
        try:
            return float(val or default)
        except (TypeError, ValueError):
            return default

    async def _dexscreener_search(self, q: str) -> List[Dict]:
        d = await self.session.get(DEXSCREENER_SEARCH_URL.format(q), "dexscreener")
        return (d or {}).get("pairs", [])

    async def _gecko_pools(self, gnet: str, page: int = 1) -> List[Dict]:
        url = f"{GECKO_BASE}/networks/{gnet}/pools?page={page}&include=base_token,quote_token,dex"
        data = await self.session.get(url, "gecko")
        return self._parse_gecko(data or {}, gnet)

    async def _gecko_trending(self, gnet: str) -> List[Dict]:
        url = f"{GECKO_BASE}/networks/{gnet}/trending_pools?include=base_token,quote_token,dex"
        data = await self.session.get(url, "gecko")
        return self._parse_gecko(data or {}, gnet)

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
            results.append({
                "chainId": chain,
                "baseToken": {"address": bt["address"], "symbol": bt.get("symbol", "")},
                "quoteToken": {"address": qt["address"], "symbol": qt.get("symbol", "")},
                "priceUsd": self._pf(attrs.get("base_token_price_usd")),
                "liquidity": {"usd": self._pf(attrs.get("reserve_in_usd"))},
                "volume": {"h24": self._pf((attrs.get("volume_usd") or {}).get("h24"))},
                "dexId": dex_id,
                "pairAddress": attrs.get("address", ""),
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
            if "_" in baddr and not baddr.startswith("0x"):
                baddr = baddr.split("_", 1)[1]
            if "_" in qaddr and not qaddr.startswith("0x"):
                qaddr = qaddr.split("_", 1)[1]
            if not baddr or not qaddr or baddr == "0x0000000000000000000000000000000000000000":
                continue
            liq_raw = p.get("liquidity", {})
            liq = self._pf(liq_raw.get("usd") if isinstance(liq_raw, dict) else liq_raw)
            vol_raw = p.get("volume", {})
            vol = self._pf(vol_raw.get("h24") if isinstance(vol_raw, dict) else vol_raw)
            if liq < MIN_LIQUIDITY or liq > MAX_LIQUIDITY:
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
            ptype = "v3" if p.get("dexId", "") in V3_DEX_IDS else "v2"
            token_map[key]["pools"].append({
                "dex": p.get("dexId", "unknown"),
                "price": self._pf(p.get("priceUsd")),
                "liquidity": liq,
                "volume_24h": vol,
                "pool_address": (p.get("pairAddress") or "").lower(),
                "pool_type": ptype
            })
            token_map[key]["total_liquidity"] += liq
            token_map[key]["total_volume_24h"] += vol

        active_tokens = []
        for key, info in token_map.items():
            if len(info["pools"]) < MIN_POOLS or len(info["pools"]) > MAX_POOLS:
                continue
            if info["total_liquidity"] < MIN_LIQUIDITY or info["total_liquidity"] > MAX_LIQUIDITY:
                continue
            vol_liq = info["total_volume_24h"] / max(1, info["total_liquidity"])
            if vol_liq < MIN_VOLUME_LIQ_RATIO:
                continue
            if any(p["price"] <= 0 for p in info["pools"]):
                continue

            sym = info["symbol"].upper()
            if sym in STABLECOIN_SYMBOLS or sym in {"WETH", "USDC", "USDT", "SOL", "UNI", "WBTC"}:
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

            # CEX price fetch
            cex = await self.session.get(f"https://api.binance.com/api/v3/ticker/price?symbol={sym}USDT", "cex")
            if cex and "price" in cex:
                state.cex_price = float(cex["price"])
            state.compute_spreads_and_profit()

            if state.profit_potential >= MIN_PROFIT_POTENTIAL_USD:
                active_tokens.append(state)

        print(f"  FINAL low-competition profit-promising pairs: {len(active_tokens)}")
        return active_tokens


# =============================================================================
# STATE ENGINE
# =============================================================================

class StateEngine:
    def __init__(self):
        self.tokens: Dict[str, TokenState] = {}
        self.lock = asyncio.Lock()

    async def update_token(self, token_key: str, chain: str, contract: str, pools: List[Dict], symbol: str = "", cex_price: float = None):
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
            state.compute_spreads_and_profit()

    async def get_all_tokens(self) -> List[TokenState]:
        async with self.lock:
            return list(self.tokens.values())


# =============================================================================
# RANKING ENGINE
# =============================================================================

class RankingEngine:
    def __init__(self, state_engine: StateEngine):
        self.state_engine = state_engine

    async def rank_tokens(self) -> List[TokenState]:
        tokens = await self.state_engine.get_all_tokens()
        scored = []
        for token in tokens:
            if token.memory.opportunity_count < MIN_OPPORTUNITY_COUNT:
                continue
            if token.profit_potential < MIN_PROFIT_POTENTIAL_USD:
                continue
            scored.append((token.profit_potential, token))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [token for _, token in scored[:TOP_N_PAIRS]]


# =============================================================================
# OBSERVATION ENGINE
# =============================================================================

class ObservationEngine:
    def __init__(self, session: SafeSession, state_engine: StateEngine, tg: TelegramNotifier):
        self.session = session
        self.state_engine = state_engine
        self.tg = tg
        self.running = False
        self.active_opps = {}
        self._sem = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async def start(self):
        self.running = True
        asyncio.create_task(self._poll_loop())

    async def _poll_loop(self):
        while self.running:
            start = time.time()
            tokens = await self.state_engine.get_all_tokens()
            async with self._sem:
                await asyncio.gather(*[self._update_token(t) for t in tokens], return_exceptions=True)
            elapsed = time.time() - start
            await asyncio.sleep(max(0.0, OBSERVE_INTERVAL - elapsed))

    async def _update_token(self, token: TokenState):
        raw = await self.session.get(DEXSCREENER_TOKEN_URL.format(token.contract), "dexscreener")
        if not raw:
            return
        new_pools = []
        for p in raw.get("pairs", []):
            if p.get("chainId") != token.chain:
                continue
            price = float(p.get("priceUsd", 0))
            liq_raw = p.get("liquidity", {})
            liquidity = float(liq_raw.get("usd", 0)) if isinstance(liq_raw, dict) else float(liq_raw) if liq_raw else 0
            vol_raw = p.get("volume", {})
            volume_24h = float(vol_raw.get("h24", 0)) if isinstance(vol_raw, dict) else float(vol_raw) if vol_raw else 0
            dex = p.get("dexId", "unknown")
            new_pools.append({
                "dex": dex,
                "price": price,
                "liquidity": liquidity,
                "volume_24h": volume_24h,
                "pool_address": p.get("pairAddress", ""),
                "pool_type": "v3" if dex in V3_DEX_IDS else "v2"
            })
        token.update_from_pools(new_pools)

        sym = token.symbol.upper().split()[0].split("/")[0]
        cex_data = await self.session.get(f"https://api.binance.com/api/v3/ticker/price?symbol={sym}USDT", "cex")
        if cex_data and "price" in cex_data:
            token.cex_price = float(cex_data["price"])

        token.compute_spreads_and_profit()
        print(f"  OBSERVE {token.symbol} ({token.chain}) | spread={token.dex_spread*100:.2f}% | profit_pot=${token.profit_potential:.1f}")

        spread = token.dex_spread
        if spread > MIN_AVG_SPREAD:
            if token.key not in self.active_opps:
                self.active_opps[token.key] = {"start": time.time(), "max_spread": spread}
            else:
                self.active_opps[token.key]["max_spread"] = max(self.active_opps[token.key]["max_spread"], spread)
        else:
            if token.key in self.active_opps:
                ao = self.active_opps.pop(token.key)
                duration = time.time() - ao["start"]
                token.memory.update(ao["max_spread"], duration)
                token.memory.real_breakouts += 1


# =============================================================================
# MICRO-DOMINATION
# =============================================================================

class MicroDomination:
    def __init__(self, state_engine: StateEngine, session: SafeSession, tg: TelegramNotifier):
        self.state_engine = state_engine
        self.session = session
        self.tg = tg
        self.running = False
        self.tasks = {}

    async def start(self):
        self.running = True

    async def watch_token(self, token_key: str):
        while self.running:
            token = self.state_engine.tokens.get(token_key)
            if not token or token.profit_potential < MIN_PROFIT_POTENTIAL_USD:
                break
            raw = await self.session.get(DEXSCREENER_TOKEN_URL.format(token.contract), "dexscreener")
            if not raw:
                await asyncio.sleep(0.2)
                continue
            new_pools = []
            for p in raw.get("pairs", []):
                if p.get("chainId") != token.chain:
                    continue
                price = float(p.get("priceUsd", 0))
                liq_raw = p.get("liquidity", {})
                liquidity = float(liq_raw.get("usd", 0)) if isinstance(liq_raw, dict) else float(liq_raw) if liq_raw else 0
                vol_raw = p.get("volume", {})
                volume_24h = float(vol_raw.get("h24", 0)) if isinstance(vol_raw, dict) else float(vol_raw) if vol_raw else 0
                dex = p.get("dexId", "unknown")
                new_pools.append({
                    "dex": dex,
                    "price": price,
                    "liquidity": liquidity,
                    "volume_24h": volume_24h,
                    "pool_address": p.get("pairAddress", ""),
                    "pool_type": "v3" if dex in V3_DEX_IDS else "v2"
                })
            token.update_from_pools(new_pools)
            token.compute_spreads_and_profit()
            if token.profit_potential < MIN_PROFIT_POTENTIAL_USD:
                break
            await asyncio.sleep(0.5)

    async def update_targets(self, top_tokens: List[TokenState]):
        top_tokens = [t for t in top_tokens if t.profit_potential >= MIN_PROFIT_POTENTIAL_USD]
        for key in list(self.tasks.keys()):
            if key not in [t.key for t in top_tokens]:
                self.tasks[key].cancel()
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
        self.state_engine = StateEngine()
        self.tg = None
        self.obs = None
        self.micro = None
        self.ranking = None
        self.running = False

    async def start(self):
        self.running = True
        await self.session.start()
        self.tg = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, self.session)

        discovery = DiscoveryEngine(self.session)
        tokens = await discovery.discover_tokens()
        for t in tokens:
            await self.state_engine.update_token(t.key, t.chain, t.contract, t.pools, t.symbol)

        sorted_tokens = sorted(tokens, key=lambda x: x.profit_potential, reverse=True)
        print(f"\n📋 PROFIT-PROMISING SHORTLIST — {len(sorted_tokens)} pairs")
        for i, t in enumerate(sorted_tokens[:15], 1):
            print(f"{i:2d}. {t.symbol:12} ({t.chain}) | liq=${t.liquidity_total:,.0f} | "
                  f"spread={t.dex_spread*100:.2f}% | **profit_potential=${t.profit_potential:.1f}**")
        await self.tg.send(f"Discovered {len(tokens)} profit-promising pairs", key="discovery", cooldown=3600)

        self.obs = ObservationEngine(self.session, self.state_engine, self.tg)
        await self.obs.start()
        print("👁️  Observation started.")

        self.micro = MicroDomination(self.state_engine, self.session, self.tg)
        await self.micro.start()
        print("🚀 Micro‑domination ready.")

        self.ranking = RankingEngine(self.state_engine)
        asyncio.create_task(self._ranking_loop())
        asyncio.create_task(self._heartbeat())

    async def _ranking_loop(self, interval=30):
        while self.running:
            top = await self.ranking.rank_tokens()
            if top:
                print(f"\n🏆 TOP {len(top)} PAIRS BY PROFIT POTENTIAL:")
                for i, t in enumerate(top, 1):
                    print(f"{i:2d}. {t.symbol:12} ({t.chain}) | profit=${t.profit_potential:.1f} | spread={t.dex_spread*100:.2f}% | liq=${t.liquidity_total:,.0f}")
                await self.micro.update_targets(top)
                top_text = "🏆 *TOP PROFIT POTENTIAL*\n"
                for t in top[:5]:
                    top_text += f"{t.symbol} ({t.chain}) | ${t.profit_potential:.1f} | {t.dex_spread*100:.2f}%\n"
                await self.tg.send(top_text, key="top_pairs", cooldown=300)
                summary = f"📊 *SUMMARY*\nPairs tracked: {len(await self.state_engine.get_all_tokens())}\nActive watchers: {len(self.micro.tasks)}"
                await self.tg.send(summary, key="summary", cooldown=600)
            else:
                print("⚠️  No pairs with sufficient history yet.")
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
    scanner = ArbitrageScanner()
    try:
        await scanner.start()
        # Keep running until interrupted
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await scanner.stop()

if __name__ == "__main__":
    asyncio.run(main())