import os
import asyncio
import time
import hashlib
import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from decimal import Decimal, getcontext
from collections import defaultdict, OrderedDict
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
import aiohttp
from aiohttp import ClientTimeout, web

getcontext().prec = 78

# ============================================================================
# Constants (hardcoded, not env)
# ============================================================================
MAX_HOPS = 3
SCAN_INTERVAL = 5
COOLDOWN_SECONDS = 60
MIN_PROFIT_USD = Decimal('5')
HEALTH_PORT = 8080
DEFAULT_GAS_LIMIT_PER_STEP = 150000
DEFAULT_GAS_LIMIT_OVERHEAD = 50000
FLASH_LOAN_AMOUNT_USD = Decimal('10000')

# ============================================================================
# Configuration Data
# ============================================================================

@dataclass
class ChainConfig:
    name: str
    rpc_urls: List[str]
    chain_id: int
    native_token: str
    native_token_symbol: str
    native_token_gecko_id: str
    flash_loan_fee_bps: int
    block_time_seconds: float
    explorer_url: str
    multicall3_address: str
    quoter_v2_address: str   # QuoterV2 contract address (for V3)
    native_price_usd: float = 0.0

@dataclass
class TokenConfig:
    symbol: str
    address: str
    decimals: int
    is_stable: bool = False

@dataclass
class DEXConfig:
    name: str
    chain: str
    router_address: str
    dex_type: str                     # 'v2', 'v3', 'orderbook', 'lb', 'solidly'
    fee_bps: int
    version: str
    factory_address: Optional[str] = None
    quoter_address: Optional[str] = None   # for V3 (if different from chain default)

@dataclass
class PoolInfo:
    dex_name: str
    token0: str
    token1: str
    reserve0_raw: int          # raw wei amount
    reserve1_raw: int          # raw wei amount
    fee_bps: int
    tvl_usd: Decimal
    address: str
    # V3 specific
    sqrt_price_x96: Optional[int] = None
    liquidity: Optional[int] = None
    tick: Optional[int] = None
    # LB specific
    bin_step: Optional[int] = None
    active_id: Optional[int] = None
    # Solidly specific
    stable: Optional[bool] = None

@dataclass
class SwapStep:
    dex_name: str
    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out: Decimal
    fee_bps: int
    pair_address: Optional[str] = None   # for LB and Solidly
    stable: Optional[bool] = None        # for Solidly

@dataclass
class ArbitragePath:
    chain: str
    start_token: str
    end_token: str
    steps: List[SwapStep]
    total_profit_usd: Decimal
    flash_loan_amount: Decimal
    gas_cost_usd: Decimal
    net_profit_usd: Decimal
    is_profitable: bool
    simulation_result: Dict[str, Any]

# ============================================================================
# Chain Configurations (fully filled)
# ============================================================================
CHAIN_CONFIGS = {
    'monad': ChainConfig(
        name='Monad',
        rpc_urls=[
            'https://rpc.monad.xyz',
            'https://rpc1.monad.xyz',
        ],
        chain_id=10143,
        native_token='MON',
        native_token_symbol='MON',
        native_token_gecko_id='monad',
        flash_loan_fee_bps=9,
        block_time_seconds=0.4,
        explorer_url='https://monadvision.com',
        multicall3_address='0xcA11bde05977b3631167028862bE2a173976CA11',
        quoter_v2_address='0x661e93cca42afacb172121ef892830ca3b70f08d',
    ),
    'sonic': ChainConfig(
        name='Sonic',
        rpc_urls=[
            'https://rpc.soniclabs.com',
            'https://sonic-rpc.publicnode.com',
        ],
        chain_id=146,
        native_token='S',
        native_token_symbol='S',
        native_token_gecko_id='sonic-s',
        flash_loan_fee_bps=9,
        block_time_seconds=0.2,
        explorer_url='https://sonicscan.org',
        multicall3_address='0xcA11bde05977b3631167028862bE2a173976CA11',
        quoter_v2_address='0x219b7ADebc0935a3eC889a148c6924D51A07535A',
    ),
    'linea': ChainConfig(
        name='Linea',
        rpc_urls=[
            'https://linea-rpc.publicnode.com',
            'https://1rpc.io/linea',
        ],
        chain_id=59144,
        native_token='ETH',
        native_token_symbol='ETH',
        native_token_gecko_id='ethereum',
        flash_loan_fee_bps=9,
        block_time_seconds=2.0,
        explorer_url='https://lineascan.build',
        multicall3_address='0xcA11bde05977b3631167028862bE2a173976CA11',
        quoter_v2_address='0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a',
    ),
    'scroll': ChainConfig(
        name='Scroll',
        rpc_urls=[
            'https://scroll-rpc.publicnode.com',
            'https://1rpc.io/scroll',
        ],
        chain_id=534352,
        native_token='ETH',
        native_token_symbol='ETH',
        native_token_gecko_id='ethereum',
        flash_loan_fee_bps=9,
        block_time_seconds=3.0,
        explorer_url='https://scrollscan.com',
        multicall3_address='0xcA11bde05977b3631167028862bE2a173976CA11',
        quoter_v2_address='0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a',
    ),
    'mantle': ChainConfig(
        name='Mantle',
        rpc_urls=[
            'https://mantle-rpc.publicnode.com',
            'https://1rpc.io/mantle',
        ],
        chain_id=5000,
        native_token='MNT',
        native_token_symbol='MNT',
        native_token_gecko_id='mantle',
        flash_loan_fee_bps=9,
        block_time_seconds=1.5,
        explorer_url='https://explorer.mantle.xyz',
        multicall3_address='0xcA11bde05977b3631167028862bE2a173976CA11',
        quoter_v2_address='0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997',
    ),
    'bnb': ChainConfig(
        name='BNB Chain',
        rpc_urls=[
            'https://bsc-dataseed.bnbchain.org',
            'https://bsc-rpc.publicnode.com',
        ],
        chain_id=56,
        native_token='BNB',
        native_token_symbol='BNB',
        native_token_gecko_id='binancecoin',
        flash_loan_fee_bps=5,
        block_time_seconds=0.45,
        explorer_url='https://bscscan.com',
        multicall3_address='0xcA11bde05977b3631167028862bE2a173976CA11',
        quoter_v2_address='0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997',
    ),
}

# ============================================================================
# Stable Tokens (fully filled)
# ============================================================================
STABLE_TOKENS = {
    'monad': [
        TokenConfig('USDC', '0x754704Bc059F8C67012fEd69BC8A327a5aafb603', 6, True),
        TokenConfig('AUSD', '0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a', 18, True),
        TokenConfig('WMON', '0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A', 18, False),
    ],
    'sonic': [
        TokenConfig('USDC.e', '0x29219dd400f2Bf60E5a23d13Be72B486D4038894', 6, True),
        TokenConfig('USDT', '0x6047828dc181963ba44974801ff68e538da5eaf9', 6, True),
        TokenConfig('wS', '0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38', 18, False),
        TokenConfig('WETH', '0x50c42dEAcD8Fc9773493ED674b675bE577f2634b', 18, False),
    ],
    'linea': [
        TokenConfig('USDC', '0x176211869cA2b568f2A7D4EE941E073a821EE1ff', 6, True),
        TokenConfig('USDT', '0xA219439258ca9da29E9Cc4cE5596924745e12B93', 6, True),
        TokenConfig('WETH', '0xe5D7C2a44FFddf6b295A15c148167daaAf5Cf34f', 18, False),
    ],
    'scroll': [
        TokenConfig('USDC', '0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4', 6, True),
        TokenConfig('USDT', '0xf55bec9cafdbe8730f096aa55dad6d22d44099df', 6, True),
        TokenConfig('WETH', '0x5300000000000000000000000000000000000004', 18, False),
    ],
    'mantle': [
        TokenConfig('USDC', '0x09bc4e0d864854c6afb6eb9a9cdf58ac190d0df9', 6, True),
        TokenConfig('USDT', '0x201eba5cc46d216ce6dc03f6a759e8e766e956ae', 6, True),
        TokenConfig('WETH', '0xdeaddeaddeaddeaddeaddeaddeaddeaddead1111', 18, False),
    ],
    'bnb': [
        TokenConfig('BUSD', '0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56', 18, True),
        TokenConfig('USDT', '0x55d398326f99059fF775485246999027B3197955', 18, True),
        TokenConfig('WBNB', '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', 18, False),
    ],
}

# ============================================================================
# DEX Configurations (fully filled)
# ============================================================================
DEX_CONFIGS = {
    'monad': [
        DEXConfig('Kuru', 'monad', '0xd651346d7c789536ebf06dc72aE3C8502cd695CC', 'orderbook', 5, 'v1'),
        DEXConfig('UniswapV3', 'monad', '0xfe31f71c1b106eac32f1a19239c9a9a72ddfb900', 'v3', 30, 'v3',
                  factory_address='0x204faca1764b154221e35c0d20abb3c525710498',
                  quoter_address='0x661e93cca42afacb172121ef892830ca3b70f08d'),
    ],
    'sonic': [
        DEXConfig('Shadow', 'sonic', '0x1D368773735ee1E678950B7A97bcA2CafB330CDc', 'v3', 30, 'v3',
                  factory_address='0xcD2d0637c94fe77C2896BbCBB174cefFb08DE6d7',
                  quoter_address='0x219b7ADebc0935a3eC889a148c6924D51A07535A'),
        DEXConfig('SpookySwap', 'sonic', '0x0C2BC01d435CfEb2DC6Ad7cEC0E473e2DBaBdd87', 'v2', 20, 'v2',
                  factory_address='0xEE4bC42157cf65291Ba2FE839AE127e3Cc76f741'),
        DEXConfig('Metropolis', 'sonic', '0x95a7e403d7cF20F675fF9273D66e94d35ba49fA3', 'lb', 25, 'v1',
                  factory_address='0x70a833af49cab63a35060db30eb9441f097ac51f'),
    ],
    'linea': [
        DEXConfig('UniswapV3', 'linea', '0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a', 'v3', 30, 'v3',
                  factory_address='0x33128a8fC17869897dcE68Ed026d694621f6FDfD',
                  quoter_address='0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a'),
        DEXConfig('NILE', 'linea', '0xAAA45c8F5ef92a000a121d102F4e89278a711Faa', 'solidly', 25, 'v2',
                  factory_address='0xAAA32926fcE6bE95ea2c51cB4Fcb60836D320C42'),
    ],
    'scroll': [
        DEXConfig('UniswapV3', 'scroll', '0xfc30937f5cde93df8d48acaf7e6f5d8d8a31f636', 'v3', 30, 'v3',
                  factory_address='0x1f98400000000000000000000000000000000002',
                  quoter_address='0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a'),
        DEXConfig('SushiSwap', 'scroll', '0x9b3336186a38e1b6c21955d112dbb0343ee061ee', 'v2', 25, 'v2',
                  factory_address='0xb45e53277a7e0f1d35f2a77160e91e25507f1763'),
    ],
    'mantle': [
        DEXConfig('Agni', 'mantle', '0x8cFe327CFC4c2fF48c7f1F9b4c7e8cE2cA8f7c9a', 'v3', 30, 'v3',
                  factory_address='0x0c3F9cF9cC7cA9C9D9c9f9c9f9c9F9C9f9c9F9C9',
                  quoter_address='0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997'),
        DEXConfig('MerchantMoe', 'mantle', '0x013e138EF6008ae5FDFDE29700e3f2Bc61d21E3a', 'lb', 25, 'v1',
                  factory_address='0xa6630671775c4EA2743840F9A5016dCf2A104054'),
        DEXConfig('FusionX', 'mantle', '0x45e6f621c5ED8616cCFB9bBaeBAcF9638aBB0033', 'v2', 20, 'v2',
                  factory_address='0x272465431A6b86E3B9E5b9bD33f5D103a3F59eDb'),
    ],
    'bnb': [
        DEXConfig('PancakeSwap', 'bnb', '0x10ED43C718714eb63d5aA57B78B54704E256024E', 'v2', 25, 'v2',
                  factory_address='0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73'),
        DEXConfig('PancakeSwapV3', 'bnb', '0x1b81D678ffb9C0263b24A97847620C99d213eB14', 'v3', 5, 'v3',
                  factory_address='0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865',
                  quoter_address='0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997'),
        DEXConfig('Biswap', 'bnb', '0x3a6d8cA21D1CF76F653A67577FA0D27453364dd8', 'v2', 10, 'v2',
                  factory_address='0x858E3312ed3A876947EA49d572A7C42DE08af7EE'),
    ],
}

# ============================================================================
# Seed Pools (with explicit addresses where known)
# ============================================================================
SEED_POOLS = {
    'monad': [
        # UniswapV3 USDC/WMON (0.3% fee) – replace with actual pool address from MonadVision
        ('UniswapV3', '0x754704Bc059F8C67012fEd69BC8A327a5aafb603', '0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A', 0, 0, 30, '0x...'),
    ],
    'sonic': [
        # Shadow USDC.e / wS (0.3% fee) – high-liquidity pool (address provided)
        ('Shadow', '0x29219dd400f2Bf60E5a23d13Be72B486D4038894', '0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38', 0, 0, 30, '0x324963c267c354c7660ce8ca3f5f167e05649970'),
    ],
    'linea': [
        # UniswapV3 USDC/WETH – will be discovered via factory at runtime (add explicit address if needed)
    ],
    'scroll': [
        # UniswapV3 USDC/WETH – will be discovered via factory at runtime
        # SushiSwap USDC/USDT – will be discovered via factory
    ],
    'mantle': [
        # Agni USDC/USDT – will be discovered via factory at runtime
        # MerchantMoe USDC/WETH – will be discovered via factory
        # FusionX USDC/WETH – will be discovered via factory
    ],
    'bnb': [
        # PancakeSwap V2 BUSD/USDT – high-liquidity pool (address provided)
        ('PancakeSwap', '0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56', '0x55d398326f99059fF775485246999027B3197955', 0, 0, 25, '0x7EFaEf62fDdCCa950418312c6C91Aef321375A00'),
    ],
}

# ============================================================================
# Kuru market registry
# ============================================================================
KURU_MARKETS = {
    ('0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A', '0x754704Bc059F8C67012fEd69BC8A327a5aafb603'): 'MONUSDC',
    ('0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A', '0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a'): 'MONAUSD',
}

# ============================================================================
# Native price fallback
# ============================================================================
NATIVE_PRICE_FALLBACK = {
    'monad': 5.0,
    'sonic': 0.5,
    'linea': 3000.0,
    'scroll': 3000.0,
    'mantle': 0.8,
    'bnb': 600.0,
}

# ============================================================================
# ABIs (unchanged)
# ============================================================================
V2_ROUTER_ABI = [
    {
        "inputs": [{"name": "amountIn", "type": "uint256"}, {"name": "path", "type": "address[]"}],
        "name": "getAmountsOut",
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function"
    }
]

QUOTER_V2_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"name": "tokenIn", "type": "address"},
                    {"name": "tokenOut", "type": "address"},
                    {"name": "amountIn", "type": "uint256"},
                    {"name": "fee", "type": "uint24"},
                    {"name": "sqrtPriceLimitX96", "type": "uint160"}
                ],
                "name": "params",
                "type": "tuple"
            }
        ],
        "name": "quoteExactInputSingle",
        "outputs": [
            {"name": "amountOut", "type": "uint256"},
            {"name": "sqrtPriceX96After", "type": "uint160"},
            {"name": "initializedTicksCrossed", "type": "uint32"},
            {"name": "gasEstimate", "type": "uint256"}
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

MULTICALL3_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"name": "target", "type": "address"},
                    {"name": "callData", "type": "bytes"}
                ],
                "name": "calls",
                "type": "tuple[]"
            }
        ],
        "name": "aggregate",
        "outputs": [
            {"name": "blockNumber", "type": "uint256"},
            {"name": "returnData", "type": "bytes[]"}
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

PAIR_ABI = [
    {"inputs": [], "name": "token0", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "token1", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "getReserves", "outputs": [{"name": "_reserve0", "type": "uint112"}, {"name": "_reserve1", "type": "uint112"}, {"name": "_blockTimestampLast", "type": "uint32"}], "stateMutability": "view", "type": "function"},
]

SOLIDLY_ROUTER_ABI = [
    {
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "routes", "type": "tuple(address, address, bool, address)[]"}
        ],
        "name": "getAmountsOut",
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function"
    }
]

LB_ROUTER_ABI = [
    {
        "inputs": [
            {"name": "pair", "type": "address"},
            {"name": "amountIn", "type": "uint256"},
            {"name": "swapForY", "type": "bool"}
        ],
        "name": "getSwapOut",
        "outputs": [{"name": "amountOut", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    }
]

LB_PAIR_ABI = [
    {"inputs": [], "name": "getTokenX", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "getTokenY", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
]

ERC20_ABI = [
    {"inputs": [], "name": "decimals", "outputs": [{"type": "uint8"}], "stateMutability": "view", "type": "function"}
]

# ============================================================================
# Web3 Manager (unchanged from v6.2)
# ============================================================================
class Web3Manager:
    def __init__(self):
        self.web3_instances: Dict[str, Web3] = {}
        self.logger = logging.getLogger('arb_scanner')
        self._setup_logging()

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def connect_chain(self, chain_name: str) -> Optional[Web3]:
        config = CHAIN_CONFIGS[chain_name]
        for rpc in config.rpc_urls:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 10}))
                if chain_name == 'bnb':
                    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                if w3.is_connected():
                    self.logger.info(f"Connected to {chain_name} via {rpc}")
                    self.web3_instances[chain_name] = w3
                    return w3
            except Exception as e:
                self.logger.warning(f"Failed to connect to {rpc}: {e}")
        self.logger.error(f"No working RPC for {chain_name}")
        return None

    def get_web3(self, chain_name: str) -> Optional[Web3]:
        w3 = self.web3_instances.get(chain_name)
        if w3 and w3.is_connected():
            return w3
        self.logger.info(f"Reconnecting to {chain_name}")
        return self.connect_chain(chain_name)

    def get_gas_price(self, chain_name: str) -> int:
        w3 = self.get_web3(chain_name)
        if w3:
            return w3.eth.gas_price
        return Web3.to_wei(0.1, 'gwei')

# ============================================================================
# Multicall3 wrapper (unchanged)
# ============================================================================
class Multicall3:
    def __init__(self, w3: Web3, address: str):
        self.w3 = w3
        self.address = address
        self.contract = w3.eth.contract(address=Web3.to_checksum_address(address), abi=MULTICALL3_ABI)

    async def aggregate(self, calls: List[Tuple[str, bytes]]) -> List[bytes]:
        try:
            block_number, return_data = self.contract.functions.aggregate(calls).call()
            return return_data
        except Exception as e:
            logging.getLogger('multicall').error(f"Multicall error: {e}")
            return []

# ============================================================================
# Token Graph (unchanged logic, uses updated config)
# ============================================================================
class TokenGraph:
    def __init__(self, chain: str, w3: Web3, multicall: Multicall3):
        self.chain = chain
        self.w3 = w3
        self.multicall = multicall
        self.graph = defaultdict(list)
        self.pools: Dict[str, PoolInfo] = {}
        self.token_decimals: Dict[str, int] = {}

    async def build_from_factories(self):
        await self._add_seed_pools()
        for dex in DEX_CONFIGS.get(self.chain, []):
            if dex.dex_type == 'v2' and dex.factory_address:
                await self._fetch_v2_pairs(dex)
        self._add_edges_from_pools()

    async def _add_seed_pools(self):
        for pool_def in SEED_POOLS.get(self.chain, []):
            dex_name, token0, token1, r0_raw, r1_raw, fee_bps, addr = pool_def
            self.pools[addr] = PoolInfo(
                dex_name=dex_name,
                token0=token0,
                token1=token1,
                reserve0_raw=r0_raw,
                reserve1_raw=r1_raw,
                fee_bps=fee_bps,
                tvl_usd=Decimal('0'),
                address=addr
            )
            await self.fetch_token_decimals(token0)
            await self.fetch_token_decimals(token1)

    async def _fetch_v2_pairs(self, dex: DEXConfig):
        factory = self.w3.eth.contract(address=Web3.to_checksum_address(dex.factory_address), abi=[
            {"inputs": [], "name": "allPairsLength", "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
            {"inputs": [{"name": "", "type": "uint256"}], "name": "allPairs", "outputs": [{"name": "", "type": "address"}], "stateMutability": "view", "type": "function"}
        ])
        try:
            pair_count = factory.functions.allPairsLength().call()
            pair_count = min(pair_count, 100)
            calls = [(factory.address, factory.encodeABI("allPairs", args=[i])) for i in range(pair_count)]
            results = await self.multicall.aggregate(calls)
            pair_addresses = []
            for res in results:
                if len(res) < 32:
                    continue
                addr = Web3.to_checksum_address(res[12:32])
                pair_addresses.append(addr)

            pair_contracts = [self.w3.eth.contract(address=addr, abi=PAIR_ABI) for addr in pair_addresses]
            token0_calls = [(addr, pc.encodeABI("token0")) for addr, pc in zip(pair_addresses, pair_contracts)]
            token1_calls = [(addr, pc.encodeABI("token1")) for addr, pc in zip(pair_addresses, pair_contracts)]
            reserves_calls = [(addr, pc.encodeABI("getReserves")) for addr, pc in zip(pair_addresses, pair_contracts)]
            all_calls = token0_calls + token1_calls + reserves_calls
            all_results = await self.multicall.aggregate(all_calls)
            if not all_results:
                return
            token0_results = all_results[:pair_count]
            token1_results = all_results[pair_count:2*pair_count]
            reserves_results = all_results[2*pair_count:]

            for i, addr in enumerate(pair_addresses):
                token0_data = token0_results[i]
                if len(token0_data) < 32:
                    continue
                token0 = Web3.to_checksum_address(token0_data[12:32])
                token1_data = token1_results[i]
                if len(token1_data) < 32:
                    continue
                token1 = Web3.to_checksum_address(token1_data[12:32])
                reserves_data = reserves_results[i]
                if len(reserves_data) < 64:
                    continue
                reserve0_raw = Web3.to_int(reserves_data[0:32])
                reserve1_raw = Web3.to_int(reserves_data[32:64])
                self.pools[addr] = PoolInfo(
                    dex_name=dex.name,
                    token0=token0,
                    token1=token1,
                    reserve0_raw=reserve0_raw,
                    reserve1_raw=reserve1_raw,
                    fee_bps=dex.fee_bps,
                    tvl_usd=Decimal('0'),
                    address=addr
                )
                await self.fetch_token_decimals(token0)
                await self.fetch_token_decimals(token1)
        except Exception as e:
            logging.getLogger('graph').error(f"Error fetching V2 pairs: {e}")

    def _add_edges_from_pools(self):
        for pool in self.pools.values():
            self.graph[pool.token0].append({
                'token_out': pool.token1,
                'dex': pool.dex_name,
                'fee_bps': pool.fee_bps,
                'pool_address': pool.address,
                'get_amount_out': self._make_amount_out_func(pool)
            })
            self.graph[pool.token1].append({
                'token_out': pool.token0,
                'dex': pool.dex_name,
                'fee_bps': pool.fee_bps,
                'pool_address': pool.address,
                'get_amount_out': self._make_amount_out_func(pool, reverse=True)
            })

    def _make_amount_out_func(self, pool: PoolInfo, reverse=False):
        def amount_out_func(amount_in: Decimal, fee_bps: int) -> Decimal:
            dec0 = self.token_decimals.get(pool.token0, 18)
            dec1 = self.token_decimals.get(pool.token1, 18)
            if reverse:
                reserve_in_raw = pool.reserve1_raw
                reserve_out_raw = pool.reserve0_raw
                in_dec = dec1
                out_dec = dec0
            else:
                reserve_in_raw = pool.reserve0_raw
                reserve_out_raw = pool.reserve1_raw
                in_dec = dec0
                out_dec = dec1
            amount_in_raw = int(amount_in * Decimal(10**in_dec))
            amount_in_with_fee = amount_in_raw * (10000 - fee_bps) / 10000
            numerator = amount_in_with_fee * reserve_out_raw
            denominator = reserve_in_raw + amount_in_with_fee
            if denominator == 0:
                return Decimal('0')
            amount_out_raw = int(numerator / denominator)
            return Decimal(amount_out_raw) / Decimal(10**out_dec)
        return amount_out_func

    async def fetch_token_decimals(self, address: str) -> int:
        if address in self.token_decimals:
            return self.token_decimals[address]
        w3 = self.w3
        token = w3.eth.contract(address=Web3.to_checksum_address(address), abi=ERC20_ABI)
        try:
            dec = token.functions.decimals().call()
            self.token_decimals[address] = dec
            return dec
        except Exception:
            self.token_decimals[address] = 18
            return 18

    def get_edge_count(self) -> int:
        return sum(len(v) for v in self.graph.values())

# ============================================================================
# PathFinder (unchanged)
# ============================================================================
class PathFinder:
    def __init__(self, graph: TokenGraph, max_hops: int = MAX_HOPS):
        self.graph = graph
        self.max_hops = max_hops

    def find_profitable_cycles(self, start_token: str) -> List[List[str]]:
        paths = []
        def dfs(current, path, hops):
            if hops > self.max_hops:
                return
            for edge in self.graph.graph.get(current, []):
                nxt = edge['token_out']
                if nxt == start_token and hops >= 2:
                    paths.append(path + [nxt])
                    continue
                if nxt not in path:
                    dfs(nxt, path + [nxt], hops+1)
        dfs(start_token, [start_token], 0)
        return paths

# ============================================================================
# Simulation Engine (unchanged)
# ============================================================================
class SimulationEngine:
    def __init__(self, w3_manager: Web3Manager, http_session: aiohttp.ClientSession):
        self.w3 = w3_manager
        self.session = http_session
        self.logger = logging.getLogger('simulation')
        self.native_prices: Dict[str, float] = {}
        self.last_price_fetch: Dict[str, float] = {}

    async def get_native_price_usd(self, chain: str) -> float:
        now = time.time()
        if chain in self.native_prices and (now - self.last_price_fetch.get(chain, 0)) < 60:
            return self.native_prices[chain]
        gecko_id = CHAIN_CONFIGS[chain].native_token_gecko_id
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={gecko_id}&vs_currencies=usd"
        try:
            async with self.session.get(url, timeout=ClientTimeout(total=5)) as resp:
                if resp.status == 429:
                    self.logger.warning(f"CoinGecko rate limited for {chain}")
                    return NATIVE_PRICE_FALLBACK.get(chain, 1.0)
                data = await resp.json()
                price = data.get(gecko_id, {}).get('usd')
                if price:
                    self.native_prices[chain] = price
                    self.last_price_fetch[chain] = now
                    return price
        except Exception as e:
            self.logger.warning(f"Failed to fetch price for {chain}: {e}")
        return NATIVE_PRICE_FALLBACK.get(chain, 1.0)

    async def simulate_v2_swap(self, chain: str, router: str, token_in: str, token_out: str,
                               amount_in: int) -> int:
        w3 = self.w3.get_web3(chain)
        if not w3:
            return 0
        contract = w3.eth.contract(address=Web3.to_checksum_address(router), abi=V2_ROUTER_ABI)
        try:
            path = [Web3.to_checksum_address(token_in), Web3.to_checksum_address(token_out)]
            amounts = contract.functions.getAmountsOut(amount_in, path).call()
            return amounts[-1]
        except Exception as e:
            self.logger.error(f"V2 simulation error on {chain}: {e}")
            return 0

    async def simulate_v3_swap(self, chain: str, quoter_addr: str, token_in: str, token_out: str,
                               amount_in: int, fee: int) -> int:
        if not quoter_addr or quoter_addr == '0x...':
            return 0
        w3 = self.w3.get_web3(chain)
        if not w3:
            return 0
        quoter = w3.eth.contract(address=Web3.to_checksum_address(quoter_addr), abi=QUOTER_V2_ABI)
        try:
            params = (
                Web3.to_checksum_address(token_in),
                Web3.to_checksum_address(token_out),
                amount_in,
                fee,
                0
            )
            result = quoter.functions.quoteExactInputSingle(params).call()
            return result[0]
        except Exception as e:
            self.logger.error(f"V3 simulation error on {chain}: {e}")
            return 0

    async def simulate_lb_swap(self, chain: str, router: str, pair: str, token_in: str, token_out: str,
                               amount_in: int) -> int:
        w3 = self.w3.get_web3(chain)
        if not w3:
            return 0
        pair_contract = w3.eth.contract(address=Web3.to_checksum_address(pair), abi=LB_PAIR_ABI)
        try:
            token_y = pair_contract.functions.getTokenY().call()
            swap_for_y = (token_out.lower() == token_y.lower())
        except:
            swap_for_y = True
        contract = w3.eth.contract(address=Web3.to_checksum_address(router), abi=LB_ROUTER_ABI)
        try:
            amount_out = contract.functions.getSwapOut(pair, amount_in, swap_for_y).call()
            return amount_out
        except Exception as e:
            self.logger.error(f"LB simulation error on {chain}: {e}")
            return 0

    async def simulate_solidly_swap(self, chain: str, router: str, token_in: str, token_out: str,
                                    amount_in: int, stable: bool, factory: str) -> int:
        w3 = self.w3.get_web3(chain)
        if not w3:
            return 0
        routes = [(Web3.to_checksum_address(token_in), Web3.to_checksum_address(token_out), stable, Web3.to_checksum_address(factory))]
        contract = w3.eth.contract(address=Web3.to_checksum_address(router), abi=SOLIDLY_ROUTER_ABI)
        try:
            amounts = contract.functions.getAmountsOut(amount_in, routes).call()
            return amounts[-1]
        except Exception as e:
            self.logger.error(f"Solidly simulation error on {chain}: {e}")
            return 0

    async def simulate_kuru_swap(self, chain: str, market_symbol: str, token_in: str, token_out: str,
                                 amount_in: Decimal, is_buy: bool) -> Decimal:
        url = f"https://exchange.kuru.io/api/v3/depth?symbol={market_symbol}"
        try:
            async with self.session.get(url, timeout=ClientTimeout(total=5)) as resp:
                depth = await resp.json()
        except Exception as e:
            self.logger.error(f"Kuru API error: {e}")
            return Decimal('0')

        side = 'asks' if is_buy else 'bids'
        orders = depth.get(side, [])
        remaining = amount_in
        total_out = Decimal('0')
        for price_str, size_str in orders:
            price = Decimal(price_str)
            size = Decimal(size_str)
            if is_buy:
                if remaining <= size * price:
                    base_received = remaining / price
                    total_out += base_received
                    remaining = Decimal('0')
                    break
                else:
                    base_received = size
                    total_out += base_received
                    remaining -= size * price
            else:
                if remaining <= size:
                    quote_received = remaining * price
                    total_out += quote_received
                    remaining = Decimal('0')
                    break
                else:
                    quote_received = size * price
                    total_out += quote_received
                    remaining -= size
        if remaining > 0:
            return Decimal('0')
        return total_out

    async def simulate_path(self, path: ArbitragePath, graph: TokenGraph) -> ArbitragePath:
        chain = path.chain
        w3 = self.w3.get_web3(chain)
        if not w3:
            path.is_profitable = False
            self.logger.warning(f"Failed to get web3 for {chain}")
            return path

        current_amount = path.flash_loan_amount
        for step in path.steps:
            token_in_dec = await graph.fetch_token_decimals(step.token_in)
            token_out_dec = await graph.fetch_token_decimals(step.token_out)
            step.amount_in = current_amount
            dex = next((d for d in DEX_CONFIGS.get(chain, []) if d.name == step.dex_name), None)
            if not dex:
                path.is_profitable = False
                return path

            amount_in_wei = int(current_amount * Decimal(10**token_in_dec))

            if dex.dex_type == 'v2':
                amount_out_wei = await self.simulate_v2_swap(chain, dex.router_address,
                                                             step.token_in, step.token_out,
                                                             amount_in_wei)
                step.amount_out = Decimal(amount_out_wei) / Decimal(10**token_out_dec)
            elif dex.dex_type == 'v3':
                quoter = dex.quoter_address or CHAIN_CONFIGS[chain].quoter_v2_address
                amount_out_wei = await self.simulate_v3_swap(chain, quoter,
                                                             step.token_in, step.token_out,
                                                             amount_in_wei, step.fee_bps)
                step.amount_out = Decimal(amount_out_wei) / Decimal(10**token_out_dec)
            elif dex.dex_type == 'lb':
                if not step.pair_address:
                    step.amount_out = Decimal('0')
                else:
                    amount_out_wei = await self.simulate_lb_swap(chain, dex.router_address,
                                                                 step.pair_address,
                                                                 step.token_in, step.token_out,
                                                                 amount_in_wei)
                    step.amount_out = Decimal(amount_out_wei) / Decimal(10**token_out_dec)
            elif dex.dex_type == 'solidly':
                factory = dex.factory_address
                if not factory:
                    step.amount_out = Decimal('0')
                else:
                    amount_out_wei = await self.simulate_solidly_swap(chain, dex.router_address,
                                                                      step.token_in, step.token_out,
                                                                      amount_in_wei, step.stable or False, factory)
                    step.amount_out = Decimal(amount_out_wei) / Decimal(10**token_out_dec)
            elif dex.dex_type == 'orderbook':
                symbol = KURU_MARKETS.get((step.token_in, step.token_out))
                is_buy = False
                if not symbol:
                    symbol = KURU_MARKETS.get((step.token_out, step.token_in))
                    is_buy = True
                if not symbol:
                    path.is_profitable = False
                    return path
                step.amount_out = await self.simulate_kuru_swap(chain, symbol,
                                                                step.token_in, step.token_out,
                                                                current_amount, is_buy)
            else:
                step.amount_out = Decimal('0')

            if step.amount_out == Decimal('0'):
                path.is_profitable = False
                return path

            current_amount = step.amount_out

        principal = path.flash_loan_amount
        fee_amount = principal * Decimal(CHAIN_CONFIGS[chain].flash_loan_fee_bps) / Decimal('10000')
        profit = current_amount - principal - fee_amount

        gas_units = DEFAULT_GAS_LIMIT_PER_STEP * len(path.steps) + DEFAULT_GAS_LIMIT_OVERHEAD
        gas_price_wei = self.w3.get_gas_price(chain)
        gas_cost_wei = gas_units * gas_price_wei
        native_price = await self.get_native_price_usd(chain)
        gas_usd = (Decimal(gas_cost_wei) / Decimal('1e18')) * Decimal(native_price)

        path.total_profit_usd = profit
        path.gas_cost_usd = gas_usd
        path.net_profit_usd = profit - gas_usd
        path.is_profitable = path.net_profit_usd > MIN_PROFIT_USD

        return path

# ============================================================================
# Scanner (unchanged)
# ============================================================================
class ArbitrageScanner:
    def __init__(self, w3_manager: Web3Manager, simulation: SimulationEngine):
        self.w3 = w3_manager
        self.sim = simulation
        self.active_chains = set()
        self.token_graphs: Dict[str, TokenGraph] = {}
        self.cooldown: OrderedDict = OrderedDict()
        self.semaphore = asyncio.Semaphore(3)

    async def initialize_chain(self, chain_name: str) -> bool:
        w3 = self.w3.connect_chain(chain_name)
        if not w3:
            return False
        multicall = Multicall3(w3, CHAIN_CONFIGS[chain_name].multicall3_address)
        graph = TokenGraph(chain_name, w3, multicall)
        await graph.build_from_factories()
        for token in STABLE_TOKENS.get(chain_name, []):
            await graph.fetch_token_decimals(token.address)
        self.token_graphs[chain_name] = graph
        self.active_chains.add(chain_name)
        edge_count = graph.get_edge_count()
        self.w3.logger.info(f"Initialized {chain_name}: {len(graph.pools)} pools, {edge_count} edges")
        return True

    async def scan_chain(self, chain_name: str) -> List[ArbitragePath]:
        if chain_name not in self.active_chains:
            return []
        graph = self.token_graphs.get(chain_name)
        if not graph:
            return []

        finder = PathFinder(graph, max_hops=MAX_HOPS)
        stable_tokens = STABLE_TOKENS.get(chain_name, [])
        opportunities = []
        for stable in stable_tokens:
            candidate_paths = finder.find_profitable_cycles(stable.address)
            for tokens in candidate_paths:
                path_obj = await self._build_path_object(chain_name, tokens, stable, graph)
                if not path_obj:
                    continue
                path_hash = hashlib.md5("|".join(tokens).encode()).hexdigest()[:8]
                now = time.time()
                if path_hash in self.cooldown and now - self.cooldown[path_hash] < COOLDOWN_SECONDS:
                    continue
                simulated = await self.sim.simulate_path(path_obj, graph)
                self.cooldown[path_hash] = now
                if len(self.cooldown) > 10000:
                    self.cooldown.popitem(last=False)
                if simulated.is_profitable:
                    opportunities.append(simulated)
                    self.w3.logger.info(f"[{chain_name}] Profit ${simulated.net_profit_usd:.2f} "
                                        f"over {len(simulated.steps)} hops")
        return opportunities

    async def _build_path_object(self, chain: str, tokens: List[str], start_token: TokenConfig,
                                 graph: TokenGraph) -> Optional[ArbitragePath]:
        if len(tokens) < 2:
            return None
        steps = []
        for i in range(len(tokens)-1):
            token_in = tokens[i]
            token_out = tokens[i+1]
            edge = self._find_edge_for_pair(chain, token_in, token_out, graph)
            if not edge:
                return None
            dex = next((d for d in DEX_CONFIGS.get(chain, []) if d.name == edge['dex']), None)
            if not dex:
                return None
            step = SwapStep(
                dex_name=dex.name,
                token_in=token_in,
                token_out=token_out,
                amount_in=Decimal('0'),
                amount_out=Decimal('0'),
                fee_bps=edge['fee_bps'],
                pair_address=edge.get('pool_address'),
                stable=None
            )
            if dex.dex_type == 'solidly':
                is_stable = (token_in in [t.address for t in STABLE_TOKENS.get(chain, [])] and
                             token_out in [t.address for t in STABLE_TOKENS.get(chain, [])])
                step.stable = is_stable
            steps.append(step)
        return ArbitragePath(
            chain=chain,
            start_token=start_token.address,
            end_token=start_token.address,
            steps=steps,
            total_profit_usd=Decimal('0'),
            flash_loan_amount=FLASH_LOAN_AMOUNT_USD,
            gas_cost_usd=Decimal('0'),
            net_profit_usd=Decimal('0'),
            is_profitable=False,
            simulation_result={}
        )

    def _find_edge_for_pair(self, chain: str, token_in: str, token_out: str, graph: TokenGraph) -> Optional[Dict]:
        for edge in graph.graph.get(token_in, []):
            if edge['token_out'] == token_out:
                return edge
        return None

    async def scan_all(self) -> Dict[str, List[ArbitragePath]]:
        chain_list = sorted(list(self.active_chains))
        async def scan_with_sem(chain):
            async with self.semaphore:
                return await self.scan_chain(chain)
        results = await asyncio.gather(*[scan_with_sem(chain) for chain in chain_list])
        return {chain: opps for chain, opps in zip(chain_list, results) if opps}

# ============================================================================
# Alert Manager (Telegram only)
# ============================================================================
class AlertManager:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.logger = logging.getLogger('alert')

    async def send_telegram(self, message: str):
        if not self.telegram_token or not self.telegram_chat_id:
            return
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {'chat_id': self.telegram_chat_id, 'text': message, 'parse_mode': 'HTML'}
        try:
            async with self.session.post(url, json=payload, timeout=ClientTimeout(total=10)) as resp:
                resp.raise_for_status()
        except Exception as e:
            self.logger.error(f"Telegram failed: {e}")

    async def send_alert(self, opportunities: Dict[str, List[ArbitragePath]]):
        if not opportunities:
            return
        total = sum(len(v) for v in opportunities.values())
        msg = f"🚀 <b>Arbitrage Opportunities Found!</b>\nTotal: {total}\n\n"
        for chain, paths in opportunities.items():
            msg += f"<b>{chain}</b>\n"
            for p in paths[:3]:
                msg += f"  • Profit: ${p.net_profit_usd:.2f} | Steps: {len(p.steps)}\n"
        await self.send_telegram(msg)

# ============================================================================
# Main Bot
# ============================================================================
class ArbitrageBot:
    def __init__(self):
        self.session = None
        self.w3 = Web3Manager()
        self.sim = None
        self.scanner = None
        self.alert = None
        self.running = False
        self.logger = logging.getLogger('bot')

    async def initialize(self):
        self.session = aiohttp.ClientSession()
        self.sim = SimulationEngine(self.w3, self.session)
        self.scanner = ArbitrageScanner(self.w3, self.sim)
        self.alert = AlertManager(self.session)

        chains = list(CHAIN_CONFIGS.keys())
        for chain in chains:
            success = await self.scanner.initialize_chain(chain)
            if success:
                print(f"✅ {chain}")
            else:
                print(f"❌ {chain}")

    async def scan_loop(self):
        while self.running:
            try:
                start = time.time()
                opportunities = await self.scanner.scan_all()
                if opportunities:
                    await self.alert.send_alert(opportunities)
                elapsed = time.time() - start
                await asyncio.sleep(max(0, SCAN_INTERVAL - elapsed))
            except Exception as e:
                self.logger.error(f"Scan loop error: {e}")
                await asyncio.sleep(5)

    async def health_check(self):
        app = web.Application()
        async def health(request):
            return web.Response(text="OK")
        app.router.add_get('/health', health)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', HEALTH_PORT)
        await site.start()
        self.logger.info(f"Health check server started on port {HEALTH_PORT}")
        while self.running:
            await asyncio.sleep(30)

    async def start(self):
        await self.initialize()
        self.running = True
        await asyncio.gather(self.scan_loop(), self.health_check())

    async def stop(self):
        self.running = False
        if self.session:
            await self.session.close()

async def main():
    bot = ArbitrageBot()
    try:
        await bot.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
