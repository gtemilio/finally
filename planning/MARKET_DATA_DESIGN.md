# Market Data Backend — Design

Implementation-ready design for the FinAlly market data subsystem: a unified data-source
API with two interchangeable implementations (a GBM price simulator and a Massive/Polygon.io
REST client), a shared thread-safe price cache, and an SSE streaming endpoint.

All code in this document lives under `backend/app/market/`. This design supersedes the
earlier drafts in `planning/archive/` — it incorporates every fix from the market data code
review (`planning/archive/MARKET_DATA_REVIEW.md`), so the snippets here match the final,
as-built architecture.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [File Structure](#2-file-structure)
3. [Data Model — `models.py`](#3-data-model--modelspy)
4. [Price Cache — `cache.py`](#4-price-cache--cachepy)
5. [Unified API — `interface.py`](#5-unified-api--interfacepy)
6. [Seed Prices & Parameters — `seed_prices.py`](#6-seed-prices--parameters--seed_pricespy)
7. [GBM Simulator — `simulator.py`](#7-gbm-simulator--simulatorpy)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client--massive_clientpy)
9. [Factory — `factory.py`](#9-factory--factorypy)
10. [SSE Streaming — `stream.py`](#10-sse-streaming--streampy)
11. [Public Package API — `__init__.py`](#11-public-package-api--__init__py)
12. [FastAPI Lifecycle Integration](#12-fastapi-lifecycle-integration)
13. [Watchlist Coordination](#13-watchlist-coordination)
14. [Testing Strategy](#14-testing-strategy)
15. [Error Handling & Edge Cases](#15-error-handling--edge-cases)
16. [Configuration Reference](#16-configuration-reference)

---

## 1. Architecture Overview

The subsystem is a **strategy pattern** with a **shared cache as the single point of truth**:

```
                    ┌────────────────────────────┐
                    │  MarketDataSource (ABC)     │
                    │  start / stop /             │
                    │  add_ticker / remove_ticker │
                    └────────────┬───────────────┘
              ┌─────────────────┴─────────────────┐
              │                                   │
   ┌──────────▼──────────┐            ┌───────────▼──────────┐
   │ SimulatorDataSource │            │  MassiveDataSource   │
   │ GBM, 500ms ticks    │            │  REST poll, 15s      │
   │ (default)           │            │  (MASSIVE_API_KEY)   │
   └──────────┬──────────┘            └───────────┬──────────┘
              │          writes PriceUpdate       │
              └─────────────────┬─────────────────┘
                     ┌──────────▼──────────┐
                     │     PriceCache      │
                     │ thread-safe, in-mem │
                     │ version counter     │
                     └──────────┬──────────┘
              ┌─────────────────┼─────────────────┐
              │                 │                 │
     ┌────────▼───────┐ ┌───────▼───────┐ ┌───────▼────────┐
     │ SSE endpoint   │ │ Portfolio     │ │ Trade          │
     │ /api/stream/   │ │ valuation     │ │ execution      │
     │ prices         │ │               │ │                │
     └────────────────┘ └───────────────┘ └────────────────┘
```

Core principles:

- **One interface, two implementations.** Downstream code (SSE, portfolio, trades) never
  knows which source is active. The factory picks the implementation from `MASSIVE_API_KEY`.
- **Push into the cache, never pull from the source.** Each source writes to the
  `PriceCache` on its own schedule (simulator: 500ms; Massive: 15s). Consumers read the
  cache at their own cadence. This decouples all timing.
- **Immutable data crossing the boundary.** The only type that leaves the market layer is
  the frozen `PriceUpdate` dataclass.
- **Version-based change detection.** The cache bumps a monotonically increasing version on
  every write; the SSE loop only sends when the version has changed, so a slow Massive poll
  doesn't cause redundant identical pushes.

---

## 2. File Structure

```
backend/
  app/
    __init__.py
    market/
      __init__.py          # Public API re-exports
      models.py            # PriceUpdate frozen dataclass
      cache.py             # PriceCache (thread-safe in-memory store)
      interface.py         # MarketDataSource ABC (the unified API)
      seed_prices.py       # SEED_PRICES, TICKER_PARAMS, correlation constants
      simulator.py         # GBMSimulator (math) + SimulatorDataSource (async wrapper)
      massive_client.py    # MassiveDataSource (REST poller)
      factory.py           # create_market_data_source()
      stream.py            # SSE endpoint factory (FastAPI router)
  tests/
    market/
      test_models.py
      test_cache.py
      test_simulator.py
      test_simulator_source.py
      test_massive.py
      test_factory.py
  pyproject.toml           # uv project; hatchling with packages = ["app"]
```

One responsibility per file. `__init__.py` re-exports the public API so the rest of the
backend imports from `app.market` without reaching into submodules.

### `pyproject.toml` essentials

`massive` is a **core dependency** (not optional/lazy-imported — see review fix #2), and
hatchling needs explicit package discovery (review fix #1):

```toml
[project]
name = "finally-backend"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.32.0",
    "numpy>=2.0.0",
    "massive>=1.0.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["app"]          # REQUIRED — without this, `uv sync` / Docker builds fail

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
```

---

## 3. Data Model — `models.py`

`PriceUpdate` is the single data structure exposed by the market layer.

```python
"""Data models for market data."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

Design decisions:

- **`frozen=True`** — value objects that never mutate are safe to share across async tasks
  and threads without copying or locking.
- **`slots=True`** — many of these are created per second; slots cut per-instance memory.
- **Computed properties** (`change`, `change_percent`, `direction`) derive from `price` and
  `previous_price`, so they can never be stale or inconsistent with the stored fields.
- **`to_dict()`** is the single serialization point used by SSE and any REST responses.

---

## 4. Price Cache — `cache.py`

The cache is the hub between producers (one data source) and consumers (SSE, portfolio,
trades). It must be thread-safe: the Massive client fetches via `asyncio.to_thread()`,
which writes from a real OS thread while readers run on the event loop.

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Automatically computes direction and change from the previous price.
        If this is the first update for the ticker, previous_price == price
        (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Why `threading.Lock`, not `asyncio.Lock`?

The Massive client's synchronous `get_snapshot_all()` call runs inside
`asyncio.to_thread()` — a real OS thread. An `asyncio.Lock` cannot protect against writes
from another thread; `threading.Lock` works correctly from both sync threads and event-loop
coroutines. The critical section is tiny (dict lookup + assignment), so contention is
negligible at this scale.

### Why a version counter?

The SSE loop polls the cache every ~500ms. When Massive is the source, prices only change
every 15s — without change detection the SSE endpoint would push 29 identical payloads
between polls. The counter makes skipping trivial:

```python
last_version = -1
while True:
    if price_cache.version != last_version:
        last_version = price_cache.version
        yield format_sse(price_cache.get_all())
    await asyncio.sleep(0.5)
```

(Reading a single `int` without the lock is atomic on CPython; the property is a read-only
fast path.)

### Memory bound

The cache stores only the *latest* `PriceUpdate` per ticker — O(tickers), no history.
Sparkline history is accumulated client-side from the SSE stream, per the project plan.

---

## 5. Unified API — `interface.py`

The abstract base class every data source implements. This is the contract that makes the
rest of the backend source-agnostic.

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

### Why the source writes to the cache instead of returning prices

A pull API (`await source.get_prices()`) would couple the caller to the source's timing —
SSE would have to know whether it can call every 500ms (simulator) or must throttle to 15s
(Massive free tier). The push model inverts this: each source updates the cache at its
natural rate, and every consumer reads at its own rate. Swapping sources requires zero
changes anywhere else.

---

## 6. Seed Prices & Parameters — `seed_prices.py`

Constants only — no logic, no imports. Shared by the simulator for starting prices, GBM
parameters, and the correlation structure.

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

Note (review fix #5): there is no separate `DEFAULT_CORR` constant — `CROSS_GROUP_CORR`
covers both cross-sector pairs and unknown tickers, since they carry the same value and a
duplicate constant was misleading.

Tickers added dynamically that aren't in `SEED_PRICES` start at a random price in
$50–$300 with `DEFAULT_PARAMS`.

---

## 7. GBM Simulator — `simulator.py`

Two classes: `GBMSimulator` (pure, synchronous math engine) and `SimulatorDataSource`
(the `MarketDataSource` implementation wrapping it in an async loop). Splitting them keeps
the math unit-testable without any async machinery.

### 7.1 The math

Geometric Brownian Motion — the model underlying Black-Scholes. Prices evolve
multiplicatively, can never go negative, and are lognormally distributed:

```
S(t+dt) = S(t) · exp((μ − σ²/2)·dt + σ·√dt·Z)
```

With 500ms ticks expressed as a fraction of a trading year
(252 days × 6.5 h × 3600 s = 5,896,800 s), `dt ≈ 8.48e-8` — sub-cent moves per tick that
accumulate into realistic intraday ranges.

**Correlated moves:** real stocks co-move by sector. Given correlation matrix `C`, compute
its Cholesky factor `L = cholesky(C)`; then for independent standard normals `Z`,
`L @ Z` yields draws with exactly the desired correlation structure.

**Random events:** each ticker has a ~0.1% chance per tick of a sudden ±2–5% shock. With
10 tickers at 2 ticks/sec that's a visible event roughly every 50 seconds — drama without
destabilizing prices.

### 7.2 `GBMSimulator`

```python
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices."""

    # 500ms expressed as a fraction of a trading year
    # 252 trading days * 6.5 hours/day * 3600 seconds/hour = 5,896,800 seconds
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability

        # Per-ticker state
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}

        # Cholesky decomposition of the correlation matrix (for correlated moves)
        self._cholesky: np.ndarray | None = None

        # Initialize all starting tickers (batch: single Cholesky rebuild at the end)
        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        This is the hot path — called every 500ms. Keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        # Generate n independent standard normal draws
        z_independent = np.random.standard_normal(n)

        # Apply Cholesky to get correlated draws
        if self._cholesky is not None:
            z_correlated = self._cholesky @ z_independent
        else:
            z_correlated = z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu = params["mu"]
            sigma = params["sigma"]

            # GBM: S(t+dt) = S(t) * exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event: ~0.1% chance per tick per ticker
            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker,
                    shock_magnitude * 100,
                    "up" if shock_sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        """Current price for a ticker, or None if not tracked."""
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        """Return the list of currently tracked tickers."""
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (for batch initialization)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky decomposition of the ticker correlation matrix.

        Called whenever tickers are added or removed. O(n^2) but n < 50.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Determine correlation between two tickers based on sector grouping.

        Correlation structure:
          - Same tech sector:    0.6
          - Same finance sector: 0.5
          - TSLA with anything:  0.3 (it does its own thing)
          - Cross-sector/other:  0.3
        """
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        # TSLA is in tech set but behaves independently
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR

        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR

        return CROSS_GROUP_CORR
```

Notes:

- `get_tickers()` is public (review fix #4) so `SimulatorDataSource` never reaches into
  private state.
- `DEFAULT_PARAMS` is copied (`dict(DEFAULT_PARAMS)`) when assigned to a ticker so a future
  per-ticker mutation cannot alias the shared default.
- The correlation matrix built from these coefficients is positive definite for any ticker
  mix, so `np.linalg.cholesky` always succeeds.

### 7.3 `SimulatorDataSource`

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(
            tickers=tickers,
            event_probability=self._event_prob,
        )
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            # Seed cache immediately so the ticker has a price right away
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step the simulation, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

Key behaviors:

- **Immediate cache seeding** in both `start()` and `add_ticker()` — the SSE endpoint has
  data on its very first tick and a newly added ticker is tradable instantly.
- **Graceful cancellation** — `stop()` cancels and awaits the task, swallowing
  `CancelledError`, so FastAPI lifespan teardown is clean. Idempotent (`stop()` twice is
  safe).
- **Per-step exception isolation** — one bad tick logs and continues; the feed never dies.

---

## 8. Massive API Client — `massive_client.py`

Polls the Massive (formerly Polygon.io) snapshot endpoint —
`GET /v2/snapshot/locale/us/markets/stocks/tickers` — which returns **all requested tickers
in one API call**. That single-call property is what keeps the free tier (5 req/min) viable
with a 15s poll interval.

Imports are at module level (review fix #2): `massive` is a core dependency declared in
`pyproject.toml`, so there is no lazy-import dance and test patches target real names.

```python
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Do an immediate first poll so the cache has data right away
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # The Massive RESTClient is synchronous — run in a thread to
            # avoid blocking the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    # Massive timestamps are Unix milliseconds → convert to seconds
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(
                        ticker=snap.ticker,
                        price=price,
                        timestamp=timestamp,
                    )
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning(
                        "Skipping snapshot for %s: %s",
                        getattr(snap, "ticker", "???"),
                        e,
                    )
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — the loop will retry on the next interval.
            # Common failures: 401 (bad key), 429 (rate limit), network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### Snapshot response shape (per ticker)

```json
{
  "ticker": "AAPL",
  "day": {
    "open": 129.61, "high": 130.15, "low": 125.07, "close": 125.07,
    "previous_close": 129.61, "change": -4.54, "change_percent": -3.50,
    "volume": 111237700
  },
  "last_trade": { "price": 125.07, "size": 100, "timestamp": 1675190399000 },
  "last_quote": { "bid_price": 125.06, "ask_price": 125.08 }
}
```

We extract `last_trade.price` and `last_trade.timestamp` (milliseconds → seconds). The
cache derives `previous_price`/`direction` from its own prior entry, exactly as it does for
the simulator, so downstream behavior is identical regardless of source.

### Resilience matrix

| Failure | Behavior |
|---|---|
| 401 invalid key | Logged as error; poller keeps retrying (user fixes `.env`, restarts) |
| 429 rate limited | Logged; next poll after `poll_interval` naturally backs off |
| Network timeout | Logged; retried on next cycle |
| Malformed snapshot | That ticker skipped with a warning; others still processed |
| Total outage | Cache retains last-known prices; SSE streams stale-but-present data |

---

## 9. Factory — `factory.py`

Environment-driven selection, imports at module level:

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

`.strip()` means a whitespace-only key falls back to the simulator instead of producing a
poller that 401s forever. The factory returns an **unstarted** source so the caller controls
when the background task begins (inside the FastAPI lifespan, after the watchlist is loaded
from SQLite).

---

## 10. SSE Streaming — `stream.py`

A FastAPI router factory. The generator is annotated `AsyncGenerator[str, None]`
(review fix #3).

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE streaming router with a reference to the price cache.

    This factory pattern lets us inject the PriceCache without globals.
    Call once during app startup.
    """

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Streams all tracked ticker prices every ~500ms. The client connects
        with EventSource and receives events in the format:

            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Sends all prices every `interval` seconds when the cache has changed.
    Stops when the client disconnects (detected via request.is_disconnected()).
    """
    # Tell the client to retry after 1 second if the connection drops
    yield "retry: 1000\n\n"

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            # Check for client disconnect
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()

                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    payload = json.dumps(data)
                    yield f"data: {payload}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### Wire format

Each event is a single JSON object keyed by ticker:

```
data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{...}}

```

Frontend consumption:

```javascript
const es = new EventSource('/api/stream/prices');
es.onmessage = (event) => {
  const prices = JSON.parse(event.data);   // { AAPL: {...}, GOOGL: {...}, ... }
  // flash green/red per `direction`, append to sparkline buffers
};
// EventSource reconnects automatically per the `retry: 1000` directive
```

### Design notes

- **Poll-and-push, not event-driven.** The loop reads the cache on a fixed 500ms cadence
  rather than being notified by producers. Evenly spaced updates make clean, regularly
  sampled sparklines on the frontend, and the version check makes idle polls free.
- **Full snapshot per event, not deltas.** With ≤ a few dozen tickers the payload is tiny,
  and a full snapshot means a client that reconnects (or misses events) is instantly
  consistent with no replay protocol.
- **Disconnect handling** via `request.is_disconnected()` plus `CancelledError` handling
  covers both graceful client close and server-side teardown.

---

## 11. Public Package API — `__init__.py`

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate         - Immutable price snapshot dataclass
    PriceCache          - Thread-safe in-memory price store
    MarketDataSource    - Abstract interface for data providers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router - FastAPI router factory for SSE endpoint
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

Downstream usage in one glance:

```python
from app.market import PriceCache, create_market_data_source

cache = PriceCache()
source = create_market_data_source(cache)   # Reads MASSIVE_API_KEY
await source.start(["AAPL", "GOOGL", "MSFT"])

price = cache.get_price("AAPL")             # float | None
update = cache.get("AAPL")                  # PriceUpdate | None
everything = cache.get_all()                # dict[str, PriceUpdate]

await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")
await source.stop()
```

---

## 12. FastAPI Lifecycle Integration

The subsystem starts and stops with the app via the `lifespan` context manager.

**In `backend/app/main.py`:**

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import PriceCache, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown of background services."""
    # --- STARTUP ---
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    source = create_market_data_source(price_cache)
    app.state.market_source = source

    # Initial tickers come from the SQLite watchlist (seeded with the 10 defaults)
    initial_tickers = await load_watchlist_tickers()
    await source.start(initial_tickers)

    app.include_router(create_stream_router(price_cache))

    yield  # App is running

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


def get_price_cache() -> PriceCache:
    return app.state.price_cache


def get_market_source() -> MarketDataSource:
    return app.state.market_source
```

Other routes access market data via dependency injection:

```python
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter(prefix="/api")


@router.post("/portfolio/trade")
async def execute_trade(
    trade: TradeRequest,
    price_cache: PriceCache = Depends(get_price_cache),
):
    current_price = price_cache.get_price(trade.ticker)
    if current_price is None:
        raise HTTPException(
            status_code=400,
            detail=f"Price not yet available for {trade.ticker}. Try again in a moment.",
        )
    # ... execute at current_price, instant fill, no fees ...
```

---

## 13. Watchlist Coordination

The data source must track the union of (watchlist tickers) ∪ (tickers with open
positions), so portfolio valuation stays live even for de-watched holdings.

**Add flow** (`POST /api/watchlist {ticker}` — manual or via LLM chat):

```
insert into watchlist table → await source.add_ticker(ticker)
  Simulator: adds to GBMSimulator, rebuilds Cholesky, seeds cache immediately
  Massive:   appends to poll list; price appears on next poll (≤15s)
```

**Remove flow** (`DELETE /api/watchlist/{ticker}`):

```python
@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    await db.delete_watchlist_entry(ticker)

    # Keep streaming prices for tickers the user still holds
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)

    return {"status": "ok"}
```

Symmetrically, when a position is fully sold and the ticker is no longer on the watchlist,
the trade route may call `remove_ticker()` to stop tracking it.

---

## 14. Testing Strategy

Tests live in `backend/tests/market/`, run with `pytest` + `pytest-asyncio`
(`asyncio_mode = "auto"`). Because `massive` is a core dependency, its names exist at
module level and mocks patch real targets — no `create=True` hacks.

### 14.1 `GBMSimulator` (pure math, no async)

```python
from app.market.simulator import GBMSimulator
from app.market.seed_prices import SEED_PRICES


class TestGBMSimulator:
    def test_initial_prices_match_seeds(self):
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim.get_price("AAPL") == SEED_PRICES["AAPL"]

    def test_prices_are_positive(self):
        """GBM prices can never go negative (exp() is always positive)."""
        sim = GBMSimulator(tickers=["AAPL"])
        for _ in range(10_000):
            assert sim.step()["AAPL"] > 0

    def test_step_returns_all_tickers(self):
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        assert set(sim.step().keys()) == {"AAPL", "GOOGL"}

    def test_add_remove_ticker(self):
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        sim.add_ticker("TSLA")
        sim.remove_ticker("GOOGL")
        result = sim.step()
        assert "TSLA" in result and "GOOGL" not in result

    def test_full_default_watchlist_cholesky(self):
        """Correlation matrix for all 10 defaults must be positive definite."""
        sim = GBMSimulator(tickers=list(SEED_PRICES.keys()))
        assert sim.step()  # Cholesky succeeded, step produces prices

    def test_unknown_ticker_gets_random_seed_price(self):
        sim = GBMSimulator(tickers=["ZZZZ"])
        assert 50.0 <= sim.get_price("ZZZZ") <= 300.0
```

### 14.2 `PriceCache`

```python
from app.market.cache import PriceCache


class TestPriceCache:
    def test_first_update_is_flat(self):
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.direction == "flat"
        assert update.previous_price == 190.50

    def test_direction_and_change(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 191.00)
        assert update.direction == "up"
        assert update.change == 1.00

    def test_version_increments(self):
        cache = PriceCache()
        v0 = cache.version
        cache.update("AAPL", 190.00)
        cache.update("AAPL", 191.00)
        assert cache.version == v0 + 2

    def test_remove(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.remove("AAPL")
        assert cache.get("AAPL") is None
```

### 14.3 `SimulatorDataSource` (async integration)

```python
import asyncio

from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource


class TestSimulatorDataSource:
    async def test_start_seeds_cache_immediately(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "GOOGL"])
        assert cache.get("AAPL") is not None   # Before the first loop tick
        await source.stop()

    async def test_add_and_remove_ticker(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])

        await source.add_ticker("TSLA")
        assert "TSLA" in source.get_tickers()
        assert cache.get("TSLA") is not None

        await source.remove_ticker("TSLA")
        assert "TSLA" not in source.get_tickers()
        assert cache.get("TSLA") is None
        await source.stop()

    async def test_double_stop_is_safe(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])
        await source.stop()
        await source.stop()  # Must not raise
```

### 14.4 `MassiveDataSource` (mocked — no network, no key)

```python
from unittest.mock import MagicMock, patch

from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource


def _make_snapshot(ticker: str, price: float, timestamp_ms: int) -> MagicMock:
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade.price = price
    snap.last_trade.timestamp = timestamp_ms
    return snap


class TestMassiveDataSource:
    async def test_poll_updates_cache_with_second_timestamps(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL"]
        source._client = MagicMock()  # Satisfy the _poll_once guard

        snaps = [_make_snapshot("AAPL", 190.50, 1707580800000)]
        with patch.object(source, "_fetch_snapshots", return_value=snaps):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get("AAPL").timestamp == 1707580800.0  # ms → s

    async def test_malformed_snapshot_skipped(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL", "BAD"]
        source._client = MagicMock()

        bad = MagicMock()
        bad.ticker = "BAD"
        bad.last_trade = None  # AttributeError on .price
        snaps = [_make_snapshot("AAPL", 190.50, 1707580800000), bad]

        with patch.object(source, "_fetch_snapshots", return_value=snaps):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("BAD") is None

    async def test_api_error_does_not_crash(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL"]
        source._client = MagicMock()

        with patch.object(source, "_fetch_snapshots", side_effect=Exception("network")):
            await source._poll_once()  # Must not raise

        assert cache.get_price("AAPL") is None
```

### 14.5 Factory

```python
import os
from unittest.mock import patch

from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource


def test_no_key_selects_simulator():
    with patch.dict(os.environ, {"MASSIVE_API_KEY": ""}):
        assert isinstance(create_market_data_source(PriceCache()), SimulatorDataSource)


def test_whitespace_key_selects_simulator():
    with patch.dict(os.environ, {"MASSIVE_API_KEY": "   "}):
        assert isinstance(create_market_data_source(PriceCache()), SimulatorDataSource)


def test_key_selects_massive():
    with patch.dict(os.environ, {"MASSIVE_API_KEY": "real-key"}):
        assert isinstance(create_market_data_source(PriceCache()), MassiveDataSource)
```

Coverage expectations: `models`/`cache`/`interface`/`seed_prices`/`factory` ≈ 100%;
`simulator` ≈ 98%; `massive_client` lower (real API paths mocked); `stream` exercised via
E2E tests rather than unit tests (an ASGI-level SSE test with `httpx.AsyncClient` is a
worthwhile addition).

---

## 15. Error Handling & Edge Cases

| Scenario | Handling |
|---|---|
| **Empty watchlist at startup** | Both sources accept `start([])`; simulator's `step()` returns `{}`, Massive skips the API call. SSE sends nothing until a ticker is added. |
| **Trade on a ticker with no cached price** | Route returns HTTP 400 with "price not yet available". Simulator makes this nearly impossible (cache seeded on `add_ticker`); Massive has a ≤15s window after adding a ticker. |
| **Invalid `MASSIVE_API_KEY`** | Every poll 401s and logs; SSE stays connected but streams no data. Fix the key and restart — the factory decision is startup-time only. |
| **Massive outage mid-session** | Cache retains last-known prices; SSE keeps streaming them (stale beats blank). Recovery is automatic on the next successful poll. |
| **Duplicate add / missing remove** | All `add_ticker`/`remove_ticker` implementations are no-ops for duplicates/absents — idempotent by contract. |
| **App shutdown** | Lifespan calls `source.stop()`, which cancels and awaits the background task; SSE generators exit via `CancelledError`. |
| **Numerical safety** | GBM is multiplicative (`exp()` > 0) so prices can't go negative or hit zero; per-tick rounding to 2 decimals; Cholesky always succeeds for the fixed correlation structure. |
| **Thread safety** | All cache mutations under `threading.Lock`; the version read is a single-int atomic read on CPython. Contention at 10 tickers × 2 writes/s is negligible. |

---

## 16. Configuration Reference

| Parameter | Location | Default | Description |
|---|---|---|---|
| `MASSIVE_API_KEY` | Environment | `""` | Non-empty → Massive API; empty/absent → simulator |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` s | Simulator tick cadence |
| `event_probability` | `SimulatorDataSource` / `GBMSimulator` | `0.001` | Shock chance per ticker per tick (±2–5%) |
| `dt` | `GBMSimulator.__init__` | `~8.48e-8` | GBM time step (0.5s as fraction of trading year) |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` s | Massive poll cadence (free-tier safe; lower on paid tiers) |
| SSE push interval | `_generate_events()` | `0.5` s | Cache-poll cadence for connected clients |
| SSE retry directive | `_generate_events()` | `1000` ms | Browser EventSource reconnect delay |
| GBM `mu` / `sigma` per ticker | `seed_prices.TICKER_PARAMS` | see §6 | Annualized drift / volatility |
| Correlations | `seed_prices.py` | tech 0.6, finance 0.5, other 0.3 | Sector co-movement structure |
