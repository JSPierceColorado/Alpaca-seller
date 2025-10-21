import os
import time
import logging
from datetime import datetime, timedelta, timezone, time as dtime
from zoneinfo import ZoneInfo
from typing import List, Optional

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest

# NEW: minimal imports to fetch SPY bars and compute MAs
import pandas as pd
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

# -----------------------
# Config
# -----------------------
API_KEY = os.environ.get("APCA_API_KEY_ID")
API_SECRET = os.environ.get("APCA_API_SECRET_KEY")
PAPER = os.environ.get("APCA_PAPER", "true").lower() in ("1", "true", "yes")
RUN_EVERY_SECONDS = int(os.environ.get("RUN_EVERY_SECONDS", "3600"))  # check once per hour by default
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "0.05"))    # 5% default

# NEW: bars needed to compute 15m MA240 safely
BARS_NEEDED = int(os.environ.get("BARS_NEEDED", "300"))

# NEW: choose data feed, default to IEX for free/paper accounts; set to "sip" if you have access
DATA_FEED = os.environ.get("ALPACA_DATA_FEED", "iex").lower()

ET_TZ = ZoneInfo("America/New_York")

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("take-profit-bot")

# -----------------------
# Helpers
# -----------------------

def is_trading_hours(trading: TradingClient) -> bool:
    """Return True only during Mon–Thu, 09:30–16:00 ET, and when Alpaca says market is open."""
    now_et = datetime.now(ET_TZ)
    # Mon=0 ... Sun=6, so Mon–Thu = 0–3
    if now_et.weekday() not in (0, 1, 2, 3):
        logger.info("Outside Monday–Thursday; skipping this cycle.")
        return False
    # Check Alpaca market clock
    try:
        clock = trading.get_clock()
        if not clock.is_open:
            logger.info(f"Market closed (next open: {clock.next_open}). Skipping.")
            return False
    except Exception as e:
        logger.warning(f"Clock check failed: {e}. Falling back to time window only.")
    t = now_et.time()
    if not (dtime(9, 30) <= t < dtime(16, 0)):
        logger.info("Outside regular hours 09:30–16:00 ET; skipping this cycle.")
        return False
    return True


def get_positions(trading: TradingClient):
    try:
        return trading.get_all_positions()
    except Exception as e:
        logger.error(f"Failed to fetch positions: {e}")
        return []


def parse_float(val) -> Optional[float]:
    try:
        return float(val)
    except Exception:
        return None


def position_gain_pct(pos) -> Optional[float]:
    plpc = parse_float(getattr(pos, "unrealized_plpc", None))
    if plpc is not None:
        return plpc
    current_price = parse_float(getattr(pos, "current_price", None))
    avg_entry = parse_float(getattr(pos, "avg_entry_price", None))
    if current_price is not None and avg_entry and avg_entry > 0:
        return (current_price - avg_entry) / avg_entry
    return None


def sell_all(trading: TradingClient, symbol: str, qty_str: str):
    try:
        order = MarketOrderRequest(
            symbol=symbol,
            qty=qty_str,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
        )
        submitted = trading.submit_order(order)
        logger.info(f"SELL ALL {symbol}: qty={qty_str} — order id {submitted.id}")
    except Exception as e:
        logger.error(f"Failed to submit SELL for {symbol}: {e}")


# NEW --------------------
# Broad market gate: allow sells only if SPY 15m MA60 > MA240
def spy_uptrend_gate(data_client: StockHistoricalDataClient) -> bool:
    """
    Returns True only if SPY's 15m MA60 > MA240 at the latest bar.
    If data is missing/insufficient, returns False (block sells).
    Tries the configured DATA_FEED first (default 'iex'), then falls back to 'iex' if needed.
    """
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=15 * (BARS_NEEDED + 5))

        base_req = dict(
            symbol_or_symbols="SPY",
            timeframe=TimeFrame(15, TimeFrameUnit.Minute),
            start=start,
            end=end,
            limit=BARS_NEEDED,
        )

        # try configured feed first, then fall back to iex if different
        try_feeds: List[str] = [DATA_FEED]
        if DATA_FEED != "iex":
            try_feeds.append("iex")

        sb = []
        last_err = None
        for feed in try_feeds:
            try:
                req = StockBarsRequest(**base_req, feed=feed)
                bars = data_client.get_stock_bars(req)
                sb = bars.data.get("SPY", [])
                if sb:
                    logger.info(f"Fetched {len(sb)} SPY 15m bars via feed='{feed}'.")
                    break
                else:
                    logger.warning(f"No bars returned from feed='{feed}'.")
            except Exception as e:
                last_err = e
                logger.warning(f"Fetching SPY via feed='{feed}' failed: {e}")

        if not sb:
            if last_err:
                logger.warning(f"All feeds failed; blocking sells this cycle. Last error: {last_err}")
            else:
                logger.info("No bars returned; blocking sells this cycle.")
            return False

        if len(sb) < 240:
            logger.info("SPY has insufficient 15m bars for MA240; blocking sells this cycle.")
            return False

        closes = pd.Series([float(b.close) for b in sb], index=[b.timestamp for b in sb])
        ma60  = closes.rolling(window=60,  min_periods=60).mean().iloc[-1]
        ma240 = closes.rolling(window=240, min_periods=240).mean().iloc[-1]
        if pd.isna(ma60) or pd.isna(ma240):
            logger.info("SPY MA values not ready; blocking sells this cycle.")
            return False

        ok = ma60 > ma240
        logger.info(f"SPY market gate (sell): MA60={ma60:.4f} vs MA240={ma240:.4f} -> {'ALLOW SELLS' if ok else 'BLOCK SELLS'}")
        return ok
    except Exception as e:
        logger.warning(f"SPY uptrend gate check failed ({e}); blocking sells this cycle.")
        return False
# -----------------------


# -----------------------
# Core loop
# -----------------------

def run_once():
    if not API_KEY or not API_SECRET:
        raise RuntimeError("APCA_API_KEY_ID and APCA_API_SECRET_KEY must be set.")

    trading = TradingClient(API_KEY, API_SECRET, paper=PAPER)
    data_client = StockHistoricalDataClient(API_KEY, API_SECRET)  # NEW

    if not is_trading_hours(trading):
        return

    # NEW: check broad market before selling
    if not spy_uptrend_gate(data_client):
        logger.info("Market gate not satisfied (SPY MA60 <= MA240). Skipping all sells this cycle.")
        return

    positions = get_positions(trading)
    if not positions:
        logger.info("No positions to evaluate.")
        return

    logger.info(f"Evaluating {len(positions)} open positions for take-profit ≥ {TAKE_PROFIT_PCT*100:.2f}%...")

    for pos in positions:
        if getattr(pos, "side", "").lower() != "long":
            continue
        symbol = getattr(pos, "symbol", "?")
        qty_str = getattr(pos, "qty", None)
        pct = position_gain_pct(pos)
        if pct is None:
            logger.info(f"{symbol}: unable to compute P/L pct. Skipping.")
            continue
        logger.info(
            f"{symbol}: gain={pct*100:.2f}% (target {TAKE_PROFIT_PCT*100:.2f}%) | avg={getattr(pos, 'avg_entry_price', '?')} curr={getattr(pos, 'current_price', '?')} qty={qty_str}"
        )
        if pct >= TAKE_PROFIT_PCT and qty_str:
            sell_all(trading, symbol, qty_str)
        else:
            logger.info(f"HOLD {symbol}")


def main():
    logger.info("Starting 5% take-profit loop (Mon–Thu 09:30–16:00 ET)...")
    while True:
        try:
            run_once()
        except Exception:
            logger.exception("Cycle error")
        finally:
            time.sleep(RUN_EVERY_SECONDS)


if __name__ == "__main__":
    main()
