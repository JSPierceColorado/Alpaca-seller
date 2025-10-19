import os
import time
import logging
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from typing import List, Optional

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest

# -----------------------
# Config
# -----------------------
API_KEY = os.environ.get("APCA_API_KEY_ID")
API_SECRET = os.environ.get("APCA_API_SECRET_KEY")
PAPER = os.environ.get("APCA_PAPER", "true").lower() in ("1", "true", "yes")
RUN_EVERY_SECONDS = int(os.environ.get("RUN_EVERY_SECONDS", "3600"))  # check once per hour by default
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "0.05"))    # 5% default

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


# -----------------------
# Core loop
# -----------------------

def run_once():
    if not API_KEY or not API_SECRET:
        raise RuntimeError("APCA_API_KEY_ID and APCA_API_SECRET_KEY must be set.")

    trading = TradingClient(API_KEY, API_SECRET, paper=PAPER)

    if not is_trading_hours(trading):
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
