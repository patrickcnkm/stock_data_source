from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import pandas as pd

from fastapi import HTTPException

from .settings import get_settings
from .futu_rate_limit import rate_limit

BASE_DIR = Path(__file__).resolve().parent.parent
CACHE_PATH = BASE_DIR / "data" / "hk_universe_cache.json"
SEED_FILE = BASE_DIR / "data" / "hk_universe_default.txt"
CACHE_TTL = timedelta(hours=6)


def _normalize_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    if "." in s and s.startswith("HK."):
        return s
    if s.endswith(".HK"):
        code = s[:-3]
        return f"HK.{code}"
    if s.isdigit() or s.replace(".", "").isdigit():
        return f"HK.{s}"
    return s


def _read_cache() -> List[str] | None:
    if not CACHE_PATH.exists():
        return None
    try:
        data = json.loads(CACHE_PATH.read_text())
        fetched_at = datetime.fromisoformat(data.get("fetched_at"))
        if datetime.now(timezone.utc) - fetched_at > CACHE_TTL:
            return None
        symbols = data.get("symbols") or []
        if not symbols:
            return None
        return [_normalize_symbol(sym) for sym in symbols]
    except Exception:
        return None


def _write_cache(symbols: List[str]) -> None:
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "symbols": symbols,
    }
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(payload, indent=2))


def _fetch_hk_universe_from_futu(settings) -> List[str]:
    # Lazy import to avoid iCloud Drive permission issues
    from futu import Market, OpenQuoteContext, RET_OK, SecurityType
    
    ctx = OpenQuoteContext(host=settings.futu_opend_ip, port=settings.futu_opend_quote_port)
    try:
        rate_limit()  # Rate limit before API call
        ret, df = ctx.get_stock_basicinfo(market=Market.HK, stock_type=SecurityType.STOCK)
        if ret != RET_OK or df is None or df.empty:
            raise RuntimeError("Futu get_stock_basicinfo failed")
        symbols = sorted({_normalize_symbol(code) for code in df["code"].tolist()})
        if not symbols:
            raise RuntimeError("Futu returned empty symbol list")
        _write_cache(symbols)
        return symbols
    finally:
        ctx.close()


def _read_seed_file() -> List[str]:
    if not SEED_FILE.exists():
        return []
    symbols: List[str] = []
    with SEED_FILE.open() as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            symbols.append(_normalize_symbol(line))
    return symbols


def load_hk_universe(force_refresh: bool = False) -> List[str]:
    """
    Return the HK stock universe.

    Precedence:
      1. HK_UNIVERSE_SYMBOLS env var starting with '!' (strict override).
      2. Otherwise, union of HK_UNIVERSE_SYMBOLS (if provided) and fetched universe.
      3. Cached universe is used when fetch fails or refresh is not requested.
    """
    settings = get_settings()
    raw = settings.hk_universe_symbols or ""
    strict = False
    if raw.startswith("!"):
        strict = True
        raw = raw[1:]

    env_symbols = [_normalize_symbol(s) for s in raw.split(",") if s.strip()]
    fetched: List[str] = []

    if not strict:
        if not force_refresh:
            cached = _read_cache()
            if cached:
                fetched = cached
        if not fetched:
            try:
                fetched = _fetch_hk_universe_from_futu(settings)
            except Exception as exc:
                cached = _read_cache()
                if cached:
                    fetched = cached
                else:
                    seed = _read_seed_file()
                    if seed:
                        fetched = seed
                    else:
                        raise HTTPException(
                            status_code=500,
                            detail=f"Failed to load HK universe from Futu ({exc}). "
                            "Set HK_UNIVERSE_SYMBOLS env var (prefixed with '!') to override.",
                        )

    if strict:
        if env_symbols:
            return env_symbols
        raise HTTPException(
            status_code=400,
            detail="HK_UNIVERSE_SYMBOLS is set to strict override ('!') but no symbols were provided.",
        )

    # Merge env overrides with fetched list, preserving order and uniqueness.
    seen = set()
    merged: List[str] = []
    seed = _read_seed_file() if not fetched else []
    for sym in env_symbols + fetched + seed:
        if sym and sym not in seen:
            seen.add(sym)
            merged.append(sym)
    if not merged:
        raise HTTPException(
            status_code=500,
            detail="Unable to determine HK universe symbols and no overrides were supplied.",
        )
    return merged


def load_hk_stocks_from_watchlist() -> List[str]:
    """
    Load HK stocks from Futu OpenD watchlist (favorite stocks).
    
    This function reads watchlist stocks using get_user_security() and filters
    for HK stocks with stock_type = "STOCK", similar to futu_web_viewer.py logic.
    
    Returns:
        List of normalized HK stock symbols (e.g., ["HK.00700", "HK.00005"])
    
    Raises:
        HTTPException: If connection fails or no HK stocks found in watchlist
    """
    # Lazy import to avoid iCloud Drive permission issues
    from futu import OpenQuoteContext, RET_OK
    
    settings = get_settings()
    favorite_stocks = []
    
    ctx = None
    try:
        rate_limit()  # Rate limit before API call
        ctx = OpenQuoteContext(host=settings.futu_opend_ip, port=settings.futu_opend_quote_port)
        
        # Try to get all watchlist groups
        # According to Futu API docs: '全部' (Chinese for "All") = all stocks, 
        # 'HK' = HK stocks, 'US' = US stocks, etc.
        watchlist_groups = ['全部', 'HK', 'US', 'CN']  # Try 全部 (All) first, then specific markets
        
        for group in watchlist_groups:
            try:
                rate_limit()  # Rate limit for each API call
                ret, fav_data = ctx.get_user_security(group)
                if ret == RET_OK:
                    # Handle DataFrame response (standard format)
                    if isinstance(fav_data, pd.DataFrame):
                        if len(fav_data) > 0:
                            for idx, row in fav_data.iterrows():
                                stock_code = row.get('code', '')
                                if pd.notna(stock_code) and stock_code:
                                    # Check if we already have this stock (avoid duplicates)
                                    code_str = str(stock_code).strip()
                                    normalized = _normalize_symbol(code_str)
                                    if normalized and not any(s['code'] == normalized for s in favorite_stocks):
                                        favorite_stocks.append({
                                            'code': normalized,
                                            'name': str(row.get('name', 'N/A')),
                                            'market': group if group != '全部' else 'MIXED',
                                            'sec_type': str(row.get('stock_type', row.get('sec_type', 'STOCK')))
                                        })
                    elif isinstance(fav_data, dict):
                        stock_list = fav_data.get('data', [])
                        if not stock_list and 'code' in fav_data:
                            stock_list = [fav_data]
                        
                        for stock in stock_list:
                            if isinstance(stock, dict):
                                stock_code = stock.get('code', '')
                                if stock_code:
                                    code_str = str(stock_code).strip()
                                    normalized = _normalize_symbol(code_str)
                                    if normalized and not any(s['code'] == normalized for s in favorite_stocks):
                                        favorite_stocks.append({
                                            'code': normalized,
                                            'name': stock.get('name', 'N/A'),
                                            'market': group if group != '全部' else 'MIXED',
                                            'sec_type': stock.get('sec_type', 'STOCK')
                                        })
                    
                    # If we got '全部' group successfully, we don't need to check other groups
                    if group == '全部' and len(favorite_stocks) > 0:
                        break
                        
            except RuntimeError as e:
                # RuntimeError from rate_limit() means quota exceeded - don't mask this error
                # Re-raise it so it propagates to the outer handler
                raise
            except Exception as e:
                # Continue to next group for other errors (network issues, API errors, etc.)
                continue
        
        # Filter to only HK stocks with stock type = "STOCK"
        hk_stocks = []
        for stock in favorite_stocks:
            code = stock.get('code', '')
            market = stock.get('market', '')
            sec_type = stock.get('sec_type', '').upper()
            
            # Check if it's an HK stock: code starts with 'HK.' or market is 'HK'
            is_hk = code.startswith('HK.') or market == 'HK' or (market == 'MIXED' and code.startswith('HK.'))
            
            # Check if stock type is "STOCK"
            is_stock_type = sec_type == 'STOCK'
            
            if is_hk and is_stock_type:
                hk_stocks.append(code)
        
        if not hk_stocks:
            raise HTTPException(
                status_code=400,
                detail=(
                    "No HK stocks (type=STOCK) found in Futu watchlist. "
                    "Please add HK stocks to your watchlist in Futu OpenD."
                ),
            )
        
        # Sort for consistency
        return sorted(hk_stocks)
        
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load HK stocks from Futu watchlist: {exc}",
        )
    finally:
        if ctx:
            ctx.close()

