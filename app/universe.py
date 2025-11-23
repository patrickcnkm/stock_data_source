from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

from fastapi import HTTPException
from futu import Market, OpenQuoteContext, RET_OK, SecurityType

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

