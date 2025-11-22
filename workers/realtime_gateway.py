import os, argparse, time, redis
from dotenv import load_dotenv
load_dotenv()

FUTU_OPEND_IP = os.getenv("FUTU_OPEND_IP", "127.0.0.1")
FUTU_OPEND_QUOTE_PORT = int(os.getenv("FUTU_OPEND_QUOTE_PORT", "11111"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
r = redis.Redis.from_url(REDIS_URL)

TICKS_PREFIX = "bus:ticks:"

from futu import OpenQuoteContext, TickerHandlerBase, SubType, RET_OK

class TickerHandler(TickerHandlerBase):
    def __init__(self, symbols):
        super().__init__(); self.symbols=set(symbols)
    def on_recv_rsp(self, rsp_str):
        ret, data = self.on_recv_rsp_ticker(rsp_str)
        if ret != RET_OK or data is None: return RET_OK, data
        for _, row in data.iterrows():
            code = row.get('code')
            if code and code in self.symbols:
                stream = f"{TICKS_PREFIX}{code}"
                payload = {
                    "ts_ex": str(row.get('time', '')),
                    "price": str(row.get('price', '')),
                    "size": str(int(row.get('volume', 0) or 0)),
                    "bid": str(row.get('bid', '')),
                    "ask": str(row.get('ask', ''))
                }
                r.xadd(stream, {k:str(v) for k,v in payload.items()},
                       maxlen=10000, approximate=True)
        return RET_OK, data

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", required=True)
    ap.add_argument("--with-orderbook", action="store_true")
    args = ap.parse_args()

    ctx = OpenQuoteContext(host=FUTU_OPEND_IP, port=FUTU_OPEND_QUOTE_PORT)
    ctx.set_handler(TickerHandler(args.symbols))
    subtypes = [SubType.TICKER]
    if args.with_orderbook:
        subtypes.append(SubType.ORDER_BOOK)
    ret, msg = ctx.subscribe(args.symbols, subtypes, subscribe_push=True)
    if ret != RET_OK: raise SystemExit(f"subscribe failed: {msg}")
    print("[gateway] started. Ctrl+C to stop.")
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        try: ctx.unsubscribe(args.symbols, subtypes)
        except Exception: pass
        ctx.close()

if __name__ == "__main__": main()
