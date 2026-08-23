"""
바이낸스 실시간 웹소켓 기반 RSI(14) 모니터링 -> 텔레그램 즉시 알림
- 완전 무료 (바이낸스 공개 웹소켓/REST는 인증 불필요)
- GitHub Actions에서 6시간마다 새로 시작되며, 매 실행은 최대 약 5시간 40분 동안
  웹소켓에 연결된 채로 실시간 체결가를 받아 RSI를 즉시 재계산함 (초 단위 반응)
- RSI가 코인별 기준값을 "새로 돌파"할 때만 알림 (쿨다운으로 스팸 방지)
- 코인마다 Short/Long 조건 방향(이상/이하)을 독립적으로 설정 가능
  (예: 추세추종형 - Short는 낮은RSI, Long은 높은RSI / 역추세형 - 그 반대도 가능)
- 스팟(spot)과 선물(futures) 마켓을 동시에 지원 (QQQUSDT 같은 선물 전용 심볼 대응)
- 반드시 Public 저장소에서 사용할 것 (Private 무료 실행시간으로는 하루도 못 버팀)
"""

import os
import json
import time
import asyncio
from pathlib import Path

import requests
import websockets

STATE_FILE = Path("rsi_state.json")

INTERVAL = "1h"
RSI_PERIOD = 14
HISTORY_SIZE = 200
ALERT_COOLDOWN_SEC = 180  # 같은 코인이 같은 구간을 반복 돌파해도 이 시간(초) 내엔 재알림 안 함
MAX_RUNTIME_SEC = int(os.environ.get("MAX_RUNTIME_SEC", 5 * 3600 + 40 * 60))  # 기본 5시간40분

# 마켓별 접속 주소 (스팟은 지역차단 우회용 공개미러, 선물은 공식 주소)
MARKET_ENDPOINTS = {
    "spot": {
        "rest": "https://data-api.binance.vision/api/v3/klines",
        "ws": "wss://data-stream.binance.vision/stream",
    },
    "futures": {
        "rest": "https://fapi.binance.com/fapi/v1/klines",
        "ws": "wss://fstream.binance.com/stream",
    },
}

# 코인별 설정: market(spot/futures), short 조건(기준값, 방향), long 조건(기준값, 방향)
# 방향은 ">=" (이상) 또는 "<=" (이하)
SYMBOL_CONFIG = {
    "BTCUSDT": {"market": "spot", "short": (17, "<="), "long": (70, ">=")},
    "ETHUSDT": {"market": "spot", "short": (25.5, "<="), "long": (70.5, ">=")},
    "BNBUSDT": {"market": "spot", "short": (9, "<="), "long": (83, ">=")},
    "SOLUSDT": {"market": "spot", "short": (14, "<="), "long": (86.5, ">=")},
    "XRPUSDT": {"market": "spot", "short": (27, "<="), "long": (93.5, ">=")},
}
DEFAULT_CONFIG = {"market": "spot", "short": (30, "<="), "long": (70, ">=")}

SYMBOLS = [s.strip().upper() for s in os.environ.get("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]


def get_config(symbol: str) -> dict:
    return SYMBOL_CONFIG.get(symbol, DEFAULT_CONFIG)


def _compare(value: float, threshold: float, op: str) -> bool:
    return value >= threshold if op == ">=" else value <= threshold


def fetch_history(symbol: str, limit: int = HISTORY_SIZE):
    """시작 시점에 과거 종가를 REST로 한 번 채워둠 (RSI 계산 기준선 확보)."""
    market = get_config(symbol)["market"]
    rest_url = MARKET_ENDPOINTS[market]["rest"]
    params = {"symbol": symbol, "interval": INTERVAL, "limit": limit}
    resp = requests.get(rest_url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    closed = [float(c[4]) for c in data[:-1]]  # 마지막 하나는 진행 중인 캔들이라 제외
    current = float(data[-1][4])
    return closed, current


def calc_rsi(closes):
    period = RSI_PERIOD
    if len(closes) < period + 1:
        raise ValueError("RSI 계산에 필요한 데이터 부족")
    gains, losses = [], []
    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
    except Exception as e:
        print(f"텔레그램 전송 실패: {e}")


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))


def zone_of(rsi: float, symbol: str) -> str:
    cfg = get_config(symbol)
    short_val, short_op = cfg["short"]
    long_val, long_op = cfg["long"]
    if _compare(rsi, short_val, short_op):
        return "short"
    if _compare(rsi, long_val, long_op):
        return "long"
    return "neutral"


def _op_label(op: str) -> str:
    return "이상" if op == ">=" else "이하"


class SymbolTracker:
    def __init__(self, symbol, closed_history, current_close, prev_zone):
        self.symbol = symbol
        self.closed_history = closed_history
        self.current_close = current_close
        self.zone = prev_zone
        self.last_alert_ts = 0.0

    def compute_rsi(self) -> float:
        series = (self.closed_history + [self.current_close])[-HISTORY_SIZE:]
        return calc_rsi(series)

    def on_price_update(self, close_price: float, candle_closed: bool) -> None:
        self.current_close = close_price
        if candle_closed:
            self.closed_history.append(close_price)
            self.closed_history = self.closed_history[-HISTORY_SIZE:]

    def check_and_alert(self) -> float:
        rsi = self.compute_rsi()
        new_zone = zone_of(rsi, self.symbol)
        now = time.time()

        if new_zone != self.zone and new_zone != "neutral":
            if now - self.last_alert_ts >= ALERT_COOLDOWN_SEC:
                cfg = get_config(self.symbol)
                if new_zone == "long":
                    val, op = cfg["long"]
                    msg = f"⚡ {self.symbol} 1H RSI(14) = {rsi:.1f}\nLong 신호 (RSI {val} {_op_label(op)}) 실시간 돌파"
                else:
                    val, op = cfg["short"]
                    msg = f"⚡ {self.symbol} 1H RSI(14) = {rsi:.1f}\nShort 신호 (RSI {val} {_op_label(op)}) 실시간 돌파"
                send_telegram(msg)
                self.last_alert_ts = now
                print(f"[{self.symbol}] 알림 전송: RSI={rsi:.1f}, {self.zone} -> {new_zone}")

        self.zone = new_zone
        return rsi


async def market_loop(market: str, symbols: list, trackers: dict, start_time: float):
    """하나의 마켓(spot 또는 futures)에 대해 웹소켓 하나로 여러 심볼을 동시에 수신."""
    if not symbols:
        return

    ws_base = MARKET_ENDPOINTS[market]["ws"]
    stream_names = "/".join(f"{s.lower()}@kline_{INTERVAL}" for s in symbols)
    ws_url = f"{ws_base}?streams={stream_names}"

    last_save = time.time()
    print(f"[{market}] 웹소켓 연결 시작 ({', '.join(symbols)})")

    while time.time() - start_time < MAX_RUNTIME_SEC:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                print(f"[{market}] 웹소켓 연결 성공, 실시간 수신 시작")
                while time.time() - start_time < MAX_RUNTIME_SEC:
                    raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    payload = json.loads(raw)
                    k = payload.get("data", {}).get("k", {})
                    if not k:
                        continue

                    symbol = payload.get("data", {}).get("s")
                    tracker = trackers.get(symbol)
                    if tracker is None:
                        continue

                    tracker.on_price_update(float(k["c"]), bool(k["x"]))
                    tracker.check_and_alert()

                    if time.time() - last_save > 30:
                        save_state({s: t.zone for s, t in trackers.items()})
                        last_save = time.time()

        except (websockets.ConnectionClosed, asyncio.TimeoutError, OSError) as e:
            print(f"[{market}] 웹소켓 연결 끊김, 5초 후 재연결 시도: {e}")
            await asyncio.sleep(5)
            continue


async def run() -> None:
    state = load_state()
    trackers = {}
    spot_symbols, futures_symbols = [], []

    print("초기 히스토리 로딩 중...")
    for symbol in SYMBOLS:
        cfg = get_config(symbol)
        try:
            closed, current = fetch_history(symbol)
        except Exception as e:
            print(f"[{symbol}] 초기 데이터 조회 실패, 이번 실행에서 제외: {e}")
            continue

        prev_zone = state.get(symbol, "neutral")
        trackers[symbol] = SymbolTracker(symbol, closed, current, prev_zone)
        rsi = trackers[symbol].compute_rsi()
        print(f"[{symbol}] ({cfg['market']}) 초기 RSI={rsi:.1f}, zone={prev_zone}")

        if cfg["market"] == "futures":
            futures_symbols.append(symbol)
        else:
            spot_symbols.append(symbol)

    start_time = time.time()
    print(f"실시간 감시 시작 (최대 {MAX_RUNTIME_SEC}초 실행 예정)")

    await asyncio.gather(
        market_loop("spot", spot_symbols, trackers, start_time),
        market_loop("futures", futures_symbols, trackers, start_time),
    )

    print("실행 시간 한도 도달. 상태 저장 후 정상 종료 (다음 스케줄에서 자동 재시작됨)")
    save_state({s: t.zone for s, t in trackers.items()})


if __name__ == "__main__":
    asyncio.run(run())
