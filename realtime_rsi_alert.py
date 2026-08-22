"""
바이낸스 실시간 웹소켓 기반 RSI(14) 모니터링 -> 텔레그램 즉시 알림
- 완전 무료 (바이낸스 공개 웹소켓/REST는 인증 불필요)
- GitHub Actions에서 6시간마다 새로 시작되며, 매 실행은 최대 약 5시간 40분 동안
  웹소켓에 연결된 채로 실시간 체결가를 받아 RSI를 즉시 재계산함 (초 단위 반응)
- RSI가 코인별 기준값을 "새로 돌파"할 때만 알림 (쿨다운으로 스팸 방지)
- 반드시 Public 저장소에서 사용할 것 (Private 무료 실행시간으로는 하루도 못 버팀)
"""

import os
import json
import time
import asyncio
from pathlib import Path

import requests
import websockets

REST_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"
WS_BASE_URL = "wss://data-stream.binance.vision/stream"
STATE_FILE = Path("rsi_state.json")

INTERVAL = "1h"
RSI_PERIOD = 14
HISTORY_SIZE = 200
ALERT_COOLDOWN_SEC = 180  # 같은 코인이 같은 구간을 반복 돌파해도 이 시간(초) 내엔 재알림 안 함
MAX_RUNTIME_SEC = int(os.environ.get("MAX_RUNTIME_SEC", 5 * 3600 + 40 * 60))  # 기본 5시간40분

SYMBOL_THRESHOLDS = {
    "BTCUSDT": (17, 87),
    "ETHUSDT": (17, 85),
    "BNBUSDT": (10, 72),
}
DEFAULT_OVERSOLD = 30
DEFAULT_OVERBOUGHT = 70

SYMBOLS = [s.strip().upper() for s in os.environ.get("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]


def get_thresholds(symbol: str):
    return SYMBOL_THRESHOLDS.get(symbol, (DEFAULT_OVERSOLD, DEFAULT_OVERBOUGHT))


def fetch_history(symbol: str, limit: int = HISTORY_SIZE):
    """시작 시점에 과거 종가를 REST로 한 번 채워둠 (RSI 계산 기준선 확보)."""
    params = {"symbol": symbol, "interval": INTERVAL, "limit": limit}
    resp = requests.get(REST_KLINES_URL, params=params, timeout=10)
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
    oversold, overbought = get_thresholds(symbol)
    if rsi >= overbought:
        return "overbought"
    if rsi <= oversold:
        return "oversold"
    return "neutral"


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
                oversold, overbought = get_thresholds(self.symbol)
                if new_zone == "overbought":
                    msg = f"⚡ {self.symbol} 1H RSI(14) = {rsi:.1f}\n과매수 구간({overbought}) 실시간 돌파"
                else:
                    msg = f"⚡ {self.symbol} 1H RSI(14) = {rsi:.1f}\n과매도 구간({oversold}) 실시간 돌파"
                send_telegram(msg)
                self.last_alert_ts = now
                print(f"[{self.symbol}] 알림 전송: RSI={rsi:.1f}, {self.zone} -> {new_zone}")

        self.zone = new_zone
        return rsi


async def run() -> None:
    state = load_state()
    trackers: dict[str, SymbolTracker] = {}

    print("초기 히스토리 로딩 중...")
    for symbol in SYMBOLS:
        closed, current = fetch_history(symbol)
        prev_zone = state.get(symbol, "neutral")
        trackers[symbol] = SymbolTracker(symbol, closed, current, prev_zone)
        rsi = trackers[symbol].compute_rsi()
        print(f"[{symbol}] 초기 RSI={rsi:.1f}, zone={prev_zone}")

    stream_names = "/".join(f"{s.lower()}@kline_{INTERVAL}" for s in SYMBOLS)
    ws_url = f"{WS_BASE_URL}?streams={stream_names}"

    start_time = time.time()
    last_save = start_time

    print(f"웹소켓 연결 시작 (최대 {MAX_RUNTIME_SEC}초 실행 예정)")
    while time.time() - start_time < MAX_RUNTIME_SEC:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                print("웹소켓 연결 성공, 실시간 수신 시작")
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
            print(f"웹소켓 연결 끊김, 5초 후 재연결 시도: {e}")
            await asyncio.sleep(5)
            continue

    print("실행 시간 한도 도달. 상태 저장 후 정상 종료 (다음 스케줄에서 자동 재시작됨)")
    save_state({s: t.zone for s, t in trackers.items()})


if __name__ == "__main__":
    asyncio.run(run())
