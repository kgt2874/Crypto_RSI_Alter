"""
바이낸스 1시간봉 RSI(14) 모니터링 -> 텔레그램 알림
- 완전 무료 (바이낸스 공개 API는 인증 불필요)
- GitHub Actions 등 무료 클라우드 스케줄러에서 매시간 실행되는 것을 전제로 작성됨
- RSI가 과매수(70 이상)/과매도(30 이하) 구간에 "새로 진입"할 때만 알림 (반복 스팸 방지)
"""

import os
import json
from pathlib import Path

import requests
import pandas as pd

# data-api.binance.vision: 바이낸스가 제공하는 "공개 시세 데이터 전용" 주소.
# api.binance.com은 미국 등 규제 지역 IP(예: GitHub Actions 서버)를 차단(451 에러)하지만,
# 이 주소는 인증이 필요없는 공개 시세 조회용이라 지역 제한 없이 열려 있음.
BINANCE_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"
STATE_FILE = Path("rsi_state.json")

INTERVAL = "1h"
RSI_PERIOD = 14

# 코인별 개별 기준값: {심볼: (과매도 기준, 과매수 기준)}
# 여기 없는 코인은 아래 DEFAULT 값을 사용함
SYMBOL_THRESHOLDS = {
    "BTCUSDT": (17, 87),
    "ETHUSDT": (17, 85),
    "BNBUSDT": (10, 72),
}
DEFAULT_OVERSOLD = 30
DEFAULT_OVERBOUGHT = 70

# 환경변수로 주입 (GitHub Actions workflow의 env: 참고)
SYMBOLS = [s.strip() for s in os.environ.get("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]


def fetch_closes(symbol: str, interval: str = INTERVAL, limit: int = 200) -> list[float]:
    """바이낸스 공개 API에서 종가 리스트를 가져온다. (API 키 불필요)"""
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    resp = requests.get(BINANCE_KLINES_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return [float(candle[4]) for candle in data]  # index 4 = close price


def calc_rsi(closes: list[float], period: int = RSI_PERIOD) -> float:
    """Wilder's RSI 계산 (지수이동평균 방식, 트레이딩뷰 기본값과 동일한 계산식)."""
    series = pd.Series(closes)
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])


def send_telegram(message: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    resp = requests.post(url, data=payload, timeout=10)
    resp.raise_for_status()


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))


def get_thresholds(symbol: str) -> tuple[float, float]:
    """코인별 (과매도, 과매수) 기준값을 반환. 지정 안 된 코인은 기본값(30/70) 사용."""
    return SYMBOL_THRESHOLDS.get(symbol, (DEFAULT_OVERSOLD, DEFAULT_OVERBOUGHT))


def zone_of(rsi: float, symbol: str) -> str:
    oversold, overbought = get_thresholds(symbol)
    if rsi >= overbought:
        return "overbought"
    if rsi <= oversold:
        return "oversold"
    return "neutral"


def main() -> None:
    state = load_state()

    for symbol in SYMBOLS:
        try:
            closes = fetch_closes(symbol)
            rsi = calc_rsi(closes)
        except Exception as e:
            print(f"[{symbol}] 데이터 조회/계산 오류: {e}")
            continue

        prev_zone = state.get(symbol, "neutral")
        zone = zone_of(rsi, symbol)
        oversold, overbought = get_thresholds(symbol)

        # 구간이 "바뀌었고" 새 구간이 과매수/과매도일 때만 알림
        if zone != prev_zone and zone != "neutral":
            if zone == "overbought":
                msg = f"⚠️ {symbol} 1H RSI(14) = {rsi:.1f}\n과매수 구간({overbought}) 진입"
            else:
                msg = f"⚠️ {symbol} 1H RSI(14) = {rsi:.1f}\n과매도 구간({oversold}) 진입"
            send_telegram(msg)
            print(f"[{symbol}] 알림 전송 완료: RSI={rsi:.1f}, {prev_zone} -> {zone}")
        else:
            print(f"[{symbol}] RSI={rsi:.1f}, zone={zone} (변화 없음, 알림 생략)")

        state[symbol] = zone

    save_state(state)


if __name__ == "__main__":
    main()
