import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

STATE_PATH = Path("state/btc_state.json")
COINGECKO_SIMPLE_URL = "https://api.coingecko.com/api/v3/simple/price"
COINGECKO_CHART_URL = "https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
TELEGRAM_URL = "https://api.telegram.org/bot{token}/sendMessage"
COINS = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
}


class BotError(Exception):
    pass


@dataclass
class Config:
    telegram_bot_token: str
    telegram_chat_ids: List[str]
    coingecko_api_key: str
    cooldown_minutes: int
    summary_interval_hours: int
    strong_confidence: int
    trigger_event: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def iso_to_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def load_config() -> Config:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id_raw = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    api_key = os.getenv("COINGECKO_API_KEY", "").strip()
    if not token or not chat_id_raw or not api_key:
        raise BotError("Missing TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, or COINGECKO_API_KEY")

    chat_ids = [item.strip() for item in chat_id_raw.split(",") if item.strip()]
    if not chat_ids:
        raise BotError("No valid TELEGRAM_CHAT_ID found")

    return Config(
        telegram_bot_token=token,
        telegram_chat_ids=chat_ids,
        coingecko_api_key=api_key,
        cooldown_minutes=int(os.getenv("COOLDOWN_MINUTES", "30")),
        summary_interval_hours=int(os.getenv("SUMMARY_INTERVAL_HOURS", "4")),
        strong_confidence=int(os.getenv("STRONG_SIGNAL_CONFIDENCE", "74")),
        trigger_event=os.getenv("TRIGGER_EVENT", "schedule").strip(),
    )


def load_state() -> Dict:
    if not STATE_PATH.exists():
        return {"signals": {}, "last_alerts": {}, "created_at": dt_to_iso(utc_now())}
    with STATE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: Dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
        f.write("\n")


def send_telegram_message(config: Config, text: str) -> None:
    url = TELEGRAM_URL.format(token=config.telegram_bot_token)
    for chat_id in config.telegram_chat_ids:
        payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": "true"}
        if ":" in chat_id:
            base_id, thread_id = chat_id.split(":", 1)
            payload["chat_id"] = base_id
            payload["message_thread_id"] = thread_id
        response = requests.post(url, data=payload, timeout=30)
        response.raise_for_status()


def cg_headers(config: Config) -> Dict[str, str]:
    return {"x-cg-demo-api-key": config.coingecko_api_key}


def fetch_simple_prices(config: Config) -> Dict[str, Dict]:
    params = {
        "ids": ",".join(COINS.keys()),
        "vs_currencies": "usd",
        "include_24hr_change": "true",
        "include_last_updated_at": "true",
    }
    response = requests.get(COINGECKO_SIMPLE_URL, params=params, headers=cg_headers(config), timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_market_chart(config: Config, coin_id: str) -> List[Tuple[datetime, float]]:
    params = {"vs_currency": "usd", "days": "1"}
    response = requests.get(COINGECKO_CHART_URL.format(coin_id=coin_id), params=params, headers=cg_headers(config), timeout=30)
    response.raise_for_status()
    data = response.json()
    prices = data.get("prices", [])
    if not prices:
        raise BotError(f"No chart data for {coin_id}")
    return [
        (datetime.fromtimestamp(ts / 1000, tz=timezone.utc), float(price))
        for ts, price in prices
    ]


def nearest_price(points: List[Tuple[datetime, float]], target: datetime) -> Optional[float]:
    candidates = [(abs((ts - target).total_seconds()), price) for ts, price in points]
    if not candidates:
        return None
    seconds, price = min(candidates, key=lambda item: item[0])
    return price if seconds <= 30 * 60 else None


def pct_change(current: float, baseline: Optional[float]) -> float:
    if not baseline:
        return 0.0
    return ((current - baseline) / baseline) * 100


def ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    multiplier = 2 / (period + 1)
    value = sum(values[:period]) / period
    for item in values[period:]:
        value = (item - value) * multiplier + value
    return value


def rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(1, period + 1):
        delta = values[i] - values[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    for i in range(period + 1, len(values)):
        delta = values[i] - values[i - 1]
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def market_regime(change_4h: float, ema_gap_pct: float, rsi_value: float) -> str:
    if change_4h > 1.5 and ema_gap_pct > 0.25 and rsi_value < 72:
        return "bullish"
    if change_4h < -1.5 and ema_gap_pct < -0.25 and rsi_value > 28:
        return "bearish"
    return "mixed"


def build_signal(coin_id: str, symbol: str, current_price: float, change_24h: float, points: List[Tuple[datetime, float]]) -> Dict:
    now = points[-1][0]
    series = [price for _, price in points]
    p15 = nearest_price(points, now - timedelta(minutes=15))
    p60 = nearest_price(points, now - timedelta(hours=1))
    p240 = nearest_price(points, now - timedelta(hours=4))

    ch15 = pct_change(current_price, p15)
    ch60 = pct_change(current_price, p60)
    ch240 = pct_change(current_price, p240)

    ema20 = ema(series, 20)
    ema50 = ema(series, 50)
    ema_gap_pct = 0.0
    if ema20 and ema50 and ema50 != 0:
        ema_gap_pct = ((ema20 - ema50) / ema50) * 100
    rsi_value = rsi(series[-80:]) or 50.0

    day_high = max(series)
    day_low = min(series)
    drop_from_high = pct_change(current_price, day_high)
    bounce_from_low = pct_change(current_price, day_low)

    score = 0
    reasons: List[str] = []
    risks: List[str] = []

    if ch15 > 0.35:
        score += 1
        reasons.append("15m momentum is positive")
    elif ch15 < -0.35:
        score -= 1
        risks.append("15m momentum is negative")

    if ch60 > 0.8:
        score += 2
        reasons.append("1h trend is pushing up")
    elif ch60 < -0.8:
        score -= 2
        risks.append("1h trend is pushing down")

    if ch240 > 1.8:
        score += 2
        reasons.append("4h structure is bullish")
    elif ch240 < -1.8:
        score -= 2
        risks.append("4h structure is bearish")

    if ema_gap_pct > 0.18:
        score += 1
        reasons.append("EMA20 is above EMA50")
    elif ema_gap_pct < -0.18:
        score -= 1
        risks.append("EMA20 is below EMA50")

    if 45 <= rsi_value <= 65 and score > 0:
        score += 1
        reasons.append("RSI supports upside without overheating")
    elif 35 <= rsi_value <= 55 and score < 0:
        score -= 1
        risks.append("RSI is not oversold enough to trust a bounce")
    elif rsi_value > 72:
        score -= 1
        risks.append("RSI looks overheated")
    elif rsi_value < 30:
        score += 1
        reasons.append("RSI is deeply oversold and bounce risk is rising")

    if change_24h > 2.0:
        score += 1
        reasons.append("24h trend is positive")
    elif change_24h < -2.0:
        score -= 1
        risks.append("24h trend is negative")

    if drop_from_high <= -2.5:
        score -= 1
        risks.append("price is well below the day high")
    if bounce_from_low >= 1.8:
        score += 1
        reasons.append("price has bounced well from the day low")

    if score >= 5:
        signal = "BUY"
        confidence = min(88, 56 + score * 5)
        hold_window = "next 6 to 24 hours"
        action = "Look for an entry on small pullbacks, not on panic candles."
    elif score <= -5:
        signal = "SELL"
        confidence = min(88, 56 + abs(score) * 5)
        hold_window = "next 6 to 24 hours"
        action = "Avoid new buys and consider reducing exposure on weak bounces."
    else:
        signal = "HOLD"
        confidence = 52 + min(16, abs(score) * 3)
        hold_window = "next 2 to 8 hours"
        action = "Wait for a cleaner move before taking a new trade."

    stop = current_price * (0.987 if signal == "BUY" else 1.013 if signal == "SELL" else 0.985)
    tp1 = current_price * (1.015 if signal == "BUY" else 0.985 if signal == "SELL" else 1.01)
    tp2 = current_price * (1.028 if signal == "BUY" else 0.972 if signal == "SELL" else 1.018)

    return {
        "coin_id": coin_id,
        "symbol": symbol,
        "signal": signal,
        "confidence": int(round(confidence)),
        "current_price": current_price,
        "change_15m": ch15,
        "change_60m": ch60,
        "change_4h": ch240,
        "change_24h": change_24h,
        "ema_gap_pct": ema_gap_pct,
        "rsi": rsi_value,
        "regime": market_regime(ch240, ema_gap_pct, rsi_value),
        "day_high": day_high,
        "day_low": day_low,
        "hold_window": hold_window,
        "action": action,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "reasons": reasons[:4],
        "risks": risks[:4],
        "score": score,
    }


def format_price(value: float) -> str:
    return f"${value:,.2f}"


def format_pct(value: float) -> str:
    return f"{value:+.2f}%"


def build_prediction_message(signal: Dict) -> str:
    reasons = signal["reasons"] or ["No strong bullish confirmation yet"]
    risks = signal["risks"] or ["No major technical warning is standing out"]
    return "\n".join([
        f"{signal['symbol']} prediction",
        f"Signal: {signal['signal']} ({signal['confidence']}%)",
        f"Price: {format_price(signal['current_price'])}",
        f"Window: {signal['hold_window']}",
        f"Regime: {signal['regime']}",
        "",
        "Why it thinks this:",
        f"- 15m: {format_pct(signal['change_15m'])}",
        f"- 1h: {format_pct(signal['change_60m'])}",
        f"- 4h: {format_pct(signal['change_4h'])}",
        f"- 24h: {format_pct(signal['change_24h'])}",
        f"- RSI14: {signal['rsi']:.1f}",
        f"- EMA20 vs EMA50: {format_pct(signal['ema_gap_pct'])}",
        f"- Day high / low: {format_price(signal['day_high'])} / {format_price(signal['day_low'])}",
        "",
        "Bullish factors:",
        *[f"- {item}" for item in reasons],
        "",
        "Risk factors:",
        *[f"- {item}" for item in risks],
        "",
        "What to do:",
        f"- {signal['action']}",
        f"- Stop / invalidation: {format_price(signal['stop'])}",
        f"- TP1 / TP2: {format_price(signal['tp1'])} / {format_price(signal['tp2'])}",
        "",
        "When the signal changes:",
        "- It turns stronger if momentum and EMA gap keep improving.",
        "- It weakens if price loses momentum or breaks the invalidation level.",
    ])


def should_send_summary(state: Dict, symbol: str, now: datetime, config: Config, signal: Dict) -> bool:
    if config.trigger_event == "workflow_dispatch":
        return True
    signal_state = state.setdefault("signals", {}).get(symbol, {})
    last_summary = signal_state.get("last_summary_at")
    last_signal = signal_state.get("signal")
    last_conf = signal_state.get("confidence")
    if not last_summary:
        return True
    if last_signal != signal["signal"]:
        return True
    if last_conf is None or abs(signal["confidence"] - int(last_conf)) >= 8:
        return True
    return now - iso_to_dt(last_summary) >= timedelta(hours=config.summary_interval_hours)


def can_send_alert(state: Dict, key: str, now: datetime, config: Config) -> bool:
    last_sent = state.setdefault("last_alerts", {}).get(key)
    if not last_sent:
        return True
    return now - iso_to_dt(last_sent) >= timedelta(minutes=config.cooldown_minutes)


def record_alert(state: Dict, key: str, now: datetime) -> None:
    state.setdefault("last_alerts", {})[key] = dt_to_iso(now)


def maybe_send_urgent_alert(config: Config, state: Dict, signal: Dict, now: datetime) -> None:
    symbol = signal["symbol"]
    if signal["signal"] in {"BUY", "SELL"} and signal["confidence"] >= config.strong_confidence:
        key = f"urgent_{symbol}_{signal['signal']}"
        if can_send_alert(state, key, now, config):
            text = "\n".join([
                f"{symbol} urgent {signal['signal']}",
                f"Confidence: {signal['confidence']}%",
                f"Price: {format_price(signal['current_price'])}",
                f"Window: {signal['hold_window']}",
                f"Stop: {format_price(signal['stop'])}",
                f"TP1 / TP2: {format_price(signal['tp1'])} / {format_price(signal['tp2'])}",
            ])
            send_telegram_message(config, text)
            record_alert(state, key, now)

    if signal["change_60m"] <= -2.0:
        key = f"dump_{symbol}"
        if can_send_alert(state, key, now, config):
            text = f"{symbol} sharp drop\n1h: {format_pct(signal['change_60m'])}\nPrice: {format_price(signal['current_price'])}"
            send_telegram_message(config, text)
            record_alert(state, key, now)

    if signal["change_60m"] >= 2.0:
        key = f"pump_{symbol}"
        if can_send_alert(state, key, now, config):
            text = f"{symbol} strong pump\n1h: {format_pct(signal['change_60m'])}\nPrice: {format_price(signal['current_price'])}"
            send_telegram_message(config, text)
            record_alert(state, key, now)


def main() -> None:
    config = load_config()
    state = load_state()
    now = utc_now()
    simple = fetch_simple_prices(config)
    outputs = []

    for coin_id, symbol in COINS.items():
        info = simple.get(coin_id, {})
        current_price = float(info.get("usd") or 0.0)
        change_24h = float(info.get("usd_24h_change") or 0.0)
        if current_price <= 0:
            raise BotError(f"Bad price data for {coin_id}: {info}")
        points = fetch_market_chart(config, coin_id)
        signal = build_signal(coin_id, symbol, current_price, change_24h, points)

        if should_send_summary(state, symbol, now, config, signal):
            send_telegram_message(config, build_prediction_message(signal))
            state.setdefault("signals", {}).setdefault(symbol, {})["last_summary_at"] = dt_to_iso(now)

        state.setdefault("signals", {}).setdefault(symbol, {}).update({
            "signal": signal["signal"],
            "confidence": signal["confidence"],
            "last_price": signal["current_price"],
            "updated_at": dt_to_iso(now),
        })
        maybe_send_urgent_alert(config, state, signal, now)
        outputs.append({
            "symbol": symbol,
            "signal": signal["signal"],
            "confidence": signal["confidence"],
            "price": signal["current_price"],
        })

    save_state(state)
    print(json.dumps(outputs, indent=2))


if __name__ == "__main__":
    main()
