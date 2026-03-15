import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

STATE_PATH = Path("state/btc_state.json")
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
TELEGRAM_URL = "https://api.telegram.org/bot{token}/sendMessage"


@dataclass
class Config:
    telegram_bot_token: str
    telegram_chat_id: str
    coingecko_api_key: str
    alert_5m_pct: float
    alert_15m_pct: float
    alert_60m_pct: float
    alert_24h_pct: float
    cooldown_minutes: int
    trigger_event: str


class BotError(Exception):
    pass


def load_config() -> Config:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    api_key = os.getenv("COINGECKO_API_KEY", "").strip()

    missing = [
        name
        for name, value in [
            ("TELEGRAM_BOT_TOKEN", token),
            ("TELEGRAM_CHAT_ID", chat_id),
            ("COINGECKO_API_KEY", api_key),
        ]
        if not value
    ]
    if missing:
        raise BotError(f"Missing required environment variables: {', '.join(missing)}")

    return Config(
        telegram_bot_token=token,
        telegram_chat_id=chat_id,
        coingecko_api_key=api_key,
        alert_5m_pct=float(os.getenv("ALERT_5M_PCT", "1.0")),
        alert_15m_pct=float(os.getenv("ALERT_15M_PCT", "2.0")),
        alert_60m_pct=float(os.getenv("ALERT_60M_PCT", "3.5")),
        alert_24h_pct=float(os.getenv("ALERT_24H_PCT", "6.0")),
        cooldown_minutes=int(os.getenv("COOLDOWN_MINUTES", "30")),
        trigger_event=os.getenv("TRIGGER_EVENT", "schedule").strip(),
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def iso_to_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def load_state() -> Dict:
    if not STATE_PATH.exists():
        return {
            "history": [],
            "last_alerts": {},
            "created_at": dt_to_iso(utc_now()),
        }

    with STATE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: Dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
        f.write("\n")


def get_market_snapshot(config: Config) -> Dict:
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd",
        "include_24hr_change": "true",
        "include_last_updated_at": "true",
    }
    headers = {"x-cg-demo-api-key": config.coingecko_api_key}

    response = requests.get(COINGECKO_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    payload = response.json()

    btc = payload.get("bitcoin")
    if not btc or "usd" not in btc:
        raise BotError(f"Unexpected CoinGecko response: {payload}")

    updated_at = btc.get("last_updated_at")
    updated_dt = (
        datetime.fromtimestamp(updated_at, tz=timezone.utc)
        if isinstance(updated_at, (int, float))
        else utc_now()
    )

    return {
        "price": float(btc["usd"]),
        "change_24h": float(btc.get("usd_24h_change") or 0.0),
        "updated_at": dt_to_iso(updated_dt),
    }


def prune_history(history: List[Dict], now: datetime) -> List[Dict]:
    cutoff = now - timedelta(hours=2)
    return [point for point in history if iso_to_dt(point["ts"]) >= cutoff]


def append_history(history: List[Dict], price: float, now: datetime) -> List[Dict]:
    history = prune_history(history, now)
    history.append({"ts": dt_to_iso(now), "price": price})
    return history


def find_baseline(history: List[Dict], now: datetime, minutes_back: int) -> Optional[Dict]:
    cutoff = now - timedelta(minutes=minutes_back)
    candidates = [point for point in history if iso_to_dt(point["ts"]) <= cutoff]
    if not candidates:
        return None
    return max(candidates, key=lambda point: iso_to_dt(point["ts"]))


def pct_change(current_price: float, baseline_price: float) -> float:
    if baseline_price == 0:
        return 0.0
    return ((current_price - baseline_price) / baseline_price) * 100


def can_send(last_alerts: Dict[str, str], key: str, now: datetime, cooldown_minutes: int) -> bool:
    last_sent = last_alerts.get(key)
    if not last_sent:
        return True
    return now - iso_to_dt(last_sent) >= timedelta(minutes=cooldown_minutes)


def record_alert(last_alerts: Dict[str, str], key: str, now: datetime) -> None:
    last_alerts[key] = dt_to_iso(now)


def fmt_price(price: float) -> str:
    return f"${price:,.2f}"


def fmt_change(value: float) -> str:
    return f"{value:+.2f}%"


def send_telegram_message(config: Config, text: str) -> None:
    url = TELEGRAM_URL.format(token=config.telegram_bot_token)
    response = requests.post(
        url,
        data={
            "chat_id": config.telegram_chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": "true",
        },
        timeout=30,
    )
    response.raise_for_status()


def build_status_message(snapshot: Dict, config: Config) -> str:
    return (
        "✅ *BTC alert bot is live*\n"
        f"Price: *{fmt_price(snapshot['price'])}*\n"
        f"24h move: *{fmt_change(snapshot['change_24h'])}*\n"
        "\n"
        "Current alert rules:\n"
        f"• 5m spike: {config.alert_5m_pct:.2f}%\n"
        f"• 15m spike: {config.alert_15m_pct:.2f}%\n"
        f"• 60m spike: {config.alert_60m_pct:.2f}%\n"
        f"• 24h big day: {config.alert_24h_pct:.2f}%\n"
        f"• Cooldown: {config.cooldown_minutes} minutes"
    )


def build_move_alert(title: str, window_label: str, move_pct: float, current_price: float, baseline_price: float, change_24h: float) -> str:
    direction = "📈" if move_pct > 0 else "📉"
    return (
        f"{direction} *{title}*\n"
        f"Window: *{window_label}*\n"
        f"Move: *{fmt_change(move_pct)}*\n"
        f"Now: *{fmt_price(current_price)}*\n"
        f"Then: *{fmt_price(baseline_price)}*\n"
        f"24h move: *{fmt_change(change_24h)}*"
    )


def build_daily_alert(change_24h: float, current_price: float) -> str:
    direction = "🚀" if change_24h > 0 else "💥"
    title = "BTC strong daily pump" if change_24h > 0 else "BTC heavy daily drop"
    return (
        f"{direction} *{title}*\n"
        f"24h move: *{fmt_change(change_24h)}*\n"
        f"Current price: *{fmt_price(current_price)}*"
    )


def maybe_send_alerts(config: Config, state: Dict, snapshot: Dict, now: datetime) -> int:
    alerts_sent = 0
    history = state["history"]
    last_alerts = state.setdefault("last_alerts", {})
    current_price = snapshot["price"]
    change_24h = snapshot["change_24h"]

    windows = [
        (5, config.alert_5m_pct, "5m", "BTC rapid move"),
        (15, config.alert_15m_pct, "15m", "BTC breakout move"),
        (60, config.alert_60m_pct, "60m", "BTC hourly move"),
    ]

    for minutes_back, threshold, key, title in windows:
        baseline = find_baseline(history, now, minutes_back)
        if not baseline:
            continue
        move = pct_change(current_price, float(baseline["price"]))
        if abs(move) < threshold:
            continue
        if not can_send(last_alerts, key, now, config.cooldown_minutes):
            continue

        message = build_move_alert(
            title=title,
            window_label=f"about {minutes_back} minutes",
            move_pct=move,
            current_price=current_price,
            baseline_price=float(baseline["price"]),
            change_24h=change_24h,
        )
        send_telegram_message(config, message)
        record_alert(last_alerts, key, now)
        alerts_sent += 1

    if abs(change_24h) >= config.alert_24h_pct:
        daily_key = "24h_up" if change_24h > 0 else "24h_down"
        if can_send(last_alerts, daily_key, now, config.cooldown_minutes * 4):
            send_telegram_message(config, build_daily_alert(change_24h, current_price))
            record_alert(last_alerts, daily_key, now)
            alerts_sent += 1

    return alerts_sent


def main() -> None:
    config = load_config()
    now = utc_now()
    state = load_state()
    snapshot = get_market_snapshot(config)

    state["history"] = append_history(
        history=state.get("history", []),
        price=snapshot["price"],
        now=now,
    )
    state["last_run"] = dt_to_iso(now)
    state["last_snapshot"] = snapshot

    if config.trigger_event == "workflow_dispatch":
        send_telegram_message(config, build_status_message(snapshot, config))

    alerts_sent = maybe_send_alerts(config, state, snapshot, now)
    state["last_alert_count"] = alerts_sent
    save_state(state)

    print(json.dumps({
        "price": snapshot["price"],
        "change_24h": snapshot["change_24h"],
        "alerts_sent": alerts_sent,
        "updated_at": snapshot["updated_at"],
    }, indent=2))


if __name__ == "__main__":
    main()
