import json
import math
import os
import re
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import feedparser
import requests

STATE_PATH = Path("state/btc_state.json")
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
TELEGRAM_URL = "https://api.telegram.org/bot{token}/sendMessage"

ASSETS = {
    "bitcoin": {
        "symbol": "BTC",
        "name": "Bitcoin",
        "base_thresholds": {"5m": 1.0, "15m": 2.0, "60m": 3.5, "24h": 6.0},
        "vol_floor": 1.3,
        "vol_cap": 4.0,
        "max_asset_pct": 12.0,
    },
    "ethereum": {
        "symbol": "ETH",
        "name": "Ethereum",
        "base_thresholds": {"5m": 1.2, "15m": 2.4, "60m": 4.0, "24h": 7.0},
        "vol_floor": 1.8,
        "vol_cap": 5.5,
        "max_asset_pct": 10.0,
    },
}

POSITIVE_TERMS = {
    "approval": 1.3,
    "approved": 1.3,
    "approve": 1.1,
    "adoption": 1.0,
    "bullish": 1.2,
    "rally": 1.0,
    "surge": 1.0,
    "breakout": 1.0,
    "partnership": 0.6,
    "institutional": 0.8,
    "inflows": 1.1,
    "record high": 1.4,
    "upgrade": 0.6,
    "launch": 0.4,
    "buyback": 0.8,
    "accumulation": 0.8,
    "etf": 0.8,
    "beats": 0.5,
}

NEGATIVE_TERMS = {
    "hack": 1.6,
    "exploit": 1.6,
    "ban": 1.3,
    "lawsuit": 1.2,
    "selloff": 1.1,
    "liquidation": 1.2,
    "outflows": 1.0,
    "bearish": 1.2,
    "crash": 1.5,
    "plunge": 1.3,
    "fraud": 1.5,
    "scam": 1.5,
    "investigation": 1.1,
    "delay": 0.7,
    "rejected": 1.1,
    "rejection": 1.1,
    "bankruptcy": 1.6,
    "security breach": 1.6,
}


class BotError(Exception):
    pass


@dataclass
class Config:
    telegram_bot_token: str
    telegram_chat_id: str
    coingecko_api_key: str
    trigger_event: str
    signal_alert_confidence: int
    cooldown_minutes: int
    daily_summary_utc_hour: int
    enable_news: bool
    base_5m_pct: float
    base_15m_pct: float
    base_60m_pct: float
    base_24h_pct: float


def load_config() -> Config:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    api_key = os.getenv("COINGECKO_API_KEY", "").strip()
    missing = [
        key
        for key, value in [
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
        trigger_event=os.getenv("TRIGGER_EVENT", "schedule").strip(),
        signal_alert_confidence=int(os.getenv("SIGNAL_ALERT_CONFIDENCE", "78")),
        cooldown_minutes=int(os.getenv("COOLDOWN_MINUTES", "30")),
        daily_summary_utc_hour=int(os.getenv("DAILY_SUMMARY_UTC_HOUR", "7")),
        enable_news=os.getenv("ENABLE_NEWS", "true").strip().lower() != "false",
        base_5m_pct=float(os.getenv("ALERT_5M_PCT", "1.0")),
        base_15m_pct=float(os.getenv("ALERT_15M_PCT", "2.0")),
        base_60m_pct=float(os.getenv("ALERT_60M_PCT", "3.5")),
        base_24h_pct=float(os.getenv("ALERT_24H_PCT", "6.0")),
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def iso_to_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def fmt_price(price: float) -> str:
    return f"${price:,.2f}"


def fmt_pct(value: float) -> str:
    return f"{value:+.2f}%"


def load_state() -> Dict:
    if not STATE_PATH.exists():
        return {
            "last_alerts": {},
            "last_recommendations": {},
            "daily_summary_sent": {},
            "created_at": dt_to_iso(utc_now()),
        }
    with STATE_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def save_state(state: Dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2)
        fh.write("\n")


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


def cg_headers(config: Config) -> Dict[str, str]:
    return {"x-cg-demo-api-key": config.coingecko_api_key}


def get_market_snapshot(config: Config) -> Dict[str, Dict]:
    params = {
        "ids": "bitcoin,ethereum",
        "vs_currencies": "usd",
        "include_24hr_change": "true",
        "include_24hr_vol": "true",
        "include_market_cap": "true",
        "include_last_updated_at": "true",
    }
    response = requests.get(
        f"{COINGECKO_BASE}/simple/price",
        params=params,
        headers=cg_headers(config),
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    snapshots: Dict[str, Dict] = {}
    for coin_id, meta in ASSETS.items():
        coin = payload.get(coin_id)
        if not coin or "usd" not in coin:
            raise BotError(f"Unexpected CoinGecko response for {coin_id}: {payload}")
        updated_at = coin.get("last_updated_at")
        updated_dt = (
            datetime.fromtimestamp(updated_at, tz=timezone.utc)
            if isinstance(updated_at, (int, float))
            else utc_now()
        )
        snapshots[coin_id] = {
            "coin_id": coin_id,
            "symbol": meta["symbol"],
            "name": meta["name"],
            "price": float(coin["usd"]),
            "change_24h": float(coin.get("usd_24h_change") or 0.0),
            "market_cap": float(coin.get("usd_market_cap") or 0.0),
            "volume_24h": float(coin.get("usd_24h_vol") or 0.0),
            "updated_at": dt_to_iso(updated_dt),
        }
    return snapshots


def get_market_chart(config: Config, coin_id: str) -> Dict:
    response = requests.get(
        f"{COINGECKO_BASE}/coins/{coin_id}/market_chart",
        params={"vs_currency": "usd", "days": "1"},
        headers=cg_headers(config),
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    prices = data.get("prices") or []
    volumes = data.get("total_volumes") or []
    if not prices:
        raise BotError(f"Missing market chart prices for {coin_id}: {data}")
    return {"prices": prices, "volumes": volumes}


def series_values(prices: List[List[float]]) -> List[float]:
    return [float(point[1]) for point in prices]


def returns_from_prices(values: List[float]) -> List[float]:
    returns = []
    for prev, curr in zip(values, values[1:]):
        if prev:
            returns.append((curr - prev) / prev)
    return returns


def ema(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    alpha = 2 / (period + 1)
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (1 - alpha) * result
    return result


def rsi(values: List[float], period: int = 14) -> float:
    if len(values) < period + 1:
        return 50.0
    gains = []
    losses = []
    for prev, curr in zip(values[-(period + 1):-1], values[-period:]):
        diff = curr - prev
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def macd(values: List[float]) -> Tuple[float, float, float]:
    if len(values) < 35:
        return 0.0, 0.0, 0.0
    ema12_series = []
    ema26_series = []
    alpha12 = 2 / 13
    alpha26 = 2 / 27
    e12 = values[0]
    e26 = values[0]
    for value in values:
        e12 = alpha12 * value + (1 - alpha12) * e12
        e26 = alpha26 * value + (1 - alpha26) * e26
        ema12_series.append(e12)
        ema26_series.append(e26)
    macd_series = [a - b for a, b in zip(ema12_series, ema26_series)]
    signal = ema(macd_series, 9)
    macd_line = macd_series[-1]
    hist = macd_line - signal
    return macd_line, signal, hist


def find_price_at_or_before(prices: List[List[float]], target_dt: datetime) -> Optional[float]:
    target_ms = target_dt.timestamp() * 1000
    baseline = None
    for ts_ms, price in prices:
        if ts_ms <= target_ms:
            baseline = float(price)
        else:
            break
    return baseline


def pct_change(current: float, previous: Optional[float]) -> float:
    if not previous:
        return 0.0
    return ((current - previous) / previous) * 100


def sign(value: float, deadzone: float = 0.05) -> int:
    if value > deadzone:
        return 1
    if value < -deadzone:
        return -1
    return 0


def google_news_rss_url(asset_query: str) -> str:
    q = quote_plus(f"{asset_query} when:24h")
    return f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"


def entry_time(entry) -> datetime:
    if getattr(entry, "published_parsed", None):
        import time
        return datetime.fromtimestamp(time.mktime(entry.published_parsed), tz=timezone.utc)
    if getattr(entry, "updated_parsed", None):
        import time
        return datetime.fromtimestamp(time.mktime(entry.updated_parsed), tz=timezone.utc)
    return utc_now()


def score_headline(text: str) -> float:
    lower = text.lower()
    score = 0.0
    for term, weight in POSITIVE_TERMS.items():
        if term in lower:
            score += weight
    for term, weight in NEGATIVE_TERMS.items():
        if term in lower:
            score -= weight
    if "bitcoin" in lower or "btc" in lower:
        score *= 1.0
    if "ethereum" in lower or "eth" in lower:
        score *= 1.0
    return score


def get_asset_news(coin_id: str) -> Dict:
    query = "bitcoin OR btc crypto" if coin_id == "bitcoin" else "ethereum OR eth crypto"
    feed = feedparser.parse(google_news_rss_url(query))
    articles = []
    weighted_scores = []
    now = utc_now()

    for entry in feed.entries[:12]:
        title = getattr(entry, "title", "") or ""
        summary = re.sub(r"<[^>]+>", " ", getattr(entry, "summary", "") or "")
        combined = f"{title} {summary}".strip()
        if not combined:
            continue
        published = entry_time(entry)
        age_hours = max((now - published).total_seconds() / 3600, 0.0)
        recency_weight = 1.0 if age_hours <= 6 else 0.75 if age_hours <= 12 else 0.5
        raw_score = score_headline(combined)
        weighted_scores.append(raw_score * recency_weight)
        articles.append(
            {
                "title": title[:180],
                "link": getattr(entry, "link", ""),
                "published_at": dt_to_iso(published),
                "score": round(raw_score, 2),
            }
        )

    avg = 0.0
    if weighted_scores:
        avg = sum(weighted_scores) / (len(weighted_scores) * 2.5)
    sentiment = clamp(avg, -1.0, 1.0)
    return {
        "article_count": len(articles),
        "sentiment": sentiment,
        "headlines": articles[:5],
    }


def compute_analysis(snapshot: Dict, chart: Dict, news: Dict, config: Config) -> Dict:
    prices = chart["prices"]
    values = series_values(prices)
    now = utc_now()
    current = snapshot["price"]

    p5 = find_price_at_or_before(prices, now - timedelta(minutes=5))
    p15 = find_price_at_or_before(prices, now - timedelta(minutes=15))
    p60 = find_price_at_or_before(prices, now - timedelta(minutes=60))

    change_5m = pct_change(current, p5)
    change_15m = pct_change(current, p15)
    change_60m = pct_change(current, p60)
    change_24h = snapshot["change_24h"]

    ema20 = ema(values[-80:], 20)
    ema50 = ema(values[-120:], 50)
    rsi14 = rsi(values, 14)
    macd_line, macd_signal, macd_hist = macd(values)
    macd_hist_pct = (macd_hist / current) * 100 if current else 0.0

    returns_5m = returns_from_prices(values[-13:])
    hourly_vol_pct = 0.0
    if len(returns_5m) >= 3:
        hourly_vol_pct = statistics.pstdev(returns_5m) * math.sqrt(12) * 100

    meta = ASSETS[snapshot["coin_id"]]
    stop_distance_pct = clamp(
        max(meta["vol_floor"], hourly_vol_pct * 2.2),
        meta["vol_floor"],
        meta["vol_cap"],
    )

    trend_gap_pct = ((ema20 - ema50) / ema50) * 100 if ema50 else 0.0
    trend_score = clamp(trend_gap_pct / 0.75, -1.0, 1.0) * 0.30
    momentum_score = (
        clamp(change_15m / 2.0, -1.0, 1.0) * 0.15
        + clamp(change_60m / 4.0, -1.0, 1.0) * 0.20
        + clamp(change_24h / 8.0, -1.0, 1.0) * 0.10
    )
    if rsi14 < 32:
        rsi_score = 0.18
    elif rsi14 < 42:
        rsi_score = 0.08
    elif rsi14 > 72:
        rsi_score = -0.22
    elif rsi14 > 62:
        rsi_score = -0.10
    else:
        rsi_score = 0.0
    macd_score = clamp(macd_hist_pct / 0.12, -1.0, 1.0) * 0.15
    news_score = news["sentiment"] * 0.30 if config.enable_news else 0.0

    total_score = clamp(trend_score + momentum_score + rsi_score + macd_score + news_score, -1.0, 1.0)

    if total_score >= 0.48:
        action = "STRONG BUY"
    elif total_score >= 0.18:
        action = "BUY"
    elif total_score <= -0.48:
        action = "STRONG SELL"
    elif total_score <= -0.18:
        action = "SELL"
    else:
        action = "HOLD"

    components = [trend_score, momentum_score, rsi_score, macd_score, news_score]
    active_signs = [sign(x, 0.03) for x in components if sign(x, 0.03) != 0]
    agreement = abs(sum(active_signs)) / len(active_signs) if active_signs else 0.0
    disagreement_penalty = 8 if (sign(news_score, 0.03) and sign(news_score, 0.03) != sign(trend_score + momentum_score + macd_score, 0.03)) else 0
    news_penalty = 8 if news["article_count"] < 3 else 0
    confidence = clamp(48 + abs(total_score) * 30 + agreement * 18 - disagreement_penalty - news_penalty, 35, 92)

    max_asset_pct = meta["max_asset_pct"]
    if action == "STRONG BUY":
        add_now_pct = round(clamp((confidence - 60) / 5, 2.5, 6.0), 1)
        suggested_position_pct = round(min(max_asset_pct, add_now_pct + 4.0), 1)
        hold_window = "12-48 hours while the signal stays Buy"
        stop_loss = current * (1 - stop_distance_pct / 100)
        take_profit_1 = current * (1 + (stop_distance_pct * 1.5) / 100)
        take_profit_2 = current * (1 + (stop_distance_pct * 3.0) / 100)
        plan = f"Add about {add_now_pct:.1f}% of portfolio now, but keep total {snapshot['symbol']} exposure under {suggested_position_pct:.1f}% of portfolio."
        exit_rule = "Trim 50% at TP1, move stop to breakeven, then let the rest run to TP2 or until the signal drops to Hold/Sell."
    elif action == "BUY":
        add_now_pct = round(clamp((confidence - 50) / 8, 1.0, 3.5), 1)
        suggested_position_pct = round(min(max_asset_pct, add_now_pct + 2.5), 1)
        hold_window = "6-24 hours while momentum stays positive"
        stop_loss = current * (1 - stop_distance_pct / 100)
        take_profit_1 = current * (1 + (stop_distance_pct * 1.2) / 100)
        take_profit_2 = current * (1 + (stop_distance_pct * 2.4) / 100)
        plan = f"Starter size only: about {add_now_pct:.1f}% of portfolio now, with total {snapshot['symbol']} exposure capped near {suggested_position_pct:.1f}% of portfolio."
        exit_rule = "Take some profit at TP1 or exit early if the signal flips back to Hold/Sell."
    elif action == "HOLD":
        add_now_pct = 0.0
        suggested_position_pct = round(min(max_asset_pct, 6.0), 1)
        hold_window = "Recheck on the next bot update"
        stop_loss = current * (1 - stop_distance_pct / 100)
        take_profit_1 = current * (1 + (stop_distance_pct * 1.0) / 100)
        take_profit_2 = current * (1 + (stop_distance_pct * 2.0) / 100)
        plan = f"Do not add fresh size here. Hold only a modest position and keep total {snapshot['symbol']} exposure around or below {suggested_position_pct:.1f}% of portfolio."
        exit_rule = "Stay patient until the signal becomes a cleaner Buy or Sell."
    elif action == "SELL":
        trim_pct = round(clamp((confidence - 45) / 6, 20.0, 45.0), 0)
        add_now_pct = 0.0
        suggested_position_pct = round(min(max_asset_pct, 4.0), 1)
        hold_window = "Lighten risk now and reassess within 2-12 hours"
        stop_loss = current * (1 + stop_distance_pct / 100)
        take_profit_1 = current * (1 - (stop_distance_pct * 1.2) / 100)
        take_profit_2 = current * (1 - (stop_distance_pct * 2.4) / 100)
        plan = f"If you already hold {snapshot['symbol']}, consider trimming about {int(trim_pct)}% of the position and avoid new buys until the signal improves."
        exit_rule = "If price bounces above the invalidation level, stop trimming and wait for the next signal."
    else:
        trim_pct = round(clamp((confidence - 50) / 4, 35.0, 70.0), 0)
        add_now_pct = 0.0
        suggested_position_pct = 0.0
        hold_window = "Act quickly; reassess on the next bot update"
        stop_loss = current * (1 + stop_distance_pct / 100)
        take_profit_1 = current * (1 - (stop_distance_pct * 1.5) / 100)
        take_profit_2 = current * (1 - (stop_distance_pct * 3.0) / 100)
        plan = f"If you hold {snapshot['symbol']}, consider selling or trimming {int(trim_pct)}% to 100% depending on your risk tolerance."
        exit_rule = "Only re-enter after the bot returns to Hold/Buy and momentum improves."

    rationale = []
    if trend_score > 0.08:
        rationale.append("short-term trend is above the medium trend")
    elif trend_score < -0.08:
        rationale.append("short-term trend is below the medium trend")
    if momentum_score > 0.10:
        rationale.append("recent price momentum is strong")
    elif momentum_score < -0.10:
        rationale.append("recent price momentum is weak")
    if rsi_score > 0.05:
        rationale.append("RSI looks washed out / rebound-friendly")
    elif rsi_score < -0.05:
        rationale.append("RSI looks stretched / overheated")
    if abs(news_score) > 0.08 and config.enable_news:
        if news_score > 0:
            rationale.append("headline sentiment is positive")
        else:
            rationale.append("headline sentiment is negative")
    if not rationale:
        rationale.append("signals are mixed, so patience is better than forcing a trade")

    return {
        "snapshot": snapshot,
        "change_5m": change_5m,
        "change_15m": change_15m,
        "change_60m": change_60m,
        "change_24h": change_24h,
        "ema20": ema20,
        "ema50": ema50,
        "trend_gap_pct": trend_gap_pct,
        "rsi14": rsi14,
        "macd_hist_pct": macd_hist_pct,
        "hourly_vol_pct": hourly_vol_pct,
        "news": news,
        "score": total_score,
        "action": action,
        "confidence": round(confidence, 1),
        "plan": plan,
        "hold_window": hold_window,
        "stop_loss": stop_loss,
        "take_profit_1": take_profit_1,
        "take_profit_2": take_profit_2,
        "exit_rule": exit_rule,
        "rationale": rationale,
        "thresholds": {
            "5m": config.base_5m_pct * (1.0 if snapshot["coin_id"] == "bitcoin" else 1.2),
            "15m": config.base_15m_pct * (1.0 if snapshot["coin_id"] == "bitcoin" else 1.2),
            "60m": config.base_60m_pct * (1.0 if snapshot["coin_id"] == "bitcoin" else 1.15),
            "24h": config.base_24h_pct * (1.0 if snapshot["coin_id"] == "bitcoin" else 1.15),
        },
    }


def build_status_intro(analyses: List[Dict], config: Config) -> str:
    parts = ["🤖 *BTC + ETH signal bot is live*", ""]
    for analysis in analyses:
        snap = analysis["snapshot"]
        parts.append(
            f"*{snap['symbol']}* {fmt_price(snap['price'])} | 24h {fmt_pct(analysis['change_24h'])} | {analysis['action']} ({analysis['confidence']:.0f}% conf)"
        )
    parts.extend([
        "",
        "This confidence is the bot's internal signal confidence, *not* a profit guarantee.",
        f"Urgent action alerts fire at about {config.signal_alert_confidence}% confidence or higher.",
    ])
    return "\n".join(parts)


def build_analysis_message(analysis: Dict) -> str:
    snap = analysis["snapshot"]
    headlines = analysis["news"]["headlines"][:3]
    headline_block = "\n".join([f"• {item['title'][:85]}" for item in headlines]) or "• No strong headlines picked up"
    return (
        f"*{snap['name']} ({snap['symbol']})*\n"
        f"Signal: *{analysis['action']}*\n"
        f"Confidence: *{analysis['confidence']:.0f}%*\n"
        f"Price: *{fmt_price(snap['price'])}*\n"
        f"5m / 15m / 60m / 24h: *{fmt_pct(analysis['change_5m'])}* / *{fmt_pct(analysis['change_15m'])}* / *{fmt_pct(analysis['change_60m'])}* / *{fmt_pct(analysis['change_24h'])}*\n"
        f"RSI14: *{analysis['rsi14']:.1f}* | EMA20 vs EMA50 gap: *{analysis['trend_gap_pct']:+.2f}%*\n"
        f"News sentiment: *{analysis['news']['sentiment']:+.2f}* from *{analysis['news']['article_count']}* headlines\n"
        "\n"
        f"Plan: {analysis['plan']}\n"
        f"Hold window: {analysis['hold_window']}\n"
        f"Stop / invalidation: *{fmt_price(analysis['stop_loss'])}*\n"
        f"TP1 / TP2: *{fmt_price(analysis['take_profit_1'])}* / *{fmt_price(analysis['take_profit_2'])}*\n"
        f"Exit rule: {analysis['exit_rule']}\n"
        f"Why: {', '.join(analysis['rationale'])}.\n"
        "\n"
        "Recent headlines:\n"
        f"{headline_block}"
    )


def build_move_alert(analysis: Dict, window: str, move_pct: float, baseline_price: float) -> str:
    snap = analysis["snapshot"]
    emoji = "📈" if move_pct > 0 else "📉"
    return (
        f"{emoji} *{snap['symbol']} fast move*\n"
        f"Window: *{window}*\n"
        f"Move: *{fmt_pct(move_pct)}*\n"
        f"Now: *{fmt_price(snap['price'])}*\n"
        f"Then: *{fmt_price(baseline_price)}*\n"
        f"Signal: *{analysis['action']}* ({analysis['confidence']:.0f}% conf)"
    )


def build_signal_alert(analysis: Dict) -> str:
    snap = analysis["snapshot"]
    hot = "🚨" if "STRONG" in analysis["action"] else "⚠️"
    return (
        f"{hot} *{snap['symbol']} urgent {analysis['action']} alert*\n"
        f"Confidence: *{analysis['confidence']:.0f}%*\n"
        f"Price: *{fmt_price(snap['price'])}*\n"
        f"Plan: {analysis['plan']}\n"
        f"Stop / invalidation: *{fmt_price(analysis['stop_loss'])}*\n"
        f"TP1 / TP2: *{fmt_price(analysis['take_profit_1'])}* / *{fmt_price(analysis['take_profit_2'])}*\n"
        f"Why: {', '.join(analysis['rationale'])}."
    )


def can_send(last_alerts: Dict[str, str], key: str, now: datetime, cooldown_minutes: int) -> bool:
    sent = last_alerts.get(key)
    if not sent:
        return True
    return now - iso_to_dt(sent) >= timedelta(minutes=cooldown_minutes)


def record_alert(last_alerts: Dict[str, str], key: str, now: datetime) -> None:
    last_alerts[key] = dt_to_iso(now)


def maybe_send_move_alerts(config: Config, state: Dict, analysis: Dict, chart: Dict, now: datetime) -> int:
    alerts_sent = 0
    last_alerts = state.setdefault("last_alerts", {})
    prices = chart["prices"]
    snap = analysis["snapshot"]
    thresholds = analysis["thresholds"]

    windows = [
        (5, analysis["change_5m"], thresholds["5m"], "5m"),
        (15, analysis["change_15m"], thresholds["15m"], "15m"),
        (60, analysis["change_60m"], thresholds["60m"], "60m"),
    ]

    for minutes_back, move, threshold, label in windows:
        if abs(move) < threshold:
            continue
        key = f"{snap['coin_id']}_{label}_{'up' if move > 0 else 'down'}"
        if not can_send(last_alerts, key, now, config.cooldown_minutes):
            continue
        baseline = find_price_at_or_before(prices, now - timedelta(minutes=minutes_back))
        if baseline is None:
            continue
        send_telegram_message(config, build_move_alert(analysis, f"about {minutes_back} minutes", move, baseline))
        record_alert(last_alerts, key, now)
        alerts_sent += 1

    if abs(analysis["change_24h"]) >= thresholds["24h"]:
        key = f"{snap['coin_id']}_24h_{'up' if analysis['change_24h'] > 0 else 'down'}"
        if can_send(last_alerts, key, now, config.cooldown_minutes * 4):
            send_telegram_message(
                config,
                f"{'🚀' if analysis['change_24h'] > 0 else '💥'} *{snap['symbol']} big day move*\n"
                f"24h move: *{fmt_pct(analysis['change_24h'])}*\n"
                f"Price: *{fmt_price(snap['price'])}*\n"
                f"Signal: *{analysis['action']}* ({analysis['confidence']:.0f}% conf)",
            )
            record_alert(last_alerts, key, now)
            alerts_sent += 1
    return alerts_sent


def maybe_send_signal_alert(config: Config, state: Dict, analysis: Dict, now: datetime) -> int:
    action = analysis["action"]
    if action not in {"BUY", "STRONG BUY", "SELL", "STRONG SELL"}:
        return 0
    if analysis["confidence"] < config.signal_alert_confidence:
        return 0

    snap = analysis["snapshot"]
    last_recs = state.setdefault("last_recommendations", {})
    rec_key = snap["coin_id"]
    previous = last_recs.get(rec_key, {})
    last_alerts = state.setdefault("last_alerts", {})
    alert_key = f"signal_{snap['coin_id']}_{action.lower().replace(' ', '_')}"

    changed = previous.get("action") != action or abs(previous.get("confidence", 0) - analysis["confidence"]) >= 8
    if not changed and not can_send(last_alerts, alert_key, now, config.cooldown_minutes * 2):
        return 0

    send_telegram_message(config, build_signal_alert(analysis))
    record_alert(last_alerts, alert_key, now)
    return 1


def maybe_send_daily_summary(config: Config, state: Dict, analyses: List[Dict], now: datetime) -> int:
    if now.hour != config.daily_summary_utc_hour:
        return 0
    day_key = now.strftime("%Y-%m-%d")
    sent = state.setdefault("daily_summary_sent", {})
    if sent.get(day_key):
        return 0
    parts = ["🗞️ *Daily BTC / ETH signal check*", ""]
    for analysis in analyses:
        snap = analysis["snapshot"]
        parts.append(
            f"*{snap['symbol']}* — {analysis['action']} ({analysis['confidence']:.0f}% conf) | 24h {fmt_pct(analysis['change_24h'])}"
        )
        parts.append(f"Plan: {analysis['plan']}")
        parts.append(f"Stop: {fmt_price(analysis['stop_loss'])} | TP1: {fmt_price(analysis['take_profit_1'])}")
        parts.append("")
    parts.append("Confidence is model confidence, not a guarantee.")
    send_telegram_message(config, "\n".join(parts))
    sent[day_key] = True
    return 1


def main() -> None:
    config = load_config()
    now = utc_now()
    state = load_state()

    snapshots = get_market_snapshot(config)
    analyses = []
    charts = {}
    for coin_id in ["bitcoin", "ethereum"]:
        chart = get_market_chart(config, coin_id)
        charts[coin_id] = chart
        news = get_asset_news(coin_id) if config.enable_news else {"article_count": 0, "sentiment": 0.0, "headlines": []}
        analyses.append(compute_analysis(snapshots[coin_id], chart, news, config))

    state["last_run"] = dt_to_iso(now)
    state["last_snapshot"] = snapshots

    if config.trigger_event == "workflow_dispatch":
        send_telegram_message(config, build_status_intro(analyses, config))
        for analysis in analyses:
            send_telegram_message(config, build_analysis_message(analysis))

    alerts_sent = 0
    for analysis in analyses:
        coin_id = analysis["snapshot"]["coin_id"]
        alerts_sent += maybe_send_move_alerts(config, state, analysis, charts[coin_id], now)
        alerts_sent += maybe_send_signal_alert(config, state, analysis, now)
        state.setdefault("last_recommendations", {})[coin_id] = {
            "action": analysis["action"],
            "confidence": analysis["confidence"],
            "score": analysis["score"],
            "updated_at": dt_to_iso(now),
        }

    alerts_sent += maybe_send_daily_summary(config, state, analyses, now)
    state["last_alert_count"] = alerts_sent
    save_state(state)

    print(
        json.dumps(
            {
                "updated_at": dt_to_iso(now),
                "alerts_sent": alerts_sent,
                "assets": [
                    {
                        "symbol": a["snapshot"]["symbol"],
                        "price": a["snapshot"]["price"],
                        "action": a["action"],
                        "confidence": a["confidence"],
                    }
                    for a in analyses
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
