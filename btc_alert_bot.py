import hashlib
import json
import math
import os
import re
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional
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
        "queries": [
            "bitcoin OR btc OR spot bitcoin etf OR crypto",
            "bitcoin OR btc OR fed OR inflation OR cpi OR sec OR etf crypto",
        ],
        "thresholds": {"5m": 1.0, "15m": 2.0, "60m": 3.5, "24h": 6.0},
        "max_exposure": 8.0,
    },
    "ethereum": {
        "symbol": "ETH",
        "name": "Ethereum",
        "queries": [
            "ethereum OR ether OR eth OR spot ether etf OR crypto",
            "ethereum OR eth OR fed OR inflation OR cpi OR sec OR etf crypto",
        ],
        "thresholds": {"5m": 1.2, "15m": 2.4, "60m": 4.0, "24h": 7.0},
        "max_exposure": 7.0,
    },
}

SOURCE_WEIGHTS = {
    "Reuters": 1.30,
    "Bloomberg": 1.25,
    "Associated Press": 1.20,
    "AP News": 1.20,
    "Financial Times": 1.18,
    "The Wall Street Journal": 1.16,
    "CNBC": 1.12,
    "Fortune": 1.08,
    "CoinDesk": 1.10,
    "The Block": 1.10,
    "Yahoo Finance": 1.05,
    "MarketWatch": 1.05,
    "Decrypt": 1.03,
    "Cointelegraph": 1.02,
}

POSITIVE_TERMS = {
    "approved": 2.0,
    "approval": 2.0,
    "etf inflow": 1.9,
    "etf inflows": 1.9,
    "inflow": 1.0,
    "inflows": 1.0,
    "adoption": 1.0,
    "strategic reserve": 2.2,
    "treasury purchase": 2.0,
    "buys bitcoin": 1.6,
    "buys ethereum": 1.6,
    "partnership": 0.7,
    "upgrade": 0.7,
    "surge": 0.8,
    "rally": 0.8,
    "record high": 1.2,
    "bullish": 1.1,
    "rate cut": 1.1,
    "rate cuts": 1.1,
    "cooling inflation": 1.2,
    "softer inflation": 1.2,
}

NEGATIVE_TERMS = {
    "rejected": 2.0,
    "rejection": 2.0,
    "delay": 0.9,
    "delays": 0.9,
    "hack": 2.6,
    "hacked": 2.6,
    "exploit": 2.5,
    "security breach": 2.5,
    "bankruptcy": 2.6,
    "fraud": 2.0,
    "lawsuit": 1.7,
    "investigation": 1.5,
    "ban": 1.8,
    "bans": 1.8,
    "outflow": 1.2,
    "outflows": 1.2,
    "liquidation": 1.6,
    "liquidations": 1.6,
    "selloff": 1.3,
    "crash": 1.6,
    "plunge": 1.5,
    "bearish": 1.1,
    "rate hike": 1.4,
    "rate hikes": 1.4,
    "hot inflation": 1.5,
    "sticky inflation": 1.4,
}


class BotError(Exception):
    pass


@dataclass
class Destination:
    chat_id: str
    message_thread_id: Optional[int] = None


@dataclass
class Config:
    telegram_bot_token: str
    telegram_destinations: List[Destination]
    coingecko_api_key: str
    trigger_event: str
    cooldown_minutes: int
    signal_alert_confidence: int
    signal_flip_min_confidence: int
    news_watch_confidence: int
    news_impact_alert_score: float
    daily_summary_utc_hour: int
    enable_news: bool


def parse_destinations(raw: str) -> List[Destination]:
    destinations: List[Destination] = []
    for item in re.split(r"[,\n]", raw):
        token = item.strip()
        if not token:
            continue
        if ":" in token:
            chat_id, maybe_thread = token.rsplit(":", 1)
            if maybe_thread.isdigit():
                destinations.append(Destination(chat_id=chat_id.strip(), message_thread_id=int(maybe_thread)))
                continue
        destinations.append(Destination(chat_id=token))
    return destinations


def load_config() -> Config:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    raw_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    api_key = os.getenv("COINGECKO_API_KEY", "").strip()

    missing = [
        name
        for name, value in [
            ("TELEGRAM_BOT_TOKEN", token),
            ("TELEGRAM_CHAT_ID", raw_chat_id),
            ("COINGECKO_API_KEY", api_key),
        ]
        if not value
    ]
    if missing:
        raise BotError(f"Missing required environment variables: {', '.join(missing)}")

    destinations = parse_destinations(raw_chat_id)
    if not destinations:
        raise BotError("TELEGRAM_CHAT_ID must contain at least one destination")

    return Config(
        telegram_bot_token=token,
        telegram_destinations=destinations,
        coingecko_api_key=api_key,
        trigger_event=os.getenv("TRIGGER_EVENT", "schedule").strip(),
        cooldown_minutes=int(os.getenv("COOLDOWN_MINUTES", "30")),
        signal_alert_confidence=int(os.getenv("SIGNAL_ALERT_CONFIDENCE", "80")),
        signal_flip_min_confidence=int(os.getenv("SIGNAL_FLIP_MIN_CONFIDENCE", "72")),
        news_watch_confidence=int(os.getenv("NEWS_WATCH_CONFIDENCE", "68")),
        news_impact_alert_score=float(os.getenv("NEWS_IMPACT_ALERT_SCORE", "2.25")),
        daily_summary_utc_hour=int(os.getenv("DAILY_SUMMARY_UTC_HOUR", "7")),
        enable_news=os.getenv("ENABLE_NEWS", "true").strip().lower() != "false",
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


def normalize_title(value: str) -> str:
    value = re.sub(r"\s+", " ", (value or "").strip().lower())
    value = re.sub(r"[^a-z0-9 ]+", "", value)
    return value


def load_state() -> Dict:
    if not STATE_PATH.exists():
        return {
            "created_at": dt_to_iso(utc_now()),
            "last_alerts": {},
            "last_recommendations": {},
            "seen_headlines": {},
            "daily_summary_sent": {},
        }
    with STATE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: Dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
        f.write("\n")


def send_telegram_message(config: Config, text: str) -> None:
    url = TELEGRAM_URL.format(token=config.telegram_bot_token)
    for dest in config.telegram_destinations:
        data = {
            "chat_id": dest.chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
        if dest.message_thread_id is not None:
            data["message_thread_id"] = str(dest.message_thread_id)
        response = requests.post(url, data=data, timeout=30)
        response.raise_for_status()


def cg_headers(config: Config) -> Dict[str, str]:
    return {"x-cg-demo-api-key": config.coingecko_api_key}


def get_market_snapshot(config: Config) -> Dict[str, Dict]:
    response = requests.get(
        f"{COINGECKO_BASE}/simple/price",
        params={
            "ids": "bitcoin,ethereum",
            "vs_currencies": "usd",
            "include_24hr_change": "true",
            "include_24hr_vol": "true",
            "include_market_cap": "true",
        },
        headers=cg_headers(config),
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    snapshots = {}
    for coin_id, meta in ASSETS.items():
        coin = data.get(coin_id)
        if not coin or "usd" not in coin:
            raise BotError(f"Unexpected CoinGecko payload for {coin_id}: {data}")
        snapshots[coin_id] = {
            "coin_id": coin_id,
            "symbol": meta["symbol"],
            "name": meta["name"],
            "price": float(coin["usd"]),
            "change_24h": float(coin.get("usd_24h_change") or 0.0),
            "market_cap": float(coin.get("usd_market_cap") or 0.0),
            "volume_24h": float(coin.get("usd_24h_vol") or 0.0),
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
    return {
        "prices": data.get("prices") or [],
        "volumes": data.get("total_volumes") or [],
    }


def series_values(points: List[List[float]]) -> List[float]:
    return [float(p[1]) for p in points]


def average(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


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
    recent = values[-(period + 1):]
    for prev, curr in zip(recent[:-1], recent[1:]):
        diff = curr - prev
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def macd(values: List[float]) -> float:
    if len(values) < 35:
        return 0.0
    ema12 = ema(values[-120:], 12)
    ema26 = ema(values[-120:], 26)
    return ema12 - ema26


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


def source_from_title(title: str) -> str:
    parts = [x.strip() for x in (title or "").rsplit(" - ", 1)]
    return parts[1] if len(parts) == 2 else "Unknown"


def title_without_source(title: str) -> str:
    parts = [x.strip() for x in (title or "").rsplit(" - ", 1)]
    return parts[0] if parts else title


def google_news_rss_url(asset_query: str) -> str:
    q = quote_plus(f"{asset_query} when:24h")
    return f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"


def entry_time(entry) -> datetime:
    if getattr(entry, "published_parsed", None):
        return datetime.fromtimestamp(time.mktime(entry.published_parsed), tz=timezone.utc)
    if getattr(entry, "updated_parsed", None):
        return datetime.fromtimestamp(time.mktime(entry.updated_parsed), tz=timezone.utc)
    return utc_now()


def relevance_for_asset(coin_id: str, text: str) -> float:
    lower = text.lower()
    if coin_id == "bitcoin" and any(t in lower for t in ["bitcoin", "btc", "spot bitcoin etf"]):
        return 1.0
    if coin_id == "ethereum" and any(t in lower for t in ["ethereum", "ether", "eth", "spot ether etf"]):
        return 1.0
    if any(t in lower for t in ["fed", "federal reserve", "cpi", "inflation", "rates", "risk assets"]):
        return 0.65
    if "crypto" in lower:
        return 0.75
    return 0.45


def score_headline(coin_id: str, text: str, source: str) -> Dict:
    lower = text.lower()
    raw = 0.0
    tags = []
    for term, weight in POSITIVE_TERMS.items():
        if term in lower:
            raw += weight
            tags.append(term)
    for term, weight in NEGATIVE_TERMS.items():
        if term in lower:
            raw -= weight
            tags.append(term)
    if coin_id == "bitcoin" and "bitcoin etf" in lower:
        raw *= 1.10
    if coin_id == "ethereum" and any(t in lower for t in ["ether etf", "ethereum etf"]):
        raw *= 1.10
    return {
        "raw_score": raw,
        "source_weight": SOURCE_WEIGHTS.get(source, 1.0),
        "relevance": relevance_for_asset(coin_id, lower),
        "direction": 1 if raw > 0 else -1 if raw < 0 else 0,
        "tags": tags[:3],
    }


def headline_id(coin_id: str, title: str) -> str:
    return f"{coin_id}:{hashlib.sha1(normalize_title(title).encode('utf-8')).hexdigest()[:16]}"


def get_asset_news(coin_id: str) -> Dict:
    now = utc_now()
    seen = set()
    articles = []
    for query in ASSETS[coin_id]["queries"]:
        feed = feedparser.parse(google_news_rss_url(query))
        for entry in feed.entries[:10]:
            title = getattr(entry, "title", "") or ""
            clean_title = title_without_source(title)
            norm = normalize_title(clean_title)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            source = source_from_title(title)
            summary = re.sub(r"<[^>]+>", " ", getattr(entry, "summary", "") or "")
            combined = f"{clean_title} {summary}".strip()
            scoring = score_headline(coin_id, combined, source)
            published = entry_time(entry)
            age_hours = max((now - published).total_seconds() / 3600, 0.0)
            recency = 1.15 if age_hours <= 2 else 1.0 if age_hours <= 6 else 0.8 if age_hours <= 12 else 0.6
            weighted = scoring["raw_score"] * scoring["source_weight"] * scoring["relevance"] * recency
            articles.append(
                {
                    "id": headline_id(coin_id, clean_title),
                    "title": clean_title[:160],
                    "source": source,
                    "published_at": dt_to_iso(published),
                    "weighted_score": round(weighted, 2),
                    "direction": scoring["direction"],
                    "tags": scoring["tags"],
                }
            )
    articles.sort(key=lambda x: abs(x["weighted_score"]), reverse=True)
    top = articles[:12]
    sentiment = clamp(average([a["weighted_score"] for a in top]) / 3.0, -1.0, 1.0)
    strongest = top[0] if top else None
    return {
        "article_count": len(top),
        "sentiment": sentiment,
        "strongest": strongest,
        "headlines": top[:3],
    }


def volume_ratio(volumes: List[List[float]]) -> float:
    vals = series_values(volumes)
    if len(vals) < 18:
        return 1.0
    recent = average(vals[-6:])
    baseline = statistics.median(vals[-24:-6]) if len(vals) >= 24 else statistics.median(vals[:-6])
    if baseline <= 0:
        return 1.0
    return recent / baseline


def regime_label(trend_gap_pct: float, rsi14: float) -> str:
    if trend_gap_pct > 0.35 and rsi14 < 72:
        return "bull"
    if trend_gap_pct < -0.35 and rsi14 > 28:
        return "bear"
    if rsi14 >= 72:
        return "hot"
    if rsi14 <= 30:
        return "oversold"
    return "neutral"


def compute_analysis(snapshot: Dict, chart: Dict, news: Dict) -> Dict:
    prices = chart["prices"]
    if not prices:
        raise BotError(f"Missing chart prices for {snapshot['coin_id']}")
    values = series_values(prices)
    current = snapshot["price"]
    now = utc_now()

    p5 = find_price_at_or_before(prices, now - timedelta(minutes=5))
    p15 = find_price_at_or_before(prices, now - timedelta(minutes=15))
    p60 = find_price_at_or_before(prices, now - timedelta(minutes=60))

    change_5m = pct_change(current, p5)
    change_15m = pct_change(current, p15)
    change_60m = pct_change(current, p60)
    change_24h = snapshot["change_24h"]

    ema20 = ema(values[-80:], 20)
    ema50 = ema(values[-120:], 50)
    trend_gap_pct = ((ema20 - ema50) / ema50) * 100 if ema50 else 0.0
    rsi14 = rsi(values, 14)
    macd_value = macd(values)
    macd_pct = (macd_value / current) * 100 if current else 0.0
    vol_ratio = volume_ratio(chart.get("volumes") or [])

    trend_score = clamp(trend_gap_pct / 0.8, -1.0, 1.0) * 0.30
    momentum_score = (
        clamp(change_5m / 1.5, -1.0, 1.0) * 0.08
        + clamp(change_15m / 2.2, -1.0, 1.0) * 0.16
        + clamp(change_60m / 4.2, -1.0, 1.0) * 0.16
        + clamp(change_24h / 8.5, -1.0, 1.0) * 0.08
    )
    if rsi14 < 30:
        rsi_score = 0.16
    elif rsi14 < 38:
        rsi_score = 0.08
    elif rsi14 > 76:
        rsi_score = -0.18
    elif rsi14 > 68:
        rsi_score = -0.09
    else:
        rsi_score = 0.0

    volume_score = 0.0
    if vol_ratio > 1.15:
        volume_score = clamp((vol_ratio - 1.0) / 0.8, 0.0, 1.0) * (0.10 if (change_15m + change_60m) > 0 else -0.10)

    news_sentiment_score = news["sentiment"] * 0.18
    strongest = news.get("strongest") or {"weighted_score": 0.0}
    news_impulse_score = clamp((strongest["weighted_score"] if strongest else 0.0) / 3.0, -1.0, 1.0) * 0.14
    news_score = news_sentiment_score + news_impulse_score

    total_score = clamp(trend_score + momentum_score + rsi_score + clamp(macd_pct / 0.1, -1.0, 1.0) * 0.10 + volume_score + news_score, -1.0, 1.0)

    if total_score >= 0.56:
        action = "STRONG BUY"
    elif total_score >= 0.22:
        action = "BUY"
    elif total_score <= -0.56:
        action = "STRONG SELL"
    elif total_score <= -0.22:
        action = "SELL"
    else:
        action = "HOLD"

    agreement_bits = [sign(x, 0.03) for x in [trend_score, momentum_score, rsi_score, news_score] if sign(x, 0.03) != 0]
    agreement = abs(sum(agreement_bits)) / len(agreement_bits) if agreement_bits else 0.0
    confidence = clamp(
        40 + abs(total_score) * 32 + agreement * 14 + max(vol_ratio - 1.0, 0.0) * 8,
        30,
        95,
    )

    stop_distance_pct = 1.3 if snapshot["coin_id"] == "bitcoin" else 1.8
    stop_distance_pct = max(stop_distance_pct, min(abs(change_60m) * 1.8, 5.0))

    max_exposure = ASSETS[snapshot["coin_id"]]["max_exposure"]
    if action == "STRONG BUY":
        add_pct = round(clamp((confidence - 60) / 4.5, 2.0, 5.0), 1)
        hold_window = "12-72ч"
        stop = current * (1 - stop_distance_pct / 100)
        tp1 = current * (1 + stop_distance_pct * 1.4 / 100)
        tp2 = current * (1 + stop_distance_pct * 2.8 / 100)
        short_plan = f"Buy {add_pct:.1f}% | max {max_exposure:.1f}%"
    elif action == "BUY":
        add_pct = round(clamp((confidence - 52) / 7.0, 1.0, 3.5), 1)
        hold_window = "6-24ч"
        stop = current * (1 - stop_distance_pct / 100)
        tp1 = current * (1 + stop_distance_pct * 1.2 / 100)
        tp2 = current * (1 + stop_distance_pct * 2.2 / 100)
        short_plan = f"Buy {add_pct:.1f}% | max {max_exposure - 1.5:.1f}%"
    elif action == "HOLD":
        hold_window = "изчакай"
        stop = current * (1 - stop_distance_pct / 100)
        tp1 = current * (1 + stop_distance_pct / 100)
        tp2 = current * (1 + stop_distance_pct * 2 / 100)
        short_plan = f"No new buy | max {max_exposure - 2.0:.1f}%"
    elif action == "SELL":
        trim_pct = round(clamp((confidence - 48) / 5.5, 20.0, 50.0), 0)
        hold_window = "сега"
        stop = current * (1 + stop_distance_pct / 100)
        tp1 = current * (1 - stop_distance_pct * 1.1 / 100)
        tp2 = current * (1 - stop_distance_pct * 2.0 / 100)
        short_plan = f"Trim {trim_pct:.0f}%"
    else:
        trim_pct = round(clamp((confidence - 56) / 3.5, 35.0, 75.0), 0)
        hold_window = "сега"
        stop = current * (1 + stop_distance_pct / 100)
        tp1 = current * (1 - stop_distance_pct * 1.4 / 100)
        tp2 = current * (1 - stop_distance_pct * 2.6 / 100)
        short_plan = f"Trim {trim_pct:.0f}%"

    why_parts = []
    if trend_gap_pct > 0.15:
        why_parts.append("trend up")
    elif trend_gap_pct < -0.15:
        why_parts.append("trend down")
    if rsi14 >= 72:
        why_parts.append("overheated")
    elif rsi14 <= 30:
        why_parts.append("oversold")
    if strongest and abs(strongest.get("weighted_score", 0.0)) >= 2.0:
        why_parts.append("big news")
    if vol_ratio > 1.2:
        why_parts.append("high volume")
    if not why_parts:
        why_parts.append("mixed setup")

    return {
        "snapshot": snapshot,
        "action": action,
        "confidence": round(confidence),
        "price": current,
        "change_5m": change_5m,
        "change_15m": change_15m,
        "change_60m": change_60m,
        "change_24h": change_24h,
        "rsi14": rsi14,
        "trend_gap_pct": trend_gap_pct,
        "vol_ratio": vol_ratio,
        "news": news,
        "score": total_score,
        "regime": regime_label(trend_gap_pct, rsi14),
        "hold_window": hold_window,
        "short_plan": short_plan,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "why": ", ".join(why_parts[:3]),
    }


def can_send(last_alerts: Dict[str, str], key: str, now: datetime, cooldown_minutes: int) -> bool:
    last = last_alerts.get(key)
    if not last:
        return True
    return now - iso_to_dt(last) >= timedelta(minutes=cooldown_minutes)


def record_alert(last_alerts: Dict[str, str], key: str, now: datetime) -> None:
    last_alerts[key] = dt_to_iso(now)


def build_status_intro(analyses: List[Dict]) -> str:
    lines = ["🤖 Bot live"]
    for analysis in analyses:
        lines.append(
            f"{analysis['snapshot']['symbol']} {fmt_price(analysis['price'])} | {analysis['action']} {analysis['confidence']:.0f}%"
        )
    return "\n".join(lines)


def build_compact_analysis(analysis: Dict) -> str:
    symbol = analysis["snapshot"]["symbol"]
    return (
        f"{symbol} | {analysis['action']} | {analysis['confidence']:.0f}%\n"
        f"Price {fmt_price(analysis['price'])} | 24h {fmt_pct(analysis['change_24h'])}\n"
        f"Action: {analysis['short_plan']}\n"
        f"SL {fmt_price(analysis['stop'])} | TP {fmt_price(analysis['tp1'])} / {fmt_price(analysis['tp2'])}\n"
        f"Why: {analysis['why']}"
    )


def build_move_alert(analysis: Dict, label: str, move_pct: float, baseline: float) -> str:
    symbol = analysis["snapshot"]["symbol"]
    emoji = "🚀" if move_pct > 0 else "🔻"
    return (
        f"{emoji} {symbol} {label} {fmt_pct(move_pct)}\n"
        f"Now {fmt_price(analysis['price'])} | then {fmt_price(baseline)}\n"
        f"Signal {analysis['action']} {analysis['confidence']:.0f}%"
    )


def build_signal_alert(analysis: Dict) -> str:
    symbol = analysis["snapshot"]["symbol"]
    emoji = "🚨" if "BUY" in analysis["action"] else "⚠️"
    return (
        f"{emoji} {symbol} {analysis['action']} {analysis['confidence']:.0f}%\n"
        f"Price {fmt_price(analysis['price'])}\n"
        f"{analysis['short_plan']}\n"
        f"SL {fmt_price(analysis['stop'])} | TP {fmt_price(analysis['tp1'])} / {fmt_price(analysis['tp2'])}\n"
        f"Why: {analysis['why']}"
    )


def build_news_watch_alert(analysis: Dict, headline: Dict) -> str:
    symbol = analysis["snapshot"]["symbol"]
    emoji = "📰🚀" if headline["weighted_score"] > 0 else "📰⚠️"
    return (
        f"{emoji} {symbol} news\n"
        f"{headline['title']}\n"
        f"{analysis['action']} {analysis['confidence']:.0f}% | {fmt_price(analysis['price'])}"
    )


def should_send_signal_alert(config: Config, state: Dict, analysis: Dict) -> bool:
    coin_id = analysis["snapshot"]["coin_id"]
    prev = state.get("last_recommendations", {}).get(coin_id, {})
    prev_action = prev.get("action")
    prev_conf = float(prev.get("confidence", 0))
    current_action = analysis["action"]
    current_conf = analysis["confidence"]

    if current_action in {"BUY", "STRONG BUY", "SELL", "STRONG SELL"} and current_conf >= config.signal_alert_confidence:
        return True

    if current_action != prev_action and current_action in {"BUY", "STRONG BUY", "SELL", "STRONG SELL"} and current_conf >= config.signal_flip_min_confidence:
        return True

    if abs(current_conf - prev_conf) >= 12 and current_action in {"BUY", "STRONG BUY", "SELL", "STRONG SELL"} and current_conf >= config.signal_flip_min_confidence:
        return True

    return False


def maybe_send_move_alerts(config: Config, state: Dict, analysis: Dict, chart: Dict, now: datetime) -> int:
    alerts_sent = 0
    last_alerts = state.setdefault("last_alerts", {})
    prices = chart["prices"]
    current = analysis["price"]
    symbol = analysis["snapshot"]["symbol"]
    thresholds = ASSETS[analysis["snapshot"]["coin_id"]]["thresholds"]

    for minutes_back, key in [(5, "5m"), (15, "15m"), (60, "60m")]:
        baseline = find_price_at_or_before(prices, now - timedelta(minutes=minutes_back))
        if not baseline:
            continue
        move = pct_change(current, baseline)
        if abs(move) < thresholds[key]:
            continue
        alert_key = f"{symbol}:{key}:{'up' if move > 0 else 'down'}"
        if not can_send(last_alerts, alert_key, now, config.cooldown_minutes):
            continue
        send_telegram_message(config, build_move_alert(analysis, key, move, baseline))
        record_alert(last_alerts, alert_key, now)
        alerts_sent += 1

    if abs(analysis["change_24h"]) >= thresholds["24h"]:
        key = f"{symbol}:24h:{'up' if analysis['change_24h'] > 0 else 'down'}"
        if can_send(last_alerts, key, now, config.cooldown_minutes * 4):
            send_telegram_message(config, build_move_alert(analysis, "24h", analysis["change_24h"], current / (1 + analysis["change_24h"] / 100)))
            record_alert(last_alerts, key, now)
            alerts_sent += 1

    return alerts_sent


def maybe_send_signal_alert(config: Config, state: Dict, analysis: Dict, now: datetime) -> int:
    if not should_send_signal_alert(config, state, analysis):
        return 0
    key = f"{analysis['snapshot']['symbol']}:signal:{analysis['action']}"
    last_alerts = state.setdefault("last_alerts", {})
    if not can_send(last_alerts, key, now, config.cooldown_minutes):
        return 0
    send_telegram_message(config, build_signal_alert(analysis))
    record_alert(last_alerts, key, now)
    return 1


def maybe_send_news_watch(config: Config, state: Dict, analysis: Dict, now: datetime) -> int:
    if not config.enable_news:
        return 0
    headline = analysis["news"].get("strongest")
    if not headline:
        return 0
    if abs(headline["weighted_score"]) < config.news_impact_alert_score:
        return 0
    if analysis["confidence"] < config.news_watch_confidence:
        return 0
    seen = state.setdefault("seen_headlines", {})
    if seen.get(headline["id"]):
        return 0
    key = f"{analysis['snapshot']['symbol']}:news:{'up' if headline['weighted_score'] > 0 else 'down'}"
    last_alerts = state.setdefault("last_alerts", {})
    if not can_send(last_alerts, key, now, config.cooldown_minutes):
        return 0
    send_telegram_message(config, build_news_watch_alert(analysis, headline))
    seen[headline["id"]] = dt_to_iso(now)
    record_alert(last_alerts, key, now)
    return 1


def maybe_send_daily_summary(config: Config, state: Dict, analyses: List[Dict], now: datetime) -> int:
    hour_key = now.strftime("%Y-%m-%d")
    sent = state.setdefault("daily_summary_sent", {})
    if now.hour != config.daily_summary_utc_hour or sent.get(hour_key):
        return 0

    actionable = [a for a in analyses if a["action"] != "HOLD" and a["confidence"] >= 65]
    if not actionable:
        sent[hour_key] = dt_to_iso(now)
        return 0

    lines = ["📌 Daily"]
    for a in actionable[:2]:
        lines.append(f"{a['snapshot']['symbol']} {a['action']} {a['confidence']:.0f}% | {fmt_price(a['price'])}")
    send_telegram_message(config, "\n".join(lines))
    sent[hour_key] = dt_to_iso(now)
    return 1


def main() -> None:
    config = load_config()
    now = utc_now()
    state = load_state()

    snapshots = get_market_snapshot(config)
    charts = {coin_id: get_market_chart(config, coin_id) for coin_id in ASSETS}
    news_by_asset = {coin_id: get_asset_news(coin_id) if config.enable_news else {"sentiment": 0.0, "strongest": None, "headlines": []} for coin_id in ASSETS}

    analyses = [
        compute_analysis(snapshots[coin_id], charts[coin_id], news_by_asset[coin_id])
        for coin_id in ASSETS
    ]

    analyses.sort(key=lambda a: (a["snapshot"]["symbol"] != "BTC", a["snapshot"]["symbol"]))

    if config.trigger_event == "workflow_dispatch":
        send_telegram_message(config, build_status_intro(analyses))
        for analysis in analyses:
            send_telegram_message(config, build_compact_analysis(analysis))

    alerts_sent = 0
    for analysis in analyses:
        coin_id = analysis["snapshot"]["coin_id"]
        alerts_sent += maybe_send_move_alerts(config, state, analysis, charts[coin_id], now)
        alerts_sent += maybe_send_signal_alert(config, state, analysis, now)
        alerts_sent += maybe_send_news_watch(config, state, analysis, now)
        state.setdefault("last_recommendations", {})[coin_id] = {
            "action": analysis["action"],
            "confidence": analysis["confidence"],
            "score": analysis["score"],
            "updated_at": dt_to_iso(now),
        }

    alerts_sent += maybe_send_daily_summary(config, state, analyses, now)
    state["last_run"] = dt_to_iso(now)
    state["last_alert_count"] = alerts_sent
    save_state(state)

    print(json.dumps({
        "updated_at": dt_to_iso(now),
        "alerts_sent": alerts_sent,
        "assets": [
            {"symbol": a["snapshot"]["symbol"], "action": a["action"], "confidence": a["confidence"], "price": a["price"]}
            for a in analyses
        ],
    }, indent=2))


if __name__ == "__main__":
    main()
