import hashlib
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
        "aliases": ["bitcoin", "btc", "spot bitcoin etf", "crypto"],
        "queries": [
            "bitcoin OR btc OR spot bitcoin etf OR crypto",
            "bitcoin OR btc OR fed OR inflation OR cpi OR rates OR sec OR etf crypto",
        ],
        "base_thresholds": {"5m": 1.0, "15m": 2.0, "60m": 3.5, "24h": 6.0},
        "vol_floor": 1.3,
        "vol_cap": 4.2,
        "max_asset_pct": 12.0,
    },
    "ethereum": {
        "symbol": "ETH",
        "name": "Ethereum",
        "aliases": ["ethereum", "ether", "eth", "spot ether etf", "crypto"],
        "queries": [
            "ethereum OR ether OR eth OR spot ether etf OR crypto",
            "ethereum OR eth OR fed OR inflation OR cpi OR rates OR sec OR etf crypto",
        ],
        "base_thresholds": {"5m": 1.2, "15m": 2.4, "60m": 4.0, "24h": 7.0},
        "vol_floor": 1.7,
        "vol_cap": 5.5,
        "max_asset_pct": 10.0,
    },
}

SOURCE_WEIGHTS = {
    "Reuters": 1.30,
    "Bloomberg": 1.28,
    "Associated Press": 1.22,
    "AP News": 1.22,
    "Financial Times": 1.22,
    "The Wall Street Journal": 1.20,
    "WSJ": 1.20,
    "CNBC": 1.16,
    "Fortune": 1.12,
    "Barron's": 1.12,
    "CoinDesk": 1.12,
    "The Block": 1.12,
    "Yahoo Finance": 1.08,
    "MarketWatch": 1.08,
    "Decrypt": 1.04,
    "Cointelegraph": 1.02,
    "Forbes": 1.06,
}

POSITIVE_TERMS = {
    "approved": 2.2,
    "approval": 2.1,
    "approve": 1.9,
    "cleared": 1.6,
    "wins approval": 2.2,
    "etf inflow": 1.8,
    "etf inflows": 1.8,
    "inflow": 1.2,
    "inflows": 1.2,
    "institutional": 1.0,
    "accumulation": 1.1,
    "adoption": 1.1,
    "strategic reserve": 2.3,
    "treasury purchase": 2.1,
    "treasury buys": 1.9,
    "buys bitcoin": 1.8,
    "buys ethereum": 1.8,
    "partnership": 0.8,
    "launch": 0.7,
    "upgrade": 0.7,
    "breakout": 0.9,
    "surge": 0.8,
    "rally": 0.8,
    "record high": 1.5,
    "bullish": 1.2,
    "beats expectations": 1.1,
    "rate cut": 1.2,
    "rate cuts": 1.2,
    "cooling inflation": 1.3,
    "softer inflation": 1.3,
    "eases": 0.7,
}

NEGATIVE_TERMS = {
    "rejected": 2.2,
    "rejection": 2.1,
    "delay": 1.0,
    "delays": 1.0,
    "hack": 2.6,
    "hacked": 2.6,
    "exploit": 2.6,
    "security breach": 2.6,
    "bankruptcy": 2.8,
    "insolvency": 2.6,
    "fraud": 2.2,
    "scam": 2.2,
    "lawsuit": 1.9,
    "sues": 1.9,
    "investigation": 1.6,
    "probe": 1.4,
    "ban": 2.0,
    "bans": 2.0,
    "outflow": 1.3,
    "outflows": 1.3,
    "liquidation": 1.8,
    "liquidations": 1.8,
    "selloff": 1.4,
    "crash": 1.8,
    "plunge": 1.6,
    "bearish": 1.2,
    "rate hike": 1.5,
    "rate hikes": 1.5,
    "hot inflation": 1.6,
    "sticky inflation": 1.5,
    "higher yields": 1.3,
    "tariff": 0.9,
    "tariffs": 0.9,
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
    telegram_chat_id: str
    telegram_destinations: List[Destination]
    coingecko_api_key: str
    trigger_event: str
    signal_alert_confidence: int
    news_watch_confidence: int
    news_impact_alert_score: float
    cooldown_minutes: int
    daily_summary_utc_hour: int
    enable_news: bool
    signal_flip_min_confidence: int
    base_5m_pct: float
    base_15m_pct: float
    base_60m_pct: float
    base_24h_pct: float


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

    destinations = parse_destinations(chat_id)
    if not destinations:
        raise BotError("TELEGRAM_CHAT_ID must contain at least one chat destination")

    return Config(
        telegram_bot_token=token,
        telegram_chat_id=chat_id,
        telegram_destinations=destinations,
        coingecko_api_key=api_key,
        trigger_event=os.getenv("TRIGGER_EVENT", "schedule").strip(),
        signal_alert_confidence=int(os.getenv("SIGNAL_ALERT_CONFIDENCE", "80")),
        news_watch_confidence=int(os.getenv("NEWS_WATCH_CONFIDENCE", "68")),
        news_impact_alert_score=float(os.getenv("NEWS_IMPACT_ALERT_SCORE", "2.25")),
        cooldown_minutes=int(os.getenv("COOLDOWN_MINUTES", "30")),
        daily_summary_utc_hour=int(os.getenv("DAILY_SUMMARY_UTC_HOUR", "7")),
        enable_news=os.getenv("ENABLE_NEWS", "true").strip().lower() != "false",
        signal_flip_min_confidence=int(os.getenv("SIGNAL_FLIP_MIN_CONFIDENCE", "72")),
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


def normalize_title(value: str) -> str:
    value = re.sub(r"\s+", " ", (value or "").strip().lower())
    value = re.sub(r"[^a-z0-9 ]+", "", value)
    return value


def load_state() -> Dict:
    if not STATE_PATH.exists():
        return {
            "last_alerts": {},
            "last_recommendations": {},
            "daily_summary_sent": {},
            "seen_news": {},
            "created_at": dt_to_iso(utc_now()),
        }
    with STATE_PATH.open("r", encoding="utf-8") as fh:
        state = json.load(fh)
    state.setdefault("seen_news", {})
    return state


def save_state(state: Dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2)
        fh.write("\n")


def prune_state(state: Dict, now: datetime) -> None:
    seen_news = state.setdefault("seen_news", {})
    cutoff = now - timedelta(days=3)
    for key, value in list(seen_news.items()):
        try:
            if iso_to_dt(value) < cutoff:
                del seen_news[key]
        except Exception:
            del seen_news[key]


def send_telegram_message(config: Config, text: str) -> None:
    url = TELEGRAM_URL.format(token=config.telegram_bot_token)
    errors = []
    for destination in config.telegram_destinations:
        payload = {
            "chat_id": destination.chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
        if destination.message_thread_id is not None:
            payload["message_thread_id"] = str(destination.message_thread_id)
        response = requests.post(url, data=payload, timeout=30)
        if not response.ok:
            errors.append(f"{destination.chat_id}: {response.status_code} {response.text[:200]}")
    if errors:
        raise BotError("Telegram send failed for one or more destinations: " + " | ".join(errors))


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


def series_values(points: List[List[float]]) -> List[float]:
    return [float(point[1]) for point in points]


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


def average(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def market_regime(trend_gap_pct: float, rsi14: float, vol_ratio: float, change_60m: float) -> str:
    if trend_gap_pct >= 0.25 and change_60m > 0.6 and vol_ratio >= 1.1:
        return "bull trend"
    if trend_gap_pct <= -0.25 and change_60m < -0.6 and vol_ratio >= 1.1:
        return "bear trend"
    if rsi14 >= 72:
        return "overheated"
    if rsi14 <= 30:
        return "oversold"
    if abs(change_60m) < 0.35 and abs(trend_gap_pct) < 0.12:
        return "range / indecisive"
    return "transition"


def source_from_title(title: str) -> str:
    parts = [x.strip() for x in (title or "").rsplit(" - ", 1)]
    if len(parts) == 2 and parts[1]:
        return parts[1]
    return "Unknown source"


def title_without_source(title: str) -> str:
    parts = [x.strip() for x in (title or "").rsplit(" - ", 1)]
    return parts[0] if parts else title


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


def relevance_for_asset(coin_id: str, text: str) -> float:
    lower = text.lower()
    meta = ASSETS[coin_id]
    if any(alias in lower for alias in meta["aliases"][:3]):
        return 1.0
    if "crypto" in lower or "digital asset" in lower:
        return 0.75
    if any(term in lower for term in ["fed", "federal reserve", "cpi", "inflation", "rates", "risk assets"]):
        return 0.65
    return 0.45


def score_headline(coin_id: str, text: str, source: str) -> Dict:
    lower = text.lower()
    raw = 0.0
    bull_tags = []
    bear_tags = []

    for term, weight in POSITIVE_TERMS.items():
        if term in lower:
            raw += weight
            bull_tags.append(term)
    for term, weight in NEGATIVE_TERMS.items():
        if term in lower:
            raw -= weight
            bear_tags.append(term)

    if coin_id == "bitcoin" and ("spot bitcoin etf" in lower or "bitcoin etf" in lower):
        raw *= 1.12
    if coin_id == "ethereum" and ("spot ether etf" in lower or "spot ethereum etf" in lower or "ethereum etf" in lower):
        raw *= 1.12

    source_weight = SOURCE_WEIGHTS.get(source, 1.0)
    relevance = relevance_for_asset(coin_id, lower)

    return {
        "raw_score": raw,
        "direction": 1 if raw > 0 else -1 if raw < 0 else 0,
        "source_weight": source_weight,
        "relevance": relevance,
        "bull_tags": bull_tags[:4],
        "bear_tags": bear_tags[:4],
    }


def headline_id(coin_id: str, title: str) -> str:
    return f"{coin_id}:{hashlib.sha1(normalize_title(title).encode('utf-8')).hexdigest()[:16]}"


def get_asset_news(coin_id: str, state: Dict) -> Dict:
    now = utc_now()
    articles = []
    seen_titles = set()

    for query in ASSETS[coin_id]["queries"]:
        feed = feedparser.parse(google_news_rss_url(query))
        for entry in feed.entries[:12]:
            title = getattr(entry, "title", "") or ""
            clean_title = title_without_source(title)
            norm = normalize_title(clean_title)
            if not norm or norm in seen_titles:
                continue
            seen_titles.add(norm)

            summary = re.sub(r"<[^>]+>", " ", getattr(entry, "summary", "") or "")
            combined = f"{clean_title} {summary}".strip()
            source = source_from_title(title)
            published = entry_time(entry)
            age_hours = max((now - published).total_seconds() / 3600, 0.0)
            recency_weight = 1.15 if age_hours <= 2 else 1.0 if age_hours <= 6 else 0.8 if age_hours <= 12 else 0.55
            scoring = score_headline(coin_id, combined, source)
            weighted = scoring["raw_score"] * recency_weight * scoring["source_weight"] * scoring["relevance"]

            articles.append(
                {
                    "id": headline_id(coin_id, clean_title),
                    "title": clean_title[:180],
                    "source": source,
                    "link": getattr(entry, "link", ""),
                    "published_at": dt_to_iso(published),
                    "raw_score": round(scoring["raw_score"], 2),
                    "weighted_score": round(weighted, 2),
                    "direction": scoring["direction"],
                    "bull_tags": scoring["bull_tags"],
                    "bear_tags": scoring["bear_tags"],
                    "source_weight": scoring["source_weight"],
                }
            )

    articles.sort(key=lambda x: abs(x["weighted_score"]), reverse=True)
    trimmed = articles[:14]
    weighted_scores = [item["weighted_score"] for item in trimmed]
    avg_weighted = average(weighted_scores)
    sentiment = clamp(avg_weighted / 2.8, -1.0, 1.0)
    max_impulse = 0.0
    if trimmed:
        max_impulse = max(trimmed, key=lambda x: abs(x["weighted_score"]))["weighted_score"]

    major = [item for item in trimmed if abs(item["weighted_score"]) >= 2.25]
    major_bull = [item for item in major if item["weighted_score"] > 0]
    major_bear = [item for item in major if item["weighted_score"] < 0]
    quality_score = clamp(
        len(trimmed) * 0.5 + average([item["source_weight"] for item in trimmed]) * 2.5 + len(major) * 1.5,
        0.0,
        12.0,
    )

    return {
        "article_count": len(trimmed),
        "sentiment": sentiment,
        "max_impulse": max_impulse,
        "quality_score": quality_score,
        "major_headlines": major[:3],
        "major_bull_count": len(major_bull),
        "major_bear_count": len(major_bear),
        "headlines": trimmed[:5],
    }


def volume_ratio(volumes: List[List[float]]) -> float:
    values = series_values(volumes)
    if len(values) < 18:
        return 1.0
    recent = average(values[-6:])
    baseline = statistics.median(values[-24:-6]) if len(values) >= 24 else statistics.median(values[:-6])
    if baseline <= 0:
        return 1.0
    return recent / baseline


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

    vol_ratio = volume_ratio(chart.get("volumes") or [])
    lookback = min(36, max(6, len(values) - 1))
    recent_window = values[-(lookback + 1):-1] if len(values) > lookback else values[:-1]
    recent_high = max(recent_window) if recent_window else current
    recent_low = min(recent_window) if recent_window else current
    breakout_up = current >= recent_high * 1.002
    breakout_down = current <= recent_low * 0.998

    meta = ASSETS[snapshot["coin_id"]]
    stop_distance_pct = clamp(
        max(meta["vol_floor"], hourly_vol_pct * 2.2),
        meta["vol_floor"],
        meta["vol_cap"],
    )

    trend_gap_pct = ((ema20 - ema50) / ema50) * 100 if ema50 else 0.0
    trend_score = clamp(trend_gap_pct / 0.75, -1.0, 1.0) * 0.26

    momentum_score = (
        clamp(change_5m / 1.4, -1.0, 1.0) * 0.08
        + clamp(change_15m / 2.0, -1.0, 1.0) * 0.16
        + clamp(change_60m / 4.0, -1.0, 1.0) * 0.20
        + clamp(change_24h / 8.0, -1.0, 1.0) * 0.08
    )

    if rsi14 < 28:
        rsi_score = 0.22
    elif rsi14 < 36:
        rsi_score = 0.10
    elif rsi14 > 76:
        rsi_score = -0.24
    elif rsi14 > 68:
        rsi_score = -0.12
    else:
        rsi_score = 0.0

    macd_score = clamp(macd_hist_pct / 0.10, -1.0, 1.0) * 0.14

    breakout_score = 0.0
    if breakout_up:
        breakout_score += 0.12
    if breakout_down:
        breakout_score -= 0.12

    volume_score = 0.0
    if vol_ratio > 1.2:
        direction = sign(change_15m + change_60m, 0.02)
        if direction > 0:
            volume_score += clamp((vol_ratio - 1.0) / 1.0, 0.0, 1.0) * 0.12
        elif direction < 0:
            volume_score -= clamp((vol_ratio - 1.0) / 1.0, 0.0, 1.0) * 0.12

    news_sentiment_score = news["sentiment"] * 0.18
    news_impulse_score = clamp(news["max_impulse"] / 3.0, -1.0, 1.0) * 0.16
    news_score = news_sentiment_score + news_impulse_score

    chart_direction = sign(trend_score + momentum_score + macd_score + breakout_score + volume_score, 0.03)
    news_direction = sign(news_score, 0.03)
    alignment_bonus = 0.0
    contradiction_penalty = 0.0
    if chart_direction != 0 and chart_direction == news_direction:
        alignment_bonus = 0.07
    elif chart_direction != 0 and news_direction != 0 and chart_direction != news_direction:
        contradiction_penalty = 0.10

    total_score = clamp(
        trend_score
        + momentum_score
        + rsi_score
        + macd_score
        + breakout_score
        + volume_score
        + news_score
        + alignment_bonus
        - contradiction_penalty,
        -1.0,
        1.0,
    )

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

    components = [trend_score, momentum_score, rsi_score, macd_score, breakout_score, volume_score, news_score]
    active_signs = [sign(x, 0.03) for x in components if sign(x, 0.03) != 0]
    agreement = abs(sum(active_signs)) / len(active_signs) if active_signs else 0.0

    action_direction = 1 if "BUY" in action else -1 if "SELL" in action else 0
    confirmations = 0
    checks = 0
    for component in [trend_score + momentum_score, macd_score, breakout_score + volume_score, news_score]:
        comp_sign = sign(component, 0.03)
        if comp_sign != 0:
            checks += 1
            if comp_sign == action_direction:
                confirmations += 1
    chart_confirmation_ratio = confirmations / checks if checks else 0.0

    overheat_penalty = 0.0
    if action_direction > 0 and rsi14 > 74:
        overheat_penalty = 8.0
    if action_direction < 0 and rsi14 < 26:
        overheat_penalty = 8.0

    confidence = clamp(
        42
        + abs(total_score) * 27
        + agreement * 16
        + news["quality_score"]
        + min(max(vol_ratio - 1.0, 0.0) * 8.0, 8.0)
        + chart_confirmation_ratio * 8.0
        - contradiction_penalty * 70
        - overheat_penalty,
        30,
        95,
    )

    max_asset_pct = meta["max_asset_pct"]
    if action == "STRONG BUY":
        add_now_pct = round(clamp((confidence - 60) / 4.5, 2.5, 6.0), 1)
        suggested_position_pct = round(min(max_asset_pct, add_now_pct + 4.5), 1)
        hold_window = "12-72 hours while price stays above invalidation and signal stays Buy"
        stop_loss = current * (1 - stop_distance_pct / 100)
        take_profit_1 = current * (1 + (stop_distance_pct * 1.4) / 100)
        take_profit_2 = current * (1 + (stop_distance_pct * 2.8) / 100)
        plan = f"Add about {add_now_pct:.1f}% of portfolio now, but keep total {snapshot['symbol']} exposure under {suggested_position_pct:.1f}% of portfolio."
        exit_rule = "Trim 40-50% at TP1, move stop to breakeven, then let the rest run while the signal stays Buy."
    elif action == "BUY":
        add_now_pct = round(clamp((confidence - 52) / 7.0, 1.0, 3.5), 1)
        suggested_position_pct = round(min(max_asset_pct, add_now_pct + 3.0), 1)
        hold_window = "6-24 hours while momentum and news stay supportive"
        stop_loss = current * (1 - stop_distance_pct / 100)
        take_profit_1 = current * (1 + (stop_distance_pct * 1.2) / 100)
        take_profit_2 = current * (1 + (stop_distance_pct * 2.2) / 100)
        plan = f"Starter size only: about {add_now_pct:.1f}% of portfolio now, with total {snapshot['symbol']} exposure capped near {suggested_position_pct:.1f}% of portfolio."
        exit_rule = "Take some profit at TP1 or exit early if the signal slips back to Hold or Sell."
    elif action == "HOLD":
        suggested_position_pct = round(min(max_asset_pct, 6.0 if snapshot["coin_id"] == "bitcoin" else 5.0), 1)
        hold_window = "Recheck on the next bot update"
        stop_loss = current * (1 - stop_distance_pct / 100)
        take_profit_1 = current * (1 + (stop_distance_pct * 1.0) / 100)
        take_profit_2 = current * (1 + (stop_distance_pct * 2.0) / 100)
        plan = f"Do not add fresh size here. Hold only a modest position and keep total {snapshot['symbol']} exposure around or below {suggested_position_pct:.1f}% of portfolio."
        exit_rule = "Wait for a cleaner Buy or Sell."
    elif action == "SELL":
        trim_pct = round(clamp((confidence - 48) / 5.5, 20.0, 50.0), 0)
        suggested_position_pct = round(min(max_asset_pct, 3.0), 1)
        hold_window = "Reduce risk now and reassess within 2-12 hours"
        stop_loss = current * (1 + stop_distance_pct / 100)
        take_profit_1 = current * (1 - (stop_distance_pct * 1.2) / 100)
        take_profit_2 = current * (1 - (stop_distance_pct * 2.2) / 100)
        plan = f"If you already hold {snapshot['symbol']}, consider trimming about {int(trim_pct)}% of the position and avoid fresh buys until the signal improves."
        exit_rule = "If price bounces above the invalidation level, stop pressing the short idea and reassess."
    else:
        trim_pct = round(clamp((confidence - 55) / 4.0, 35.0, 70.0), 0)
        suggested_position_pct = 0.0
        hold_window = "Exit fast and reassess within the next few hours"
        stop_loss = current * (1 + stop_distance_pct / 100)
        take_profit_1 = current * (1 - (stop_distance_pct * 1.4) / 100)
        take_profit_2 = current * (1 - (stop_distance_pct * 2.8) / 100)
        plan = f"High-risk setup. If you already hold {snapshot['symbol']}, consider cutting about {int(trim_pct)}% now and keep remaining exposure very small."
        exit_rule = "Preserve capital first. Only re-enter if the signal recovers to Hold or Buy."

    rationale = []
    if breakout_up and vol_ratio > 1.2:
        rationale.append("breakout with volume confirmation")
    elif breakout_down and vol_ratio > 1.2:
        rationale.append("breakdown with volume confirmation")
    if trend_gap_pct > 0.15:
        rationale.append("short trend is above the medium trend")
    elif trend_gap_pct < -0.15:
        rationale.append("short trend is below the medium trend")
    if rsi14 > 72:
        rationale.append("RSI looks stretched / overheated")
    elif rsi14 < 30:
        rationale.append("RSI looks washed out / oversold")
    if news["major_bull_count"]:
        rationale.append("high-impact bullish headlines are showing up")
    if news["major_bear_count"]:
        rationale.append("high-impact bearish headlines are showing up")
    if not rationale:
        rationale.append("signals are mixed and there is no clean edge yet")

    bullish_watch = False
    bearish_watch = False
    if news["major_headlines"]:
        top_major = news["major_headlines"][0]
        if top_major["weighted_score"] >= config.news_impact_alert_score and (confidence >= config.news_watch_confidence or chart_direction >= 0):
            bullish_watch = True
        if top_major["weighted_score"] <= -config.news_impact_alert_score and (confidence >= config.news_watch_confidence or chart_direction <= 0):
            bearish_watch = True

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
        "macd_line": macd_line,
        "macd_signal": macd_signal,
        "macd_hist_pct": macd_hist_pct,
        "hourly_vol_pct": hourly_vol_pct,
        "volume_ratio": vol_ratio,
        "recent_high": recent_high,
        "recent_low": recent_low,
        "news": news,
        "score": total_score,
        "action": action,
        "confidence": round(confidence, 1),
        "chart_confirmation_ratio": round(chart_confirmation_ratio, 2),
        "regime": market_regime(trend_gap_pct, rsi14, vol_ratio, change_60m),
        "plan": plan,
        "hold_window": hold_window,
        "stop_loss": stop_loss,
        "take_profit_1": take_profit_1,
        "take_profit_2": take_profit_2,
        "exit_rule": exit_rule,
        "rationale": rationale,
        "bullish_watch": bullish_watch,
        "bearish_watch": bearish_watch,
        "thresholds": {
            "5m": config.base_5m_pct * (1.0 if snapshot["coin_id"] == "bitcoin" else 1.2),
            "15m": config.base_15m_pct * (1.0 if snapshot["coin_id"] == "bitcoin" else 1.2),
            "60m": config.base_60m_pct * (1.0 if snapshot["coin_id"] == "bitcoin" else 1.15),
            "24h": config.base_24h_pct * (1.0 if snapshot["coin_id"] == "bitcoin" else 1.15),
        },
    }


def build_status_intro(analyses: List[Dict], config: Config) -> str:
    parts = ["🤖 BTC + ETH signal bot is live", ""]
    for analysis in analyses:
        snap = analysis["snapshot"]
        parts.append(
            f"{snap['symbol']} {fmt_price(snap['price'])} | 24h {fmt_pct(analysis['change_24h'])} | {analysis['action']} ({analysis['confidence']:.0f}% conf) | {analysis['regime']}"
        )
    parts.extend([
        "",
        "Confidence is the bot's internal signal confidence, not a profit guarantee.",
        f"Urgent BUY/SELL alerts fire around {config.signal_alert_confidence}% confidence or higher.",
        f"Major news-watch alerts can fire from about {config.news_watch_confidence}% when the headlines look market-moving.",
    ])
    return "\n".join(parts)


def build_analysis_message(analysis: Dict) -> str:
    snap = analysis["snapshot"]
    headlines = analysis["news"]["headlines"][:3]
    headline_block = "\n".join(
        [f"• {item['title'][:74]} — {item['source']}" for item in headlines]
    ) or "• No strong headlines picked up"

    return (
        f"{snap['name']} ({snap['symbol']})\n"
        f"Signal: {analysis['action']}\n"
        f"Confidence: {analysis['confidence']:.0f}%\n"
        f"Price: {fmt_price(snap['price'])}\n"
        f"5m / 15m / 60m / 24h: {fmt_pct(analysis['change_5m'])} / {fmt_pct(analysis['change_15m'])} / {fmt_pct(analysis['change_60m'])} / {fmt_pct(analysis['change_24h'])}\n"
        f"RSI14: {analysis['rsi14']:.1f} | EMA20 vs EMA50 gap: {analysis['trend_gap_pct']:+.2f}%\n"
        f"Regime: {analysis['regime']} | Vol ratio: {analysis['volume_ratio']:.2f}x | News sentiment: {analysis['news']['sentiment']:+.2f} from {analysis['news']['article_count']} headlines\n"
        f"Chart confirmation: {analysis['chart_confirmation_ratio'] * 100:.0f}%\n"
        "\n"
        f"Plan: {analysis['plan']}\n"
        f"Hold window: {analysis['hold_window']}\n"
        f"Stop / invalidation: {fmt_price(analysis['stop_loss'])}\n"
        f"TP1 / TP2: {fmt_price(analysis['take_profit_1'])} / {fmt_price(analysis['take_profit_2'])}\n"
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
        f"{emoji} {snap['symbol']} fast move\n"
        f"Window: {window}\n"
        f"Move: {fmt_pct(move_pct)}\n"
        f"Now: {fmt_price(snap['price'])}\n"
        f"Then: {fmt_price(baseline_price)}\n"
        f"Signal: {analysis['action']} ({analysis['confidence']:.0f}% conf)"
    )


def build_signal_alert(analysis: Dict) -> str:
    snap = analysis["snapshot"]
    hot = "🚨" if "STRONG" in analysis["action"] else "⚠️"
    top_headline = analysis["news"]["major_headlines"][0] if analysis["news"]["major_headlines"] else None
    headline_line = f"\nNews trigger: {top_headline['title'][:95]} — {top_headline['source']}" if top_headline else ""
    return (
        f"{hot} {snap['symbol']} urgent {analysis['action']} alert\n"
        f"Confidence: {analysis['confidence']:.0f}%\n"
        f"Price: {fmt_price(snap['price'])}\n"
        f"Regime: {analysis['regime']}\n"
        f"Plan: {analysis['plan']}\n"
        f"Stop / invalidation: {fmt_price(analysis['stop_loss'])}\n"
        f"TP1 / TP2: {fmt_price(analysis['take_profit_1'])} / {fmt_price(analysis['take_profit_2'])}\n"
        f"Why: {', '.join(analysis['rationale'])}."
        f"{headline_line}"
    )


def build_news_watch_alert(analysis: Dict, direction: str) -> str:
    snap = analysis["snapshot"]
    headline = analysis["news"]["major_headlines"][0]
    if direction == "bullish":
        trigger = max(analysis["recent_high"] * 1.001, snap["price"] * 1.003)
        action_line = f"This is an early bullish heads-up. Watch for price to hold above about {fmt_price(trigger)}."
        emoji = "📰🚀"
    else:
        trigger = min(analysis["recent_low"] * 0.999, snap["price"] * 0.997)
        action_line = f"This is an early bearish heads-up. Watch for price to break below about {fmt_price(trigger)}."
        emoji = "📰💥"

    return (
        f"{emoji} {snap['symbol']} news-watch alert\n"
        f"Headline: {headline['title'][:115]}\n"
        f"Source: {headline['source']}\n"
        f"Signal now: {analysis['action']} ({analysis['confidence']:.0f}% conf)\n"
        f"Price: {fmt_price(snap['price'])}\n"
        f"Regime: {analysis['regime']} | Chart confirmation: {analysis['chart_confirmation_ratio'] * 100:.0f}%\n"
        f"Why it matters: the headline scored as high-impact and could move the market.\n"
        f"{action_line}"
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
                f"{'🚀' if analysis['change_24h'] > 0 else '💥'} {snap['symbol']} big day move\n"
                f"24h move: {fmt_pct(analysis['change_24h'])}\n"
                f"Price: {fmt_price(snap['price'])}\n"
                f"Signal: {analysis['action']} ({analysis['confidence']:.0f}% conf)",
            )
            record_alert(last_alerts, key, now)
            alerts_sent += 1
    return alerts_sent


def maybe_send_signal_alert(config: Config, state: Dict, analysis: Dict, now: datetime) -> int:
    action = analysis["action"]
    if action not in {"BUY", "STRONG BUY", "SELL", "STRONG SELL"}:
        return 0

    snap = analysis["snapshot"]
    last_recs = state.setdefault("last_recommendations", {})
    rec_key = snap["coin_id"]
    previous = last_recs.get(rec_key, {})
    last_alerts = state.setdefault("last_alerts", {})
    alert_key = f"signal_{snap['coin_id']}_{action.lower().replace(' ', '_')}"

    changed_action = previous.get("action") != action
    meaningful_confidence_jump = abs(previous.get("confidence", 0) - analysis["confidence"]) >= 6
    allowed_by_confidence = analysis["confidence"] >= config.signal_alert_confidence
    flip_override = changed_action and analysis["confidence"] >= config.signal_flip_min_confidence
    if not (allowed_by_confidence or flip_override):
        return 0

    if not (changed_action or meaningful_confidence_jump) and not can_send(last_alerts, alert_key, now, config.cooldown_minutes * 2):
        return 0

    send_telegram_message(config, build_signal_alert(analysis))
    record_alert(last_alerts, alert_key, now)
    return 1


def maybe_send_news_watch_alert(config: Config, state: Dict, analysis: Dict, now: datetime) -> int:
    if not analysis["news"]["major_headlines"]:
        return 0

    direction = None
    if analysis["bullish_watch"]:
        direction = "bullish"
    elif analysis["bearish_watch"]:
        direction = "bearish"
    if direction is None:
        return 0

    top = analysis["news"]["major_headlines"][0]
    seen = state.setdefault("seen_news", {})
    last_alerts = state.setdefault("last_alerts", {})
    alert_key = f"news_watch_{analysis['snapshot']['coin_id']}_{direction}_{top['id']}"

    if top["id"] in seen:
        return 0
    if not can_send(last_alerts, f"news_watch_{analysis['snapshot']['coin_id']}_{direction}", now, config.cooldown_minutes * 2):
        return 0

    send_telegram_message(config, build_news_watch_alert(analysis, direction))
    seen[top["id"]] = dt_to_iso(now)
    record_alert(last_alerts, f"news_watch_{analysis['snapshot']['coin_id']}_{direction}", now)
    return 1


def maybe_send_daily_summary(config: Config, state: Dict, analyses: List[Dict], now: datetime) -> int:
    if now.hour != config.daily_summary_utc_hour:
        return 0
    day_key = now.strftime("%Y-%m-%d")
    sent = state.setdefault("daily_summary_sent", {})
    if sent.get(day_key):
        return 0
    parts = ["🗞️ Daily BTC / ETH signal check", ""]
    for analysis in analyses:
        snap = analysis["snapshot"]
        parts.append(
            f"{snap['symbol']} — {analysis['action']} ({analysis['confidence']:.0f}% conf) | 24h {fmt_pct(analysis['change_24h'])} | {analysis['regime']}"
        )
        parts.append(f"Plan: {analysis['plan']}")
        parts.append(f"Stop: {fmt_price(analysis['stop_loss'])} | TP1: {fmt_price(analysis['take_profit_1'])}")
        if analysis["news"]["major_headlines"]:
            top = analysis["news"]["major_headlines"][0]
            parts.append(f"Top news: {top['title'][:75]} — {top['source']}")
        parts.append("")
    parts.append("Confidence is model confidence, not a guarantee.")
    send_telegram_message(config, "\n".join(parts))
    sent[day_key] = True
    return 1


def main() -> None:
    config = load_config()
    now = utc_now()
    state = load_state()
    prune_state(state, now)

    snapshots = get_market_snapshot(config)
    analyses = []
    charts = {}
    for coin_id in ["bitcoin", "ethereum"]:
        chart = get_market_chart(config, coin_id)
        charts[coin_id] = chart
        news = get_asset_news(coin_id, state) if config.enable_news else {
            "article_count": 0,
            "sentiment": 0.0,
            "max_impulse": 0.0,
            "quality_score": 0.0,
            "major_headlines": [],
            "major_bull_count": 0,
            "major_bear_count": 0,
            "headlines": [],
        }
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
        alerts_sent += maybe_send_news_watch_alert(config, state, analysis, now)
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
                        "news_major": len(a["news"]["major_headlines"]),
                    }
                    for a in analyses
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
