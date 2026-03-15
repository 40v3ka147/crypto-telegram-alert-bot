# BTC Telegram Alert Bot

This bot checks BTC on a schedule and sends Telegram alerts when the move is big enough.

## Files

- `btc_alert_bot.py` — the Python script
- `.github/workflows/btc-alert.yml` — the scheduler that runs on GitHub
- `requirements.txt` — Python package list
- `state/btc_state.json` — saved price history and cooldown state

## What it alerts on by default

- 5-minute move >= 1.0%
- 15-minute move >= 2.0%
- 60-minute move >= 3.5%
- 24-hour move >= 6.0%
- 30-minute cooldown between same-type alerts

## GitHub variables you can add later

- `ALERT_5M_PCT`
- `ALERT_15M_PCT`
- `ALERT_60M_PCT`
- `ALERT_24H_PCT`
- `COOLDOWN_MINUTES`

## GitHub secrets you must add

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `COINGECKO_API_KEY`
