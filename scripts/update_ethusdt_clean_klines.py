#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import sys

import pandas as pd  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLEAN_ROOT = Path(os.environ.get("LLM_KLINE_CLEAN_ROOT", str(ROOT / "data" / "clean" / "ethusdt_perp")))
BINANCE_FAPI = "https://fapi.binance.com"
TZ_BJT = "Asia/Shanghai"

TIMEFRAMES = {
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "8h": "8h",
    "12h": "12h",
    "1d": "1D",
    "3d": "3D",
    "1w": "W-MON",
    "1mo": "MS",
}

TAIL_1M_ROWS = 30_000


def parse_bjt(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z") or "+" in text[-6:] or "-" in text[-6:]:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    dt = datetime.fromisoformat(text)
    return dt.replace(tzinfo=timezone(timedelta(hours=8))).astimezone(timezone.utc)


def floor_minute(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(second=0, microsecond=0)


def latest_closed_open_time() -> datetime:
    return floor_minute(datetime.now(timezone.utc)) - timedelta(minutes=1)


def iso_bjt_index(index: pd.DatetimeIndex) -> pd.Index:
    return index.tz_convert(TZ_BJT).astype(str)


def fetch_binance_1m(symbol: str, start_ms: int, end_ms: int) -> list[list[Any]]:
    rows: list[list[Any]] = []
    cursor = start_ms
    while cursor <= end_ms:
        query = urllib.parse.urlencode(
            {
                "symbol": symbol.upper(),
                "interval": "1m",
                "startTime": cursor,
                "endTime": end_ms,
                "limit": 1500,
            }
        )
        url = f"{BINANCE_FAPI}/fapi/v1/klines?{query}"
        last_error: Exception | None = None
        for attempt in range(1, 6):
            try:
                with urllib.request.urlopen(url, timeout=30) as resp:  # noqa: S310
                    batch = json.loads(resp.read().decode("utf-8"))
                break
            except Exception as exc:
                last_error = exc
                time.sleep(min(10.0, attempt * 1.5))
        else:
            raise RuntimeError(f"Binance K线下载失败 cursor={cursor}: {last_error}")
        if not batch:
            break
        max_ms = cursor
        for item in batch:
            ms = int(item[0])
            if start_ms <= ms <= end_ms:
                rows.append([ms, item[1], item[2], item[3], item[4], item[5]])
                max_ms = max(max_ms, ms)
        next_cursor = max_ms + 60_000
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        time.sleep(0.08)
    return rows


def read_clean_1m(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, compression="gzip")
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df = df.set_index("timestamp_utc").sort_index()
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df[["open", "high", "low", "close", "volume"]].dropna()
    df = df[~df.index.duplicated(keep="last")]
    return df


def fetched_to_df(rows: list[list[Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"], index=pd.DatetimeIndex([], tz="UTC", name="timestamp_utc"))
    df = pd.DataFrame(rows, columns=["timestamp_ms", "open", "high", "low", "close", "volume"])
    df["timestamp_utc"] = pd.to_datetime(pd.to_numeric(df["timestamp_ms"]), unit="ms", utc=True)
    df = df.set_index("timestamp_utc").sort_index()
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[["open", "high", "low", "close", "volume"]].dropna()


def resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    return (
        df[["open", "high", "low", "close", "volume"]]
        .resample(rule, label="right", closed="left")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna(subset=["open", "high", "low", "close"])
    )


def write_csv_gz(df: pd.DataFrame, path: Path) -> None:
    out = df.copy()
    out.insert(0, "timestamp_utc", out.index.astype(str))
    out.insert(1, "timestamp_bjt", iso_bjt_index(out.index))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        out.to_csv(tmp_path, index=False, compression="gzip")
        with gzip.open(tmp_path, "rb") as fp:
            while fp.read(1024 * 1024):
                pass
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def audit(df: pd.DataFrame) -> dict[str, Any]:
    first_ts = df.index.min()
    last_ts = df.index.max()
    expected = int((last_ts - first_ts) / pd.Timedelta(minutes=1)) + 1
    missing = max(0, expected - len(df))
    diffs = df.index.to_series().diff().dropna()
    gaps = diffs[diffs > pd.Timedelta(minutes=1)]
    overlaps = diffs[diffs < pd.Timedelta(minutes=1)]
    high_ok = bool((df["high"] >= df[["open", "close"]].max(axis=1)).all())
    low_ok = bool((df["low"] <= df[["open", "close"]].min(axis=1)).all())
    high_low_ok = bool((df["high"] >= df["low"]).all())
    negative_volume = int((df["volume"] < 0).sum())
    status = "OK"
    if missing or len(gaps):
        status = "CHECK"
    if not high_ok or not low_ok or not high_low_ok or negative_volume or len(overlaps):
        status = "FAIL"
    return {
        "clean_rows_1m": int(len(df)),
        "start_utc": str(first_ts),
        "end_utc": str(last_ts),
        "start_bjt": str(first_ts.tz_convert(TZ_BJT)),
        "end_bjt": str(last_ts.tz_convert(TZ_BJT)),
        "expected_rows_by_range": int(expected),
        "missing_1m_bars": int(missing),
        "gap_count": int(len(gaps)),
        "max_gap_minutes": float(gaps.max() / pd.Timedelta(minutes=1)) if len(gaps) else 0.0,
        "overlap_count": int(len(overlaps)),
        "high_ge_max_open_close": high_ok,
        "low_le_min_open_close": low_ok,
        "high_ge_low": high_low_ok,
        "negative_volume_count": negative_volume,
        "status": status,
    }


def update_clean(symbol: str, clean_root: Path, target_utc: datetime | None) -> dict[str, Any]:
    clean_root.mkdir(parents=True, exist_ok=True)
    path_1m = clean_root / f"{symbol.upper()}-1m-clean.csv.gz"
    if not path_1m.exists():
        raise FileNotFoundError(f"缺少 1m clean 文件: {path_1m}")
    current = read_clean_1m(path_1m)
    last_open = current.index.max().to_pydatetime()
    target_open = target_utc or latest_closed_open_time()
    target_open = floor_minute(target_open)
    if target_open > datetime.now(timezone.utc):
        raise ValueError("目标时间不能超过当前时间")
    fetched_rows: list[list[Any]] = []
    if target_open > last_open:
        start_ms = int((last_open + timedelta(minutes=1)).timestamp() * 1000)
        end_ms = int(target_open.timestamp() * 1000)
        fetched_rows = fetch_binance_1m(symbol, start_ms, end_ms)
        fetched = fetched_to_df(fetched_rows)
        clean = pd.concat([current, fetched]).sort_index()
        clean = clean[~clean.index.duplicated(keep="last")]
    else:
        clean = current
    report = audit(clean)
    if report["status"] == "FAIL":
        raise RuntimeError(f"数据质量失败: {report}")
    write_csv_gz(clean, path_1m)
    generated = [path_1m.name]
    tail_path = clean_root / f"{symbol.upper()}-1m-tail-clean.csv.gz"
    write_csv_gz(clean.tail(TAIL_1M_ROWS), tail_path)
    generated.append(tail_path.name)
    for tf, rule in TIMEFRAMES.items():
        out_path = clean_root / f"{symbol.upper()}-{tf}-clean.csv.gz"
        write_csv_gz(resample_ohlcv(clean, rule), out_path)
        generated.append(out_path.name)
    report_path = clean_root / "data_quality_report.csv"
    pd.DataFrame([{"raw_rows": int(len(clean)), **report}]).to_csv(report_path, index=False)
    meta = {
        "symbol": symbol.upper(),
        "updated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "target_open_utc": target_open.isoformat(timespec="seconds"),
        "target_open_bjt": str(pd.Timestamp(target_open).tz_convert(TZ_BJT)),
        "generated_files": generated,
        "timeframes": TIMEFRAMES,
        "no_lookahead_policy": [
            "1m timestamps are Binance open times; use close_time=open_time+1m when filtering completed 1m bars.",
            "Resampled candles are right-labelled by candle close time with closed='left'.",
            "UI filters out partial high-timeframe rows beyond the latest complete 1m source time.",
        ],
        "data_quality_report": "data_quality_report.csv",
    }
    (clean_root / "baseline_metadata.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {
        "symbol": symbol.upper(),
        "clean_root": str(clean_root),
        "last_open_before_utc": last_open.isoformat(timespec="seconds"),
        "target_open_utc": target_open.isoformat(timespec="seconds"),
        "fetched_1m_rows": len(fetched_rows),
        "report": report,
        "generated_files": generated,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="更新 ETHUSDT clean K线并重建多周期。")
    parser.add_argument("--symbol", default="ETHUSDT")
    parser.add_argument("--clean-root", default=str(DEFAULT_CLEAN_ROOT))
    parser.add_argument("--target-bjt", default="", help="可选，北京时间，例如 2026-06-16T09:00；不填则更新到最新已收盘 1m。")
    args = parser.parse_args()
    target = parse_bjt(args.target_bjt) if args.target_bjt else None
    result = update_clean(args.symbol, Path(args.clean_root), target)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
