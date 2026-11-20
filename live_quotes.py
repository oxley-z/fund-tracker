#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""盘中轻量行情快照生成器 —— 只负责「几分钟就能刷」的那部分数据。

与 main.py 的分工：
  * main.py（每晚 22:30）负责全量：150+ 只基金净值、ETF 份额资金流、CFTC、
    季报持仓、指数 PE —— 这些一天只有一次，不能 15 分钟刷一遍。
  * 本脚本（交易时段每 15 分钟）只取宏观 12 项 + 20 只场内宽基的现价/涨跌幅/溢价。

设计约束：
  1. 只依赖 requests，不 import main.py，不碰 pandas / akshare ——
     CI 里 `pip install requests` 就能跑，冷启动 1 秒内完成。
  2. 单源超时 5 秒、失败重试 2 次；任一源失败不让整轮失败。
  3. 整轮一个都没取到 → 保留上次的文件并标 stale，退出码仍为 0
     （GitHub Actions 里不该因为上游临时抽风就变红）。
  4. 输出 data/live_quotes.json，页面只读这一个文件。

用法：
    python live_quotes.py                 # 写 data/live_quotes.json
    python live_quotes.py --out x.json    # 指定输出路径
    python live_quotes.py --no-proxy      # 忽略系统代理（CI 用）
    python live_quotes.py --quiet
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

import requests

BEIJING_TZ = timezone(timedelta(hours=8))
OUT_PATH = os.path.join("data", "live_quotes.json")

TIMEOUT = 5           # 单源超时（秒）
RETRIES = 2           # 失败重试次数（合计最多 3 次尝试）
MIN_INTERVAL = 0.2    # 两次请求之间的最小间隔，避免瞬时打爆上游

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "*/*",
}
SINA_HEADERS = {**HEADERS, "Referer": "https://finance.sina.com.cn/"}
EM_HEADERS = {**HEADERS, "Referer": "https://quote.eastmoney.com/"}
GT_HEADERS = {**HEADERS, "Referer": "https://gu.qq.com/"}

# ── 场内宽基 ETF：dual_etf_dashboard.py 里的 19 只 + 501225（国泰中证有色） ──
ETF_CODES = [
    "510300", "510310", "510330", "510050", "510500",
    "512100", "510180", "560010", "588080",          # 上交所 9 只
    "159919", "159915", "159949", "159901", "159922",
    "159903", "159907", "159906", "159908", "159912",  # 深交所 10 只
    "501225",                                         # 上交所 LOF
]

# ── 宏观指标键：与 main.py 里卡片 data-live-key 一一对应 ──
MACRO_KEYS = [
    "fng", "vix", "vxn", "vxsmh", "skew", "usdcny",
    "gold_london", "gold_shfe", "silver_london",
    "copper_lme", "brent", "btc",
]

_lock = threading.Lock()
_last_request_at = 0.0


def now_beijing() -> datetime:
    return datetime.now(BEIJING_TZ)


def log(msg: str, quiet: bool = False) -> None:
    if not quiet:
        print(msg, flush=True)


def _throttle() -> None:
    """全局轻节流：保证并发线程不会在同一瞬间砸向上游。"""
    global _last_request_at
    with _lock:
        wait = MIN_INTERVAL - (time.time() - _last_request_at)
        if wait > 0:
            time.sleep(wait)
        _last_request_at = time.time()


def _request(session: requests.Session, url: str, *, params=None,
             headers=None, timeout: int = TIMEOUT) -> requests.Response:
    """带重试的 GET。重试次数用尽后抛最后一个异常，由调用方决定兜底。"""
    last_err: Optional[Exception] = None
    for attempt in range(RETRIES + 1):
        _throttle()
        try:
            resp = session.get(url, params=params, headers=headers or HEADERS,
                               timeout=timeout)
            if resp.status_code == 429:
                # 被限流就直接放弃本轮，不做升级重试（旧文件会兜底）
                resp.raise_for_status()
            resp.raise_for_status()
            return resp
        except Exception as e:  # noqa: BLE001 - 上游异常类型太杂，统一兜
            last_err = e
            if attempt < RETRIES:
                time.sleep(0.8 * (attempt + 1) + random.random() * 0.4)
    raise last_err  # type: ignore[misc]


def _json(session, url, **kw) -> Any:
    return _request(session, url, **kw).json()


def _text(session, url, encoding: str = "utf-8", **kw) -> str:
    resp = _request(session, url, **kw)
    resp.encoding = encoding
    return resp.text


def _to_float(val: Any) -> Optional[float]:
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    return f if f != 0 else None


# ==============================================================================
# 宏观：每个 getter 返回 float 或 None（None 由调用方用上一轮的值补）
# ==============================================================================
def _yahoo(session, symbol: str) -> Optional[float]:
    data = _json(
        session,
        f"https://query1.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(symbol)}",
        params={"interval": "1d", "range": "5d"},
    )
    result = (data.get("chart", {}).get("result") or [{}])[0]
    price = result.get("meta", {}).get("regularMarketPrice")
    if price is None:
        closes = (result.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
        valid = [c for c in closes if c is not None]
        price = valid[-1] if valid else None
    return _to_float(price)


def _cboe_quote(session, symbol: str) -> Optional[float]:
    data = _json(
        session,
        f"https://cdn.cboe.com/api/global/delayed_quotes/quotes/{symbol}.json",
        headers={**HEADERS, "Referer": "https://www.cboe.com/"},
    )
    d = data.get("data", {})
    return _to_float(d.get("last_trade_price") or d.get("current_price") or d.get("close"))


def _cboe_csv_last(session, symbol: str) -> Optional[float]:
    """CBOE 日线 CSV 的最后一行收盘价（SKEW / VXSMH 只有这个口子）。"""
    text = _text(
        session,
        f"https://cdn.cboe.com/api/global/us_indices/daily_prices/{symbol}_History.csv",
        headers={**HEADERS, "Referer": "https://www.cboe.com/"},
    )
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return None
    header = [h.strip().strip('"').upper() for h in lines[0].split(",")]
    idx = header.index("CLOSE") if "CLOSE" in header else len(header) - 1
    if symbol.upper() in header:
        idx = header.index(symbol.upper())
    for ln in reversed(lines[1:]):
        parts = ln.split(",")
        if len(parts) > idx:
            val = _to_float(parts[idx])
            if val:
                return val
    return None


def _sina_quote(session, symbol: str) -> list[str]:
    text = _text(session, f"https://hq.sinajs.cn/list={symbol}", encoding="gbk",
                 headers=SINA_HEADERS)
    match = re.search(r'"([^"]*)"', text)
    return match.group(1).split(",") if match else []


def get_fng(session) -> Optional[float]:
    data = _json(
        session,
        "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
        headers={
            **HEADERS,
            "Referer": "https://edition.cnn.com/markets/fear-and-greed",
            "Origin": "https://edition.cnn.com",
            "Accept": "application/json, text/plain, */*",
        },
        timeout=6,
    )
    return _to_float((data.get("fear_and_greed") or {}).get("score"))


def get_vix(session) -> Optional[float]:
    for fn in (lambda: _cboe_quote(session, "_VIX"),
               lambda: _yahoo(session, "^VIX")):
        try:
            v = fn()
            if v:
                return round(v, 2)
        except Exception:
            pass
    parts = _sina_quote(session, "gb_$vix")
    return round(_to_float(parts[1]), 2) if len(parts) > 1 and _to_float(parts[1]) else None


def get_vxn(session) -> Optional[float]:
    for fn in (lambda: _cboe_quote(session, "_VXN"),
               lambda: _yahoo(session, "^VXN")):
        try:
            v = fn()
            if v:
                return round(v, 2)
        except Exception:
            pass
    return None


def get_skew(session) -> Optional[float]:
    for fn in (lambda: _cboe_csv_last(session, "SKEW"),
               lambda: _yahoo(session, "^SKEW"),
               lambda: _cboe_quote(session, "_SKEW")):
        try:
            v = fn()
            if v:
                return round(v, 2)
        except Exception:
            pass
    return None


def get_vxsmh(session) -> Optional[float]:
    for fn in (lambda: _cboe_csv_last(session, "VXSMH"),
               lambda: _cboe_quote(session, "_VXSMH")):
        try:
            v = fn()
            if v:
                return round(v, 2)
        except Exception:
            pass
    return None


def get_usdcny(session) -> Optional[float]:
    parts = _sina_quote(session, "fx_susdcny")
    for idx in (1, 8):
        if len(parts) > idx:
            v = _to_float(parts[idx])
            if v:
                return round(v, 4)
    try:
        data = _json(session, "https://open.er-api.com/v6/latest/USD")
        v = _to_float((data.get("rates") or {}).get("CNY"))
        if v:
            return round(v, 4)
    except Exception:
        pass
    try:
        v = _yahoo(session, "USDCNY=X")
        if v:
            return round(v, 4)
    except Exception:
        pass
    return None


def get_gold_london(session) -> Optional[float]:
    try:
        v = _yahoo(session, "GC=F")
        if v:
            return round(v, 2)
    except Exception:
        pass
    parts = _sina_quote(session, "hf_XAU")
    v = _to_float(parts[0]) if parts else None
    return round(v, 2) if v else None


def get_gold_shfe(session) -> Optional[float]:
    parts = _sina_quote(session, "nf_AU0")
    for idx in (8, 7, 6, 5):
        if len(parts) > idx:
            v = _to_float(parts[idx])
            if v:
                return round(v, 2)
    return None


def get_silver_london(session) -> Optional[float]:
    try:
        v = _yahoo(session, "SI=F")
        if v:
            return round(v, 3)
    except Exception:
        pass
    parts = _sina_quote(session, "hf_XAG")
    v = _to_float(parts[0]) if parts else None
    return round(v, 3) if v else None


def get_copper_lme(session) -> Optional[float]:
    try:
        v = _yahoo(session, "HG=F")
        if v:
            return round(v, 2)
    except Exception:
        pass
    parts = _sina_quote(session, "hf_CAD")
    v = _to_float(parts[0]) if parts else None
    # 新浪 CAD 是美元/吨，统一换算美元/磅
    return round(v / 2204.62, 2) if v else None


def get_brent(session) -> Optional[float]:
    try:
        v = _yahoo(session, "BZ=F")
        if v:
            return round(v, 2)
    except Exception:
        pass
    parts = _sina_quote(session, "hf_OIL")
    v = _to_float(parts[0]) if parts else None
    return round(v, 2) if v else None


def get_btc(session) -> Optional[float]:
    sources = [
        ("https://data-api.binance.vision/api/v3/ticker/price?symbol=BTCUSDT",
         lambda j: j.get("price")),
        ("https://api.coinbase.com/v2/prices/BTC-USD/spot",
         lambda j: (j.get("data") or {}).get("amount")),
        ("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd",
         lambda j: (j.get("bitcoin") or {}).get("usd")),
    ]
    for url, parser in sources:
        try:
            v = _to_float(parser(_json(session, url, timeout=6)))
            if v:
                return round(v, 2)
        except Exception:
            continue
    return None


MACRO_GETTERS: dict[str, Callable[[requests.Session], Optional[float]]] = {
    "fng": get_fng,
    "vix": get_vix,
    "vxn": get_vxn,
    "vxsmh": get_vxsmh,
    "skew": get_skew,
    "usdcny": get_usdcny,
    "gold_london": get_gold_london,
    "gold_shfe": get_gold_shfe,
    "silver_london": get_silver_london,
    "copper_lme": get_copper_lme,
    "brent": get_brent,
    "btc": get_btc,
}


# ==============================================================================
# 场内 ETF：东财 push2 一次批量拿 20 只
# ==============================================================================
def _secid(code: str) -> str:
    """东财 secid：1.=沪市，0.=深市。5/6/9 开头的在沪市。"""
    return ("1." if code[:1] in ("5", "6", "9") else "0.") + code


def _gt_code(code: str) -> str:
    """腾讯行情代码：sh/sz 前缀。5/6/9 开头在沪市。"""
    return ("sh" if code[:1] in ("5", "6", "9") else "sz") + code


def fetch_etf_iopv(session: requests.Session) -> dict[str, dict]:
    """腾讯行情：一次批量取 20 只的 IOPV 与溢价率。

    交易所 IOPV 的免费口子只剩这一个（都实测过）：
      * 东财 push2 —— 没有任何 IOPV 字段，`f71` 是均价
        （成交额 18,175,530,912 ÷ 45,417,170 手 ×100 = 4.0021 = f71，股票也能算出同样的值）
      * 新浪 hq.sinajs.cn —— 字段里去没有 IOPV
      * 上交所云行情 yunhq.sse.com.cn:32041 —— `total:0 / list:[]`，不对外开放
      * 集思录 etf_list —— 不带 cookie 时 iopv 全为 "-"

    qt.gtimg.cn 的 `~` 分隔字段（实测 510300 / 159915 逐只核对）：
        [3] 现价    [32] 涨跌幅%    [77] 溢价率%（正=溢价）    [78] IOPV
    510300：4.018 / -1.03 / -0.31 / 4.0305，
    (4.018-4.0305)/4.0305 = -0.310%，与 [77] 完全吻合；159915 的 +0.16% 同样对得上。

    LOF（501225）交易所不发布 IOPV，[78] 为空 —— 该只不带溢价，不拿别的数顶替。
    返回 {代码: {name, price, change_pct, iopv, premium_pct}}。
    """
    query = ",".join(_gt_code(c) for c in ETF_CODES)
    text = _text(session, f"https://qt.gtimg.cn/q={query}",
                 encoding="gbk", headers=GT_HEADERS, timeout=6)
    wanted = set(ETF_CODES)
    out: dict[str, dict] = {}
    for line in text.split(";"):
        if "=" not in line:
            continue
        body = line.split("=", 1)[1].strip().strip('"')
        fields = body.split("~")
        if len(fields) < 79:
            continue
        code = fields[2].strip()
        if code not in wanted:
            continue
        price = _to_float(fields[3])
        iopv = _to_float(fields[78])
        reported = _to_float(fields[77])
        premium: Optional[float] = None
        if price is not None and iopv:
            calc = (price - iopv) / iopv * 100
            # 官方口径与自己算的差超过 0.1 个点，说明这条 IOPV 是脏的，
            # 宁可留空也不往页面上写个错的溢价
            if reported is None or abs(calc - reported) <= 0.1:
                premium = round(calc, 3)
            else:
                iopv = None
        out[code] = {
            "name": fields[1].strip(),
            "price": price,
            "change_pct": _to_float(fields[32]),
            "iopv": round(iopv, 4) if iopv else None,
            "premium_pct": premium,
        }
    return out


def _em_etf_rows(session: requests.Session) -> tuple[list[dict], str]:
    """东财 push2 一次批量拿 20 只的现价/涨跌幅/昨收。失败时抛异常。"""
    fields = "f2,f3,f4,f12,f13,f14,f18,f124"
    data = _json(
        session,
        "https://push2.eastmoney.com/api/qt/ulist.np/get",
        params={
            "fltt": 2, "invt": 2, "fields": fields,
            "secids": ",".join(_secid(c) for c in ETF_CODES),
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        },
        headers=EM_HEADERS,
    )
    rows = ((data.get("data") or {}).get("diff") or [])
    out: list[dict] = []
    latest_ts = 0
    for row in rows:
        code = str(row.get("f12") or "").strip()
        if not code:
            continue
        ts = row.get("f124") or 0
        if isinstance(ts, (int, float)) and ts > latest_ts:
            latest_ts = int(ts)
        out.append({
            "code": code,
            "name": str(row.get("f14") or "").strip(),
            "price": _to_float(row.get("f2")),
            "change_pct": _to_float(row.get("f3")),
            "prev_close": _to_float(row.get("f18")),
            "iopv": None,
            "premium_pct": None,
        })
    trade_date = ""
    if latest_ts:
        trade_date = datetime.fromtimestamp(latest_ts, BEIJING_TZ).strftime("%Y-%m-%d")
    return out, trade_date


def fetch_etf_quotes(session: requests.Session,
                     quiet: bool = False) -> tuple[list[dict], str, list[str]]:
    """ETF 行情 = 东财现价 + 腾讯 IOPV。两源互为兜底。

    东财整块失败（历史上被 IP 级限流过）时用腾讯的现价顶上，不至于整块空；
    腾讯挂了只是少掉溢价那一列，价格照常。返回 (行列表, 数据日期, 失败源名)。
    """
    failed: list[str] = []
    em_rows: list[dict] = []
    trade_date = ""

    try:
        em_rows, trade_date = _em_etf_rows(session)
    except Exception as e:  # noqa: BLE001
        failed.append("etf_eastmoney")
        log(f"  [warn] ETF 东财行情失败: {type(e).__name__}: {e}", quiet)

    gt_map: dict[str, dict] = {}
    try:
        gt_map = fetch_etf_iopv(session)
        if not gt_map:
            failed.append("etf_iopv")
    except Exception as e:  # noqa: BLE001
        failed.append("etf_iopv")
        log(f"  [warn] ETF IOPV 失败: {type(e).__name__}: {e}", quiet)

    by_code = {r["code"]: r for r in em_rows}
    for code, gt in gt_map.items():
        row = by_code.get(code)
        if row is None:
            # 东财没返回这只（整块失败或个别漏掉）→ 用腾讯的现价建行
            row = {
                "code": code,
                "name": gt.get("name") or "",
                "price": gt.get("price"),
                "change_pct": gt.get("change_pct"),
                "prev_close": None,
                "iopv": None,
                "premium_pct": None,
            }
            by_code[code] = row
            em_rows.append(row)
        if row.get("price") is None:
            row["price"] = gt.get("price")
        if row.get("change_pct") is None:
            row["change_pct"] = gt.get("change_pct")
        row["iopv"] = gt.get("iopv")
        # 溢价率按「页面最终展示的那个价格」重算，否则两个源的现价有分毫差异时，
        # 页面上的涨价和溢价会对不上。重算不出数（LOF 无 IOPV）才退回腾讯口径。
        iopv = row.get("iopv")
        price = row.get("price")
        if iopv and price is not None:
            row["premium_pct"] = round((price - iopv) / iopv * 100, 3)
        else:
            row["premium_pct"] = gt.get("premium_pct")

    for row in em_rows:
        row.setdefault("iopv", None)
        row.setdefault("premium_pct", None)

    order = {c: i for i, c in enumerate(ETF_CODES)}
    em_rows.sort(key=lambda r: order.get(r["code"], 999))
    return em_rows, trade_date, failed


# ==============================================================================
# 主流程
# ==============================================================================
def load_previous(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def fetch_all(quiet: bool = False) -> tuple[dict, dict, list[str], str]:
    """返回 (quotes, etf 元信息, 失败键列表, ETF 数据日期)。"""
    session = requests.Session()
    quotes: dict[str, Optional[float]] = {}
    failed: list[str] = []

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {
            ex.submit(fn, session): key for key, fn in MACRO_GETTERS.items()
        }
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                val = fut.result(timeout=TIMEOUT * (RETRIES + 1) + 10)
            except Exception as e:  # noqa: BLE001
                log(f"  [warn] 宏观 {key}: {type(e).__name__}: {e}", quiet)
                val = None
            if val is None:
                failed.append(key)
            quotes[key] = val

    try:
        etf_rows, trade_date, etf_failed = fetch_etf_quotes(session, quiet=quiet)
    except Exception as e:  # noqa: BLE001
        log(f"  [warn] ETF 批量行情失败: {type(e).__name__}: {e}", quiet)
        etf_rows, trade_date, etf_failed = [], "", ["etf"]
    if not etf_rows:
        failed.append("etf")
    else:
        failed.extend(etf_failed)

    # 保持 MACRO_KEYS 的固定顺序，方便人眼 diff
    quotes = {k: quotes.get(k) for k in MACRO_KEYS}
    return quotes, {"rows": etf_rows}, failed, trade_date


def main() -> int:
    parser = argparse.ArgumentParser(description="生成盘中轻量行情 JSON")
    parser.add_argument("--out", default=OUT_PATH, help="输出路径")
    parser.add_argument("--no-proxy", action="store_true",
                        help="忽略系统代理环境变量（CI 默认行为）")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if args.no_proxy:
        for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
                    "ALL_PROXY", "all_proxy"):
            os.environ.pop(var, None)

    started = time.time()
    previous = load_previous(args.out)
    prev_quotes = previous.get("quotes") or {}
    prev_etf = {r.get("code"): r for r in (previous.get("etf") or []) if isinstance(r, dict)}

    quotes, etf_meta, failed, trade_date = fetch_all(quiet=args.quiet)

    # 本轮「真正抓到」的数量要在继承旧值之前记下来，
    # 否则继承了上一轮的 ETF 之后，「颗粒无收」就永远判不出来了。
    fresh_macro = sum(1 for k in MACRO_KEYS if quotes.get(k) is not None)
    fresh_etf = bool(etf_meta["rows"])

    # 单项失败 → 用上一轮的值补齐，并在 stale_keys 里留痕
    stale_keys: list[str] = []
    for key in MACRO_KEYS:
        if quotes.get(key) is None:
            old = prev_quotes.get(key)
            quotes[key] = old
            if old is not None:
                stale_keys.append(key)

    etf_rows = etf_meta["rows"]
    if not etf_rows:
        # ETF 整块失败：沿用上次的报价，只把 code 顺序对齐
        etf_rows = [prev_etf[c] for c in ETF_CODES if c in prev_etf]
        if etf_rows:
            stale_keys.append("etf")

    if fresh_macro == 0 and not fresh_etf:
        # 整轮颗粒无收：不覆盖旧文件，避免把好数据洗成空
        log("⛔ 本轮所有源均失败，保留上一次的 live_quotes.json（stale=true）", args.quiet)
        previous["stale"] = True
        previous["stale_reason"] = "all_sources_failed"
        previous["checked_at_beijing"] = now_beijing().strftime("%Y-%m-%d %H:%M:%S")
        _write(args.out, previous)
        return 0

    payload = {
        "updated_at_beijing": now_beijing().strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date": trade_date,
        "stale": bool(stale_keys),
        "stale_keys": stale_keys,
        "quotes": quotes,
        "etf": etf_rows,
    }
    _write(args.out, payload)

    elapsed = time.time() - started
    usable_macro = sum(1 for k in MACRO_KEYS if quotes.get(k) is not None)
    premium_n = sum(1 for r in etf_rows if r.get("premium_pct") is not None)
    log(
        f"✅ 宏观 {fresh_macro}/{len(MACRO_KEYS)} 项新数据（可用 {usable_macro}）"
        f" · ETF {len(etf_rows)}/{len(ETF_CODES)} 只（含溢价 {premium_n}）"
        f" · 失败 {('、'.join(failed) or '无')} · 耗时 {elapsed:.1f}s"
        f" → {args.out}",
        args.quiet,
    )
    return 0


def _write(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
        f.write("\n")
    os.replace(tmp, path)


if __name__ == "__main__":
    sys.exit(main())
