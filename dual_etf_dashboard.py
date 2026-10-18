"""
沪深两市宽基 ETF 份额监控（断点续传 + 断网保存 + 增量更新）
支持上海证券交易所(SSE)和深圳证券交易所(SZSE)
依赖安装：pip install requests plotly openpyxl pandas

运行方式：python dual_etf_dashboard.py
脚本启动时自动判断运行模式：

  【历史下载模式】无 checkpoint 或历史尚未抓完
    从今天/断点起向过去方向逐日抓至 2022-01-01
    每成功一天立即写断点；Ctrl+C 或断网自动保存

  【增量更新模式】checkpoint 存在且 note 含"完成"
    找到已有数据的最新日期，从次日起正向抓到今天
    新数据追加合并，重新生成 HTML + Excel

输出：web/dual_final_dashboard.html  交互看板（已并入 main.py 主页面）
      data/dual_checkpoint.json     进度文件（永久保留）
      data/shindex_checkpoint.json  上证指数缓存
"""

import requests
import json
import time
import os
import re
import sys
import warnings
import webbrowser
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ── 1. 配置 ────────────────────────────────────────────────────────────────────

# 上海证券交易所 ETF (原有9只)
SSE_ETF_MAP = {
    '510300': '华泰柏瑞沪深300ETF',
    '510310': '易方达沪深300ETF',
    '510330': '华夏沪深300ETF',
    '510050': '华夏上证50ETF',
    '510500': '南方中证500ETF',
    '512100': '南方中证1000ETF',
    '510180': '华安上证180ETF',
    '560010': '广发中证1000ETF',
    '588080': '易方达上证科创板50ETF',
}

# 深圳证券交易所 ETF (前十名)
SZSE_ETF_MAP = {
    '159919': '嘉实沪深300ETF',
    '159915': '易方达创业板ETF',
    '159949': '华安创业板50ETF',
    '159901': '易方达深证100ETF',
    '159922': '嘉实中证500ETF',
    '159903': '南方深成ETF',
    '159907': '广发中小板300ETF',
    '159906': '大成深证成长40ETF',
    '159908': '博时深证基本面200ETF',
    '159912': '汇添富深证300ETF',
}

TARGET_DAYS      = 1500
CUTOFF_DATE      = datetime(2022, 1, 1)
OUTPUT_HTML      = os.path.join('web', 'dual_final_dashboard.html')
CHECKPOINT       = os.path.join('data', 'dual_checkpoint.json')
os.makedirs(os.path.dirname(OUTPUT_HTML), exist_ok=True)
os.makedirs(os.path.dirname(CHECKPOINT), exist_ok=True)
MAX_NET_FAILURES = 5
NET_RETRY_WAIT   = 3
CHECKPOINT_VERSION = 2
SZSE_HISTORY_CHUNK_DAYS = 170

# 请求头
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer':    'https://www.sse.com.cn/',
    'Accept':     '*/*',
}

# 上证指数配置
SHINDEX_CHECKPOINT = os.path.join('data', 'shindex_checkpoint.json')
SHINDEX_SOHU_URL   = (
    'http://q.stock.sohu.com/hisHq'
    '?code=zs_000001'
    '&start=20220101'
    '&end=99991231'
    '&stat=1&order=D&period=d'
    '&callback=historySearchHandler&rt=jsonp'
)
SHINDEX_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer':    'http://q.stock.sohu.com/',
    'Accept':     '*/*',
}
SHARES_PER_YI = {
    # 上交所规模表单位为万份；深交所日频规模表单位为份。
    'SSE': 10_000.0,
    'SZSE': 100_000_000.0,
}

# ── 2. 断点工具 ────────────────────────────────────────────────────────────────

def load_checkpoint():
    if not os.path.exists(CHECKPOINT):
        return [], None
    try:
        with open(CHECKPOINT, 'r', encoding='utf-8') as f:
            data = json.load(f)
        results   = data.get('results', [])
        last_date = data.get('last_date', None)
        note      = data.get('note', '')
        print(f'📂 断点文件：{len(results)} 个交易日 | 最早 {last_date} | 备注: {note}')
        return results, last_date
    except Exception as e:
        print(f'  [警告] 断点文件读取失败，重新开始: {e}')
        return [], None


def save_checkpoint(results, note=''):
    """原子写入断点文件，永久保留。"""
    if not results:
        return
    dates     = [r['date'] for r in results]
    last_date = min(dates)   # 最早（历史最远）
    first_date = max(dates)  # 最新（最近今天）

    # 过滤：仅保留 ETF_MAP 中的 ETF，且只存 SEC_CODE + TOT_VOL 字段
    tracked_codes = set(list(SSE_ETF_MAP.keys()) + list(SZSE_ETF_MAP.keys()))
    slim_results = []
    for day in results:
        slim_items = []
        for item in day.get('items', []):
            code = str(item.get('SEC_CODE', '')).strip()
            if code in tracked_codes:
                slim_items.append({
                    'SEC_CODE': code,
                    'TOT_VOL':  item.get('TOT_VOL'),
                    'exchange': item.get('exchange', 'SSE'),  # 标记交易所
                })
        slim_results.append({'date': day['date'], 'items': slim_items})

    tmp = CHECKPOINT + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump({
            'version':    CHECKPOINT_VERSION,
            'sources':    {'SSE': 'official_etf_scale', 'SZSE': 'official_daily_scale'},
            'last_date':  last_date,
            'first_date': first_date,
            'count':      len(slim_results),
            'saved_at':   datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'note':       note,
            'results':    slim_results,
        }, f, ensure_ascii=False, separators=(',', ':'))
    os.replace(tmp, CHECKPOINT)


def today_str():
    """返回当天日期字符串 YYYY-MM-DD。"""
    return datetime.today().strftime('%Y-%m-%d')


def get_existing_dates():
    """返回含上交所真实数据的交易日集合，排除周末误采集记录。"""
    if not os.path.exists(CHECKPOINT):
        return set()
    try:
        with open(CHECKPOINT, 'r', encoding='utf-8') as f:
            results = json.load(f).get('results', [])
        return {
            r['date'] for r in results
            if any(str(item.get('SEC_CODE', '')).strip() in SSE_ETF_MAP
                   for item in r.get('items', []))
        }
    except Exception:
        return set()


def read_latest_date():
    """读取 checkpoint 中最新（最大）的日期。"""
    try:
        with open(CHECKPOINT, 'r', encoding='utf-8') as f:
            return json.load(f).get('first_date', None)
    except Exception:
        return None


def read_all_results():
    try:
        with open(CHECKPOINT, 'r', encoding='utf-8') as f:
            return json.load(f).get('results', [])
    except Exception:
        return []


def read_checkpoint_version():
    try:
        with open(CHECKPOINT, 'r', encoding='utf-8') as f:
            return int(json.load(f).get('version', 1))
    except Exception:
        return 1


def is_weekday(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').weekday() < 5
    except (TypeError, ValueError):
        return False

# ── 3. 网络请求 ────────────────────────────────────────────────────────────────

def is_network_error(e):
    msg = str(e).lower()
    return any(k in msg for k in (
        'connectionerror', 'timeout', 'max retries', 'connection reset',
        'remotedisconnected', 'network', 'proxy', 'nodename nor servname',
        'name or service not known', 'failed to establish',
    ))


def fetch_sse_day(date_str):
    """获取上海证券交易所ETF数据"""
    ts  = int(time.time() * 1000)
    url = (
        'https://query.sse.com.cn/commonQuery.do'
        f'?isPagination=true&pageHelp.pageSize=1000'
        f'&sqlId=COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L'
        f'&STAT_DATE={date_str}&_{ts}'
    )
    try:
        resp  = requests.get(url, headers=HEADERS, timeout=10,
                             proxies={'http': None, 'https': None})
        resp.raise_for_status()
        items = resp.json().get('pageHelp', {}).get('data', [])
        # 标记为上交所数据
        for item in items:
            item['exchange'] = 'SSE'
        return (items, 'ok') if items else (None, 'nodata')
    except Exception as e:
        return (None, 'neterr') if is_network_error(e) else (None, 'apierr')


def fetch_szse_day(date_str):
    """获取深交所官方基金规模日频数据；仅接受总份额字段。"""
    if not is_weekday(date_str):
        return None, 'nodata'
    try:
        records = fetch_szse_range(date_str, date_str)
    except Exception as e:
        return (None, 'neterr') if is_network_error(e) else (None, 'apierr')
    if records is None:
        return None, 'neterr'
    items = records.get(date_str, [])
    return (items, 'ok') if items else (None, 'nodata')


def _szse_scale_chunk(start_str, end_str):
    """读取一个不超过六个月的深交所官方份额区间。"""
    url = 'https://www.szse.cn/api/report/ShowReport'
    params = {
        'SHOWTYPE': 'xlsx',
        'CATALOGID': 'scsj_fund_jjgm',
        'TABKEY': 'tab1',
        'txtStart': start_str,
        'txtEnd': end_str,
        'jjlb': 'ETF',
        'random': '0.123456789',
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.szse.cn/market/fund/volume/etf/index.html',
        'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*',
    }
    resp = requests.get(url, params=params, headers=headers, timeout=45,
                        proxies={'http': None, 'https': None})
    resp.raise_for_status()
    if not resp.content:
        return {}
    with warnings.catch_warnings():
        warnings.filterwarnings(
            'ignore',
            message='Workbook contains no default style.*',
            category=UserWarning,
        )
        df = pd.read_excel(BytesIO(resp.content), dtype={'基金代码': str})
    column_map = {str(column).strip(): column for column in df.columns}
    date_column = column_map.get('日期')
    code_column = column_map.get('基金代码')
    share_column = next((
        column_map[name] for name in ('基金份额', '基金规模(份)', '基金规模')
        if name in column_map
    ), None)
    if date_column is None or code_column is None or share_column is None:
        raise ValueError(f'深交所份额表字段异常: {list(df.columns)}')

    result = {}
    for _, row in df.iterrows():
        code = str(row.get(code_column, '')).strip().split('.')[0].zfill(6)
        if code not in SZSE_ETF_MAP:
            continue
        date_value = pd.to_datetime(row.get(date_column), errors='coerce')
        share_value = parse_val(row.get(share_column))
        if pd.isna(date_value) or share_value is None:
            continue
        date_key = date_value.strftime('%Y-%m-%d')
        result.setdefault(date_key, []).append({
            'SEC_CODE': code,
            'TOT_VOL': share_value,
            'exchange': 'SZSE',
        })
    return result


def fetch_szse_range(start_str, end_str):
    """分块获取深交所官方日频份额，返回 date -> items。"""
    try:
        start = datetime.strptime(start_str, '%Y-%m-%d')
        end = datetime.strptime(end_str, '%Y-%m-%d')
    except ValueError:
        return None
    if end < start:
        return {}

    all_records = {}
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=SZSE_HISTORY_CHUNK_DAYS), end)
        chunk = _szse_scale_chunk(
            cursor.strftime('%Y-%m-%d'),
            chunk_end.strftime('%Y-%m-%d'),
        )
        for date_key, items in chunk.items():
            all_records.setdefault(date_key, []).extend(items)
        print(f'   [深交所] {cursor:%Y-%m-%d} ~ {chunk_end:%Y-%m-%d} '
              f'获取 {len(chunk)} 个交易日')
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.25)
    return all_records


def refresh_szse_history(results):
    """
    将旧断点中的深交所备用接口/模拟数据替换为官方日频总份额。
    旧数据保留到新数据完整获取成功后才原子替换。
    """
    valid_dates = sorted({
        r['date'] for r in results
        if any(str(item.get('SEC_CODE', '')).strip() in SSE_ETF_MAP
               for item in r.get('items', []))
    })
    if not valid_dates:
        return results

    print(f'▶  修复深交所历史口径：{valid_dates[0]} ~ {valid_dates[-1]}')
    try:
        official = fetch_szse_range(valid_dates[0], valid_dates[-1])
    except Exception as e:
        print(f'  [警告] 深交所官方数据获取失败，保留旧断点: {str(e)[:120]}')
        return results
    if not official:
        print('  [警告] 未获取到深交所官方数据，保留旧断点。')
        return results

    coverage = len(set(official).intersection(valid_dates)) / max(len(valid_dates), 1)
    if coverage < 0.75:
        print(f'  [警告] 官方数据覆盖率仅 {coverage:.1%}，为避免丢失数据保留旧断点。')
        return results

    valid_date_set = set(valid_dates)
    merged = []
    for day in results:
        date_key = day.get('date')
        if date_key not in valid_date_set:
            continue
        sse_items = [
            item for item in day.get('items', [])
            if str(item.get('SEC_CODE', '')).strip() in SSE_ETF_MAP
        ]
        szse_items = official.get(date_key, [])
        if sse_items or szse_items:
            merged.append({'date': date_key, 'items': sse_items + szse_items})

    merged.sort(key=lambda x: x['date'], reverse=True)
    save_checkpoint(merged, note='深交所份额已切换为官方日频口径')
    print(f'✅ 深交所历史修复完成：{len(merged)} 个交易日，覆盖率 {coverage:.1%}')
    return merged


def fetch_combined_day(date_str, szse_records=None):
    """获取双交易所合并数据"""
    sse_items, sse_status = fetch_sse_day(date_str)
    if szse_records is None:
        szse_items, szse_status = fetch_szse_day(date_str)
    else:
        szse_items = szse_records.get(date_str, [])
        szse_status = 'ok' if szse_items else 'nodata'

    all_items = []
    if sse_status == 'ok' and sse_items:
        all_items.extend(sse_items)
    if szse_status == 'ok' and szse_items:
        all_items.extend(szse_items)

    if sse_status == 'neterr' or szse_status == 'neterr':
        return (None, 'neterr')
    elif sse_status == 'apierr' and szse_status == 'apierr':
        return (None, 'apierr')
    else:
        return (all_items, 'ok') if all_items else (None, 'nodata')

# ── 4. 通用抓取循环 ─────────────────────────────────────────────────────────

def fetch_dates(date_list, existing_dates, mode_label, is_recent=False):
    """对 date_list 中尚未在 existing_dates 里的日期逐一抓取。"""
    new_records    = []
    net_fail_count = 0
    stop_reason    = 'completed'
    pending_dates  = [
        date_str for date_str in date_list
        if date_str not in existing_dates and is_weekday(date_str)
    ]

    try:
        # 深交所支持区间下载。长区间先批量取数，避免逐日请求。
        szse_records = None
        if len(pending_dates) > 3:
            try:
                szse_records = fetch_szse_range(min(pending_dates), max(pending_dates))
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f'   [深交所] 区间预取失败，回退逐日获取: {str(e)[:100]}')

        for date_str in pending_dates:
            print(f'同步 {date_str} [{mode_label}，已新增 {len(new_records)} 天]', end='  ')
            items, status = fetch_combined_day(date_str, szse_records=szse_records)

            if status == 'ok':
                new_records.append({'date': date_str, 'items': items})
                net_fail_count = 0
                print('✓')

            elif status == 'nodata':
                net_fail_count = 0
                if is_recent:
                    print('— (数据未发布或非交易日)')
                else:
                    print('— (非交易日)')

            elif status == 'neterr':
                net_fail_count += 1
                print(f'✗ 网络异常 ({net_fail_count}/{MAX_NET_FAILURES})')
                if net_fail_count >= MAX_NET_FAILURES:
                    stop_reason = 'network'
                    break
                time.sleep(NET_RETRY_WAIT)
                continue

            else:
                print('— (API错误，跳过)')

            time.sleep(0.4)

    except KeyboardInterrupt:
        stop_reason = 'interrupt'
        print('\n\n⏸  Ctrl+C 捕获')

    return new_records, stop_reason

# ── 5. 历史下载模式 ────────────────────────────────────────────────────────────

def collect_history():
    """从今天/断点起向过去方向逐日抓取。"""
    results, last_date = load_checkpoint()
    existing_dates     = {r['date'] for r in results}

    if last_date:
        start = datetime.strptime(last_date, '%Y-%m-%d') - timedelta(days=1)
        print(f'▶  历史下载（续传），从 {start.strftime("%Y-%m-%d")} 继续向前')
    else:
        start = datetime.today()
        print(f'▶  历史下载（首次），从今天起向前追溯')

    print(f'   目标 {TARGET_DAYS} 个交易日 | 截止 {CUTOFF_DATE.strftime("%Y-%m-%d")}')
    print('   Ctrl+C 可随时中断并保存进度')
    print('=' * 60)

    # 生成待抓日期列表（倒序）
    date_list = []
    d = start
    while d >= CUTOFF_DATE and (len(existing_dates) + len(date_list)) < TARGET_DAYS:
        if d.weekday() < 5:
            date_list.append(d.strftime('%Y-%m-%d'))
        d -= timedelta(days=1)

    new_records, stop_reason = fetch_dates(date_list, existing_dates, '历史')

    # 合并
    for r in new_records:
        results.append(r)
        existing_dates.add(r['date'])
        save_checkpoint(results, note='运行中')

    completed = (stop_reason == 'completed')

    if stop_reason == 'interrupt':
        save_checkpoint(results, note='Ctrl+C 中断')
        print(f'✅ 已保存 {len(results)} 个交易日 → {CHECKPOINT}')
    elif stop_reason == 'network':
        save_checkpoint(results, note='断网自动保存')
        print(f'🔌 断网，已保存 {len(results)} 个交易日 → {CHECKPOINT}')
    else:
        save_checkpoint(results, note='全量采集完成')
        print(f'✅ 历史数据采集完成，共 {len(results)} 个交易日')

    return results, completed

# ── 6. 增量更新模式 ────────────────────────────────────────────────────────────

def incremental_update():
    """增量更新模式"""
    latest_str = read_latest_date()
    if not latest_str:
        print('❌ 无法读取已有最新日期，请检查断点文件。')
        return [], 0

    latest = datetime.strptime(latest_str, '%Y-%m-%d')
    today  = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    if latest >= today:
        print(f'✅ 数据已是最新（{latest_str}），无需更新。')
        return read_all_results(), 0

    # 生成待检查日期（正向）
    date_list, d = [], latest + timedelta(days=1)
    while d <= today:
        if d.weekday() < 5:
            date_list.append(d.strftime('%Y-%m-%d'))
        d += timedelta(days=1)

    if not date_list:
        print('✅ 当前日期之后没有新的工作日，无需更新。')
        return read_all_results(), 0

    print(f'▶  增量更新模式')
    print(f'   已有数据最新日期 : {latest_str}')
    print(f'   待检查范围       : {date_list[0]} ~ {date_list[-1]}')
    print('=' * 60)

    existing_results = read_all_results()
    existing_dates   = {r['date'] for r in existing_results}

    new_records, stop_reason = fetch_dates(date_list, existing_dates, '增量', is_recent=True)

    # 合并去重
    merged_map = {r['date']: r for r in existing_results}
    for r in new_records:
        merged_map[r['date']] = r
    merged_results = sorted(merged_map.values(), key=lambda x: x['date'], reverse=True)

    new_count = len(new_records)
    if new_count > 0:
        if stop_reason == 'completed':
            note = f'增量更新完成，新增 {new_count} 天'
        else:
            note = f'增量更新中断（{stop_reason}），已新增 {new_count} 天'
        save_checkpoint(merged_results, note=note)
        print(f'\n✅ 新增 {new_count} 个交易日，已合并保存')
    else:
        if stop_reason == 'completed':
            print(f'\n✅ 检查完毕，期间暂无新交易日数据。')
        else:
            print(f'\n⚠  更新中断（{stop_reason}），未获取到新数据。')

    return merged_results, new_count

# ── 7. 数据整理 ────────────────────────────────────────────────────────────────

def parse_val(v):
    """解析数值，支持多种格式"""
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        # 移除千分位分隔符和空格
        clean_v = str(v).replace(',', '').replace(' ', '').strip()
        # 处理百分比等特殊格式
        if '%' in clean_v:
            return float(clean_v.replace('%', '')) / 100
        # 处理带单位的数值
        if '万' in clean_v:
            return float(clean_v.replace('万', '')) * 10000
        if '亿' in clean_v:
            return float(clean_v.replace('亿', '')) * 100000000
        return float(clean_v)
    except (ValueError, AttributeError):
        return None


def share_to_yi(value, exchange):
    """将交易所原始份额统一折算为亿份。"""
    parsed = parse_val(value)
    divisor = SHARES_PER_YI.get(exchange)
    if parsed is None or not divisor:
        return None
    return parsed / divisor


def build_plot_data(results, index_prices=None):
    """
    构建 Plotly trace 列表。
    关键：所有 ETF trace 共用同一组 all_dates（按日期升序），
    上证指数 x 也必须与之完全对齐，避免 category 轴错位。
    """
    if not results:
        return []

    # ── 只保留真实交易日；有指数时以指数交易日为准 ───────────────────────────
    candidate_dates = {
        r['date'] for r in results
        if is_weekday(r.get('date', ''))
    }
    if index_prices:
        all_dates = sorted(candidate_dates.intersection(index_prices))
    else:
        all_dates = sorted(candidate_dates)
    if not all_dates:
        return []

    # ── 预建 date→items 索引 ─────────────────────────────────────────────────
    date_to_items = {}
    for r in results:
        date_to_items.setdefault(r['date'], []).extend(r.get('items', []))

    plot_data = []

    # ── 收集每只 ETF 在每个日期的值 ─────────────────────────────────────────
    sse_vals  = {code: {} for code in SSE_ETF_MAP}
    szse_vals = {code: {} for code in SZSE_ETF_MAP}

    for date, items in date_to_items.items():
        for item in items:
            code     = str(item.get('SEC_CODE', item.get('szse_code', ''))).strip()
            exchange = item.get('exchange', 'SSE')
            val      = share_to_yi(
                item.get('TOT_VOL', item.get('szse_share')),
                exchange,
            )
            if code in SSE_ETF_MAP and exchange == 'SSE' and val is not None:
                sse_vals[code][date] = val
            elif code in SZSE_ETF_MAP and exchange == 'SZSE' and val is not None:
                szse_vals[code][date] = val

    # ── 调色板 ───────────────────────────────────────────────────────────────
    sse_palette = ['#2563eb','#0f766e','#7c3aed','#0891b2','#4f46e5',
                   '#0284c7','#059669','#6d28d9','#0369a1']
    szse_palette = ['#d97706','#dc2626','#ea580c','#b45309','#be123c',
                    '#c2410c','#a21caf','#b91c1c','#92400e','#9a3412']

    # ── 上交所 traces ─────────────────────────────────────────────────────────
    for i, (code, name) in enumerate(SSE_ETF_MAP.items()):
        y_vals = [sse_vals[code].get(d, None) for d in all_dates]
        # 跳过完全没有数据的 ETF
        if all(v is None for v in y_vals):
            continue
        plot_data.append({
            'x':            all_dates,
            'y':            y_vals,
            'name':         f'{name}({code})',
            'short_name':   name,
            'code':         code,
            'mode':         'lines',
            'line':         {'width': 1.7, 'color': sse_palette[i % len(sse_palette)]},
            'connectgaps':  True,
            'yaxis':        'y',
            'exchange':     'SSE',
            'hovertemplate': f'<b>{name}({code})</b><br>%{{x}}<br>总份额: <b>%{{y:,.2f}}</b> 亿份<extra></extra>',
        })

    # ── 深交所 traces ─────────────────────────────────────────────────────────
    for i, (code, name) in enumerate(SZSE_ETF_MAP.items()):
        y_vals = [szse_vals[code].get(d, None) for d in all_dates]
        if all(v is None for v in y_vals):
            continue
        plot_data.append({
            'x':            all_dates,
            'y':            y_vals,
            'name':         f'{name}({code})',
            'short_name':   name,
            'code':         code,
            'mode':         'lines',
            'line':         {'width': 1.7, 'color': szse_palette[i % len(szse_palette)], 'dash': 'dot'},
            'connectgaps':  True,
            'yaxis':        'y',
            'exchange':     'SZSE',
            'hovertemplate': f'<b>{name}({code})</b><br>%{{x}}<br>总份额: <b>%{{y:,.2f}}</b> 亿份<extra></extra>',
        })

    # ── 上证指数（最后一条，右轴，x 完全对齐） ───────────────────────────────
    if index_prices and plot_data:
        idx_y = [index_prices.get(d, None) for d in all_dates]
        plot_data.append({
            'x':            all_dates,
            'y':            idx_y,
            'name':         '上证指数(000001)',
            'short_name':   '上证指数',
            'code':         '000001',
            'trace_kind':   'index',
            'mode':         'lines',
            'line':         {'width': 2.2, 'color': '#f85149'},
            'connectgaps':  True,
            'yaxis':        'y2',
            'hovertemplate': '<b>上证指数</b><br>%{x}<br>收盘: <b>%{y:,.2f}</b> 点<extra></extra>',
        })

    return plot_data


# ── 8. 上证指数数据抓取 ────────────────────────────────────────────────────────────

def fetch_shindex():
    """获取上证指数数据"""
    try:
        resp = requests.get(SHINDEX_SOHU_URL, headers=SHINDEX_HEADERS,
                            timeout=20, proxies={'http': None, 'https': None})
        resp.raise_for_status()
        text = resp.text.strip()
        m    = re.search(r'historySearchHandler\((.+)\)\s*;?\s*$', text, re.DOTALL)
        if not m:
            return {}
        payload = json.loads(m.group(1))
        hq = payload[0].get('hq', [])
        result = {}
        for row in hq:
            if len(row) >= 3:
                date_str = row[0]
                try:
                    close = float(str(row[2]).replace(',', ''))
                    result[date_str] = close
                except (ValueError, TypeError):
                    pass
        return result
    except Exception:
        return {}


def get_shindex_data():
    """智能获取上证指数数据"""
    cached = {}
    if os.path.exists(SHINDEX_CHECKPOINT):
        try:
            with open(SHINDEX_CHECKPOINT, 'r', encoding='utf-8') as f:
                data = json.load(f)
            cached = data.get('prices', {})
        except Exception:
            pass

    today  = today_str()
    latest = max(cached) if cached else None

    if latest and latest >= today:
        return cached

    fresh = fetch_shindex()
    if fresh:
        # 只有出现比缓存更新的日期时才重写，避免每天无数据也改动 saved_at 造成无意义提交
        newest_fresh = max(fresh)
        if latest is None or newest_fresh > latest:
            cached.update(fresh)
            tmp = SHINDEX_CHECKPOINT + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump({
                    'count':    len(cached),
                    'latest':   max(cached),
                    'earliest': min(cached),
                    'saved_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'prices':   cached,
                }, f, ensure_ascii=False)
            os.replace(tmp, SHINDEX_CHECKPOINT)
        return cached
    else:
        return cached

# ── 9. Excel 导出 ──────────────────────────────────────────────────────────────

def generate_excel(plot_data, output_path, index_prices=None):
    wb = Workbook()

    # 按交易所分组数据
    sse_data = {}
    szse_data = {}

    for trace in plot_data:
        if '上证指数' in trace['name']:
            continue

        exchange = trace.get('exchange', 'SSE')
        if exchange == 'SSE':
            sse_data[trace['name']] = dict(zip(trace['x'], trace['y']))
        else:
            szse_data[trace['name']] = dict(zip(trace['x'], trace['y']))

    # Sheet1: 上交所ETF数据
    ws1 = wb.active
    ws1.title = '上交所ETF'
    date_sets = [set(d.keys()) for d in sse_data.values()]
    date_sets += [set(d.keys()) for d in szse_data.values()]
    all_dates = sorted(set().union(*date_sets)) if date_sets else []

    # 表头
    ws1.cell(1, 1, '统计日期')
    for i, (name, data) in enumerate(sse_data.items(), 2):
        ws1.cell(1, i, f'{name}（亿份）')

    # 数据行
    for r, date in enumerate(sorted(all_dates), 2):
        ws1.cell(r, 1, date)
        for c, (name, data) in enumerate(sse_data.items(), 2):
            ws1.cell(r, c, data.get(date, None))

    # Sheet2: 深交所ETF数据
    ws2 = wb.create_sheet('深交所ETF')
    ws2.cell(1, 1, '统计日期')
    for i, (name, data) in enumerate(szse_data.items(), 2):
        ws2.cell(1, i, f'{name}（亿份）')

    for r, date in enumerate(sorted(all_dates), 2):
        ws2.cell(r, 1, date)
        for c, (name, data) in enumerate(szse_data.items(), 2):
            ws2.cell(r, c, data.get(date, None))

    # 最新快照便于直接排序和筛选。
    snapshot = []
    for trace in plot_data:
        if trace.get('exchange') not in ('SSE', 'SZSE'):
            continue
        y = trace.get('y', [])
        latest = next((v for v in reversed(y) if v is not None), None)
        prev = next((v for v in reversed(y[:-1]) if v is not None), None) if len(y) > 1 else None
        delta = latest - prev if latest is not None and prev is not None else None
        delta_pct = delta / prev * 100 if delta is not None and prev else None
        snapshot.append((
            trace.get('code', ''),
            trace.get('short_name', trace['name']),
            trace.get('exchange', ''),
            latest,
            delta,
            delta_pct,
        ))
    snapshot.sort(key=lambda row: row[3] if row[3] is not None else float('-inf'), reverse=True)
    ws_snapshot = wb.create_sheet('最新份额', 0)
    headers = ['基金代码', '基金简称', '交易所', '最新份额（亿份）',
               '单日变化（亿份）', '单日变化率']
    ws_snapshot.append(headers)
    for rank, row in enumerate(snapshot, 2):
        ws_snapshot.append([row[0], row[1], row[2], row[3], row[4], row[5]])
        ws_snapshot.cell(rank, 6).number_format = '0.00%'
    ws_snapshot.freeze_panes = 'A2'
    for column, width in {'A': 12, 'B': 24, 'C': 10, 'D': 20, 'E': 20, 'F': 14}.items():
        ws_snapshot.column_dimensions[column].width = width

    # Sheet3: 上证指数
    if index_prices:
        ws3 = wb.create_sheet('上证指数')
        ws3.cell(1, 1, '统计日期')
        ws3.cell(1, 2, '收盘点位')
        for r, (date, price) in enumerate(sorted(index_prices.items()), 2):
            ws3.cell(r, 1, date)
            ws3.cell(r, 2, price)

    wb.save(output_path)
    print(f'✅ Excel 已生成：{output_path}')

# ── 10. HTML 生成 ───────────────────────────────────────────────────────────────

DUAL_THEME_CSS = """
  /* ============ 🌙 深色主题（与主看板联动） ============ */
  [data-theme="dark"]{
    color-scheme:dark;
    --bg:#14161a;--surface:#1b1e24;--surface-soft:#20242c;--border:#2c313a;
    --text:#d7dae0;--muted:#8b93a1;--blue:#69a8ff;--teal:#2dd4bf;--amber:#f5a524;
    --red:#ff7b72;--green:#56d364;--shadow:0 1px 2px rgba(0,0,0,.35)
  }
  [data-theme="dark"] .segmented{background:#171a20}
  [data-theme="dark"] th,[data-theme="dark"] td{border-bottom-color:#262b33}
  [data-theme="dark"] tbody tr:hover{background:#232a36}
  [data-theme="dark"] .exchange-tag.SSE{background:#1d2b3d;color:#7cc4ff}
  [data-theme="dark"] .exchange-tag.SZSE{background:#33290f;color:#ffc069}
  [data-theme="dark"] ::-webkit-scrollbar{width:10px;height:10px}
  [data-theme="dark"] ::-webkit-scrollbar-track{background:#171a20}
  [data-theme="dark"] ::-webkit-scrollbar-thumb{background:#3a4049;border-radius:5px}
"""

DUAL_THEME_JS = """<script>
/* 主题联动：?theme= 参数 / 主看板 postMessage / localStorage */
(function () {
  function normalize(v) { return v === 'dark' ? 'dark' : (v === 'light' ? 'light' : null); }
  function fromQuery() { try { return normalize(new URLSearchParams(location.search).get('theme')); } catch (e) { return null; } }
  function fromHash() { var m = /(?:^|[#&])theme=(dark|light)/.exec(location.hash || ''); return m ? m[1] : null; }
  function fromStorage() { try { return normalize(localStorage.getItem('theme')); } catch (e) { return null; } }
  function apply(t) {
    t = normalize(t) || 'light';
    document.documentElement.setAttribute('data-theme', t);
    document.documentElement.style.colorScheme = t;
    try { localStorage.setItem('theme', t); } catch (e) {}
    if (typeof window.__dualThemeChanged === 'function') {
      try { window.__dualThemeChanged(); } catch (e) {}
    }
  }
  window.dualSetTheme = apply;
  apply(fromQuery() || fromHash() || fromStorage() || 'light');
  window.addEventListener('message', function (ev) {
    var d = ev && ev.data;
    if (d === null || d === undefined) return;
    if (typeof d === 'string') { apply(d); return; }
    if (d.type && d.type !== 'cftc-theme') return;
    apply(d.theme);
  });
  window.addEventListener('storage', function (ev) {
    if (ev && ev.key === 'theme') apply(ev.newValue);
  });
})();
</script>"""

DUAL_THEME_TAIL = """<script>
/* 暗色下让 Plotly 图表跟随主题（在 layoutConfig 定义之后包裹一次） */
(function () {
  var orig = window.layoutConfig;
  if (typeof orig !== 'function') return;
  window.layoutConfig = function () {
    var cfg = orig();
    if (document.documentElement.getAttribute('data-theme') !== 'dark') return cfg;
    var cs = getComputedStyle(document.documentElement);
    var v = function (n, d) { var x = cs.getPropertyValue(n).trim(); return x || d; };
    var surface = v('--surface', '#1b1e24');
    var muted = v('--muted', '#8b93a1');
    var border = v('--border', '#2c313a');
    var text = v('--text', '#d7dae0');
    var red = v('--red', '#ff7b72');
    cfg.paper_bgcolor = surface;
    cfg.plot_bgcolor = surface;
    cfg.font.color = muted;
    cfg.hoverlabel = { bgcolor: text, bordercolor: text, font: { size: 12, color: surface } };
    if (cfg.legend && cfg.legend.font) cfg.legend.font.color = muted;
    ['xaxis', 'yaxis', 'yaxis2'].forEach(function (k) {
      var ax = cfg[k];
      if (!ax) return;
      if (ax.gridcolor !== undefined) ax.gridcolor = border;
      if (ax.linecolor !== undefined) ax.linecolor = border;
      if (ax.tickcolor !== undefined) ax.tickcolor = border;
      var c = (k === 'yaxis2') ? red : muted;
      if (ax.tickfont) ax.tickfont.color = c;
      if (ax.title && ax.title.font) ax.title.font.color = c;
    });
    return cfg;
  };
  window.__dualThemeChanged = function () {
    if (typeof drawChart === 'function') { try { drawChart(); } catch (e) {} }
  };
  window.__dualThemeChanged();
})();
</script>"""


def generate_html(plot_data, output_path, completed=True, index_prices=None):
    """生成份额总览、交互趋势和明细排行看板。"""
    import json as _json

    all_etf   = [t for t in plot_data if t.get('exchange') in ('SSE', 'SZSE')]
    sse_cnt   = sum(1 for t in all_etf if t.get('exchange') == 'SSE')
    szse_cnt  = sum(1 for t in all_etf if t.get('exchange') == 'SZSE')
    dates     = all_etf[0]['x'] if all_etf else []
    date_from = dates[0]  if dates else '暂无'
    date_to   = dates[-1] if dates else '暂无'
    day_count = len(dates)

    def total_at(index):
        if index < 0 or index >= day_count:
            return None
        values = [
            trace['y'][index] for trace in all_etf
            if len(trace.get('y', [])) > index and trace['y'][index] is not None
        ]
        return sum(values) if values else None

    latest_total = sum(
        t['y'][-1] for t in all_etf
        if t.get('y') and t['y'][-1] is not None
    ) if all_etf else 0
    prev_total = sum(
        t['y'][-2] for t in all_etf
        if len(t.get('y', [])) > 1 and t['y'][-2] is not None
    ) if all_etf else 0
    total_delta = latest_total - prev_total if prev_total else 0
    total_delta_pct = total_delta / prev_total * 100 if prev_total else 0
    week_base_total = total_at(day_count - 6)
    month_base_total = total_at(day_count - 21)
    week_delta = latest_total - week_base_total if week_base_total else None
    month_delta = latest_total - month_base_total if month_base_total else None
    week_delta_pct = week_delta / week_base_total * 100 if week_delta is not None else None
    month_delta_pct = month_delta / month_base_total * 100 if month_delta is not None else None

    top = max(
        (t for t in all_etf if t.get('y') and t['y'][-1] is not None),
        key=lambda t: t['y'][-1],
        default=None,
    )
    top_value = top['y'][-1] if top else 0
    top_name = top.get('short_name', top['name']) if top else '暂无'

    daily_moves = []
    for trace in all_etf:
        y = trace.get('y', [])
        if len(y) >= 2 and y[-1] is not None and y[-2] is not None:
            daily_moves.append((y[-1] - y[-2], trace))
    move_up = max(daily_moves, default=(0, None), key=lambda item: item[0])
    move_down = min(daily_moves, default=(0, None), key=lambda item: item[0])

    idx_trace = next((t for t in plot_data if t.get('trace_kind') == 'index'), None)
    idx_latest, idx_chg, idx_pct = 0, 0, 0
    if idx_trace:
        valid_idx = [(i, v) for i, v in enumerate(idx_trace['y']) if v is not None]
        if valid_idx:
            idx_latest = valid_idx[-1][1]
            if len(valid_idx) > 1:
                idx_prev = valid_idx[-2][1]
                idx_chg = idx_latest - idx_prev
                idx_pct = idx_chg / idx_prev * 100 if idx_prev else 0

    status_tag = '' if completed else '（数据更新中）'
    status_text = '数据采集完成' if completed else '数据仍在更新'
    delta_class = 'positive' if total_delta >= 0 else 'negative'
    delta_sign = '+' if total_delta >= 0 else ''
    week_class = 'positive' if week_delta is not None and week_delta >= 0 else 'negative'
    month_class = 'positive' if month_delta is not None and month_delta >= 0 else 'negative'
    idx_class = 'positive' if idx_chg >= 0 else 'negative'
    idx_sign = '+' if idx_chg >= 0 else ''
    latest_total_str = f'{latest_total:,.1f}'
    total_delta_str = f'{delta_sign}{total_delta:,.1f}'
    total_delta_pct_str = f'{delta_sign}{total_delta_pct:.2f}%'
    week_delta_str = (
        f'{"+" if week_delta >= 0 else ""}{week_delta:,.1f}'
        if week_delta is not None else '暂无'
    )
    month_delta_str = (
        f'{"+" if month_delta >= 0 else ""}{month_delta:,.1f}'
        if month_delta is not None else '暂无'
    )
    week_delta_pct_str = (
        f'{"+" if week_delta_pct >= 0 else ""}{week_delta_pct:.2f}%'
        if week_delta_pct is not None else '暂无'
    )
    month_delta_pct_str = (
        f'{"+" if month_delta_pct >= 0 else ""}{month_delta_pct:.2f}%'
        if month_delta_pct is not None else '暂无'
    )
    idx_latest_str = f'{idx_latest:,.2f}' if idx_latest else '暂无'
    idx_change_str = (
        f'{idx_sign}{idx_chg:,.2f} 点 · {idx_sign}{idx_pct:.2f}%'
        if idx_latest else '暂无当日指数数据'
    )
    move_up_value, move_up_trace = move_up
    move_down_value, move_down_trace = move_down
    move_up_name = move_up_trace.get('short_name', '暂无') if move_up_trace else '暂无'
    move_down_name = move_down_trace.get('short_name', '暂无') if move_down_trace else '暂无'

    RAW_JSON = _json.dumps(plot_data, ensure_ascii=False)

    # Use a plain string with __RAW_DATA__ sentinel to avoid f-string brace conflicts
    TMPL = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>沪深宽基 ETF 份额看板</title>
__DUAL_THEME_JS__
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  :root{
    --bg:#f4f6f8;--surface:#ffffff;--surface-soft:#f8fafc;--border:#dfe5ea;
    --text:#17212b;--muted:#66717d;--blue:#1f63d6;--teal:#087f75;--amber:#b45309;
    --red:#c43d3d;--green:#137a4b;--radius:8px;--shadow:0 1px 2px rgba(16,24,40,.04)
  }
  *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
  body{font-family:"Microsoft YaHei","PingFang SC","Noto Sans CJK SC",sans-serif;background:var(--bg);color:var(--text);min-height:100vh;padding:24px;font-size:13px}
  .page{max-width:1560px;margin:0 auto}
  .header{display:flex;align-items:flex-start;justify-content:space-between;gap:24px;padding-bottom:18px;border-bottom:1px solid var(--border)}
  .header h1{font-size:24px;line-height:1.25;font-weight:650;letter-spacing:0}
  .header p{font-size:12px;color:var(--muted);margin-top:7px}
  .badges{display:flex;gap:7px;flex-wrap:wrap;justify-content:flex-end}
  .badge{display:inline-flex;align-items:center;gap:6px;padding:5px 9px;border-radius:6px;border:1px solid var(--border);background:var(--surface);color:var(--muted);font-size:11px;white-space:nowrap}
  .dot{width:7px;height:7px;border-radius:50%;flex:0 0 auto}.dot.blue{background:var(--blue)}.dot.amber{background:var(--amber)}.dot.red{background:var(--red)}
  .status{display:inline-flex;align-items:center;gap:6px;margin-top:8px;color:var(--green);font-size:11px}
  .status::before{content:"";width:6px;height:6px;border-radius:50%;background:currentColor}
  .stats{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:12px;margin:18px 0}
  .card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:15px 16px;box-shadow:var(--shadow);min-width:0}
  .clabel{font-size:11px;color:var(--muted);margin-bottom:8px;white-space:nowrap}
  .cval{font-family:Consolas,"SFMono-Regular",monospace;font-size:24px;font-weight:600;line-height:1.05;white-space:nowrap;font-variant-numeric:tabular-nums}
  .csub{font-size:11px;color:var(--muted);margin-top:7px;line-height:1.35;min-height:15px;overflow-wrap:anywhere}
  .blue{color:var(--blue)}.teal{color:var(--teal)}.amber{color:var(--amber)}.red{color:var(--red)}.green{color:var(--green)}
  .positive{color:var(--red)}.negative{color:var(--green)}
  .toolbar{display:flex;align-items:flex-end;gap:20px;flex-wrap:wrap;margin-bottom:12px}
  .control{display:flex;flex-direction:column;gap:6px}
  .control-label{font-size:10px;color:var(--muted);font-weight:600}
  .segmented{display:inline-flex;padding:3px;border:1px solid var(--border);border-radius:7px;background:#e9edf1;gap:2px}
  .segmented button{border:0;border-radius:5px;background:transparent;color:var(--muted);padding:6px 10px;font:inherit;font-size:11px;cursor:pointer;white-space:nowrap}
  .segmented button:hover{color:var(--text)}
  .segmented button.active{background:var(--surface);color:var(--text);box-shadow:0 1px 2px rgba(16,24,40,.1);font-weight:600}
  select{height:32px;min-width:260px;max-width:360px;border:1px solid var(--border);border-radius:6px;background:var(--surface);color:var(--text);padding:0 30px 0 10px;font:inherit;font-size:12px;cursor:pointer}
  .toggle{display:inline-flex;align-items:center;gap:7px;height:32px;padding:0 10px;border:1px solid var(--border);border-radius:6px;background:var(--surface);color:var(--muted);font-size:11px;cursor:pointer;user-select:none}
  .toggle input{width:14px;height:14px;accent-color:var(--red);cursor:pointer}
  .chart-panel,.table-panel{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);box-shadow:var(--shadow)}
  .chart-head{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:14px 18px 2px}
  .chart-title{font-size:13px;font-weight:650}
  .chart-note{font-size:11px;color:var(--muted);text-align:right}
  #chart{width:100%;height:520px}
  .table-panel{margin-top:14px;overflow:hidden}
  .table-head{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:15px 18px;border-bottom:1px solid var(--border)}
  .table-title{font-size:14px;font-weight:650}
  .table-meta{font-size:11px;color:var(--muted)}
  .table-scroll{overflow:auto;max-height:560px}
  table{width:100%;border-collapse:separate;border-spacing:0;min-width:920px;font-size:12px}
  th,td{padding:11px 14px;text-align:right;border-bottom:1px solid #edf0f3;white-space:nowrap;font-variant-numeric:tabular-nums}
  th:first-child,td:first-child,th:nth-child(2),td:nth-child(2){text-align:left}
  thead th{position:sticky;top:0;z-index:2;background:var(--surface-soft);color:var(--muted);font-size:10px;font-weight:600}
  thead button{border:0;background:transparent;color:inherit;font:inherit;cursor:pointer;padding:0}
  thead button:hover{color:var(--text)}
  tbody tr:hover{background:#fafbfc}
  tbody tr:last-child td{border-bottom:0}
  .etf-cell{display:flex;flex-direction:column;gap:2px}.etf-code{font-size:10px;color:var(--muted);font-family:Consolas,monospace}
  .exchange-tag{display:inline-flex;justify-content:center;min-width:44px;padding:3px 7px;border-radius:4px;font-size:10px;font-weight:600}
  .exchange-tag.SSE{background:#eaf1ff;color:var(--blue)}.exchange-tag.SZSE{background:#fff2df;color:var(--amber)}
  .footer{padding:14px 2px 4px;color:var(--muted);font-size:10px;line-height:1.7}
  .empty{padding:50px 20px;text-align:center;color:var(--muted)}
  @media(max-width:1280px){.stats{grid-template-columns:repeat(4,minmax(0,1fr))}.header{flex-direction:column}.badges{justify-content:flex-start}}
  @media(max-width:900px){.stats{grid-template-columns:repeat(3,minmax(0,1fr))}}
  @media(max-width:720px){body{padding:14px}.header h1{font-size:20px}.stats{grid-template-columns:repeat(2,minmax(0,1fr))}.stats .card:last-child{grid-column:1/-1}.toolbar{gap:12px}.control,.control select{width:100%;max-width:none}.segmented{max-width:100%;overflow-x:auto}.chart-head{align-items:flex-start;flex-direction:column}.chart-note{text-align:left}#chart{height:430px}.cval{font-size:21px}.chart-panel,.table-panel{overflow:hidden}}
__DUAL_THEME_CSS__
 </style>
</head>
<body>
<div class="page">
<div class="header">
  <div class="header-left">
    <h1>__TITLE__</h1>
    <p>数据截至 __DATE_TO__ &nbsp;·&nbsp; 覆盖 __DATE_FROM__ 起共 __DAY_COUNT__ 个交易日 &nbsp;·&nbsp; 交易所清算后总份额</p>
    <span class="status">__STATUS_TEXT__</span>
  </div>
  <div class="badges">
    <div class="badge"><span class="dot blue"></span>沪市 ETF</div>
    <div class="badge"><span class="dot amber"></span>深市 ETF</div>
    <div class="badge"><span class="dot red"></span>上证指数</div>
  </div>
</div>
<div class="stats">
  <div class="card"><div class="clabel">跟踪 ETF 最新总份额</div><div class="cval blue">__LATEST_TOTAL__<span style="font-size:12px;color:var(--muted);margin-left:4px">亿份</span></div><div class="csub">__ETF_COUNT__ 只 · 沪 __SSE_CNT__ / 深 __SZSE_CNT__</div></div>
  <div class="card"><div class="clabel">单日份额净变化</div><div class="cval __DELTA_CLASS__">__TOTAL_DELTA__<span style="font-size:12px;color:var(--muted);margin-left:4px">亿份</span></div><div class="csub __DELTA_CLASS__">__TOTAL_DELTA_PCT__ · 较前一交易日</div></div>
  <div class="card"><div class="clabel">上周份额净变化</div><div class="cval __WEEK_CLASS__">__WEEK_DELTA__<span style="font-size:12px;color:var(--muted);margin-left:4px">亿份</span></div><div class="csub __WEEK_CLASS__">__WEEK_DELTA_PCT__ · 较 5 个交易日前</div></div>
  <div class="card"><div class="clabel">上月份额净变化</div><div class="cval __MONTH_CLASS__">__MONTH_DELTA__<span style="font-size:12px;color:var(--muted);margin-left:4px">亿份</span></div><div class="csub __MONTH_CLASS__">__MONTH_DELTA_PCT__ · 较 20 个交易日前</div></div>
  <div class="card"><div class="clabel">最大份额 ETF</div><div class="cval amber">__MAX_VAL__<span style="font-size:12px;color:var(--muted);margin-left:4px">亿份</span></div><div class="csub">__MAX_NAME__</div></div>
  <div class="card"><div class="clabel">上证指数收盘</div><div class="cval red">__IDX_LATEST__</div><div class="csub __IDX_CLASS__">__IDX_CHG__</div></div>
  <div class="card"><div class="clabel">单日份额增量 / 减量</div><div class="cval green" style="font-size:17px">+__MOVE_UP__ / __MOVE_DOWN__</div><div class="csub">__MOVE_UP_NAME__ · __MOVE_DOWN_NAME__</div></div>
</div>
<div class="toolbar">
  <div class="control">
    <span class="control-label">市场</span>
    <div class="segmented" id="exchange-control">
      <button class="active" data-value="ALL">全部</button>
      <button data-value="SSE">沪市</button>
      <button data-value="SZSE">深市</button>
    </div>
  </div>
  <div class="control">
    <span class="control-label">时间范围</span>
    <div class="segmented" id="range-control">
      <button data-value="M1">1 月</button>
      <button data-value="M3">3 月</button>
      <button data-value="M6">6 月</button>
      <button class="active" data-value="Y1">1 年</button>
      <button data-value="ALL">全部</button>
    </div>
  </div>
  <div class="control">
    <span class="control-label">显示口径</span>
    <div class="segmented" id="mode-control">
      <button class="active" data-value="ABS">份额规模</button>
      <button data-value="BASE100">区间归一</button>
    </div>
  </div>
  <div class="control">
    <span class="control-label">标的</span>
    <select id="etf-select" aria-label="选择 ETF"></select>
  </div>
  <label class="toggle"><input id="index-toggle" type="checkbox" checked>显示上证指数</label>
</div>
<div class="chart-panel">
  <div class="chart-head">
    <div class="chart-title" id="chart-title">ETF 总份额走势</div>
    <div class="chart-note" id="chart-note">左轴：亿份 · 右轴：上证指数点位</div>
  </div>
  <div id="chart"></div>
</div>
<div class="table-panel">
  <div class="table-head">
    <div>
      <div class="table-title">ETF 最新份额明细</div>
      <div class="table-meta" id="table-meta">__DATE_TO__ · 按最新总份额降序</div>
    </div>
    <div class="table-meta">点击表头排序</div>
  </div>
  <div class="table-scroll">
    <table>
      <thead>
        <tr>
          <th><button data-sort="name">ETF</button></th>
          <th><button data-sort="exchange">市场</button></th>
          <th><button data-sort="latest">最新份额（亿份）</button></th>
          <th><button data-sort="delta">单日变化（亿份）</button></th>
          <th><button data-sort="deltaPct">单日变化率</button></th>
          <th><button data-sort="rangePct">所选区间变化</button></th>
        </tr>
      </thead>
      <tbody id="detail-body"></tbody>
    </table>
  </div>
</div>
<div class="footer">数据来源：上海证券交易所、深圳证券交易所及公开指数行情。份额口径按交易所披露值统一折算为亿份，不使用模拟数据。</div>
</div>
<script>
var rawData=__RAW_DATA__;
var etfData=rawData.filter(function(t){return t.exchange==='SSE'||t.exchange==='SZSE';});
var indexTrace=rawData.find(function(t){return t.trace_kind==='index';});
var dates=etfData.length?etfData[0].x:[];
var state={exchange:'ALL',range:'Y1',mode:'ABS',etf:'ALL',indexOn:true};
var sortState={key:'latest',direction:'desc'};
var rangeLength={M1:23,M3:66,M6:130,Y1:250,ALL:dates.length};

function fmtNumber(value,digits){
  if(value===null||value===undefined||!Number.isFinite(Number(value))) return '暂无';
  return Number(value).toLocaleString('zh-CN',{minimumFractionDigits:digits,maximumFractionDigits:digits});
}
function fmtSigned(value,digits,suffix){
  if(value===null||value===undefined||!Number.isFinite(Number(value))) return '暂无';
  var n=Number(value);
  return (n>=0?'+':'')+fmtNumber(n,digits)+(suffix||'');
}
function fmtPct(value){
  if(value===null||value===undefined||!Number.isFinite(Number(value))) return '暂无';
  return fmtSigned(value,2,'%');
}
function firstValid(y,start){
  for(var i=start;i<y.length;i++){
    if(y[i]!==null&&y[i]!==undefined&&Number.isFinite(Number(y[i]))) return i;
  }
  return -1;
}
function currentStart(){
  var count=rangeLength[state.range]||dates.length;
  return Math.max(0,dates.length-count);
}
function visibleEtfs(){
  return etfData.filter(function(t){
    var marketOk=state.exchange==='ALL'||t.exchange===state.exchange;
    var codeOk=state.etf==='ALL'||t.code===state.etf;
    return marketOk&&codeOk;
  });
}
function fillSelect(){
  var select=document.getElementById('etf-select');
  var previous=state.etf;
  select.innerHTML='';
  var all=document.createElement('option');
  all.value='ALL';all.textContent='全部 ETF';select.appendChild(all);
  etfData.filter(function(t){return state.exchange==='ALL'||t.exchange===state.exchange;}).forEach(function(t){
    var option=document.createElement('option');
    option.value=t.code;
    option.textContent='['+(t.exchange==='SSE'?'沪':'深')+'] '+t.short_name+'  '+t.code;
    select.appendChild(option);
  });
  var stillVisible=state.etf==='ALL'||etfData.some(function(t){
    return t.code===state.etf&&(state.exchange==='ALL'||t.exchange===state.exchange);
  });
  state.etf=stillVisible?previous:'ALL';
  select.value=state.etf;
}
function chartData(){
  var start=currentStart();
  var x=dates.slice(start);
  var traces=[];
  visibleEtfs().forEach(function(t){
    var y=t.y.slice(start);
    var first=firstValid(y,0);
    if(first>=0&&state.mode==='BASE100'){
      var baseline=Number(y[first]);
      y=y.map(function(v){
        return v===null||v===undefined?null:(Number(v)/baseline*100);
      });
    }
    traces.push({
      x:x,y:y,name:t.short_name+' ('+t.code+')',type:'scatter',mode:'lines',
      line:{color:t.line.color,width:visibleEtfs().length===1?2.8:1.7,dash:t.line.dash||'solid'},
      connectgaps:true,yaxis:'y',
      hovertemplate:'<b>'+t.short_name+' ('+t.code+')</b><br>%{x}<br>'+
        (state.mode==='ABS'?'总份额: <b>%{y:,.2f} 亿份</b>':'区间指数: <b>%{y:.1f}</b>')+'<extra></extra>'
    });
  });
  if(indexTrace&&state.indexOn){
    var iy=indexTrace.y.slice(start);
    var firstIdx=firstValid(iy,0);
    if(firstIdx>=0&&state.mode==='BASE100'){
      var idxBase=Number(iy[firstIdx]);
      iy=iy.map(function(v){return v===null||v===undefined?null:Number(v)/idxBase*100;});
    }
    traces.push({
      x:x,y:iy,name:'上证指数',type:'scatter',mode:'lines',
      line:{color:'#c43d3d',width:2.3},connectgaps:true,
      yaxis:state.mode==='BASE100'?'y':'y2',
      hovertemplate:'<b>上证指数</b><br>%{x}<br>'+
        (state.mode==='ABS'?'收盘: <b>%{y:,.2f}</b> 点':'区间指数: <b>%{y:.1f}</b>')+'<extra></extra>'
    });
  }
  return traces;
}
function layoutConfig(){
  var normalized=state.mode==='BASE100';
  return {
    paper_bgcolor:'#ffffff',plot_bgcolor:'#ffffff',
    font:{family:'"Microsoft YaHei","PingFang SC",sans-serif',color:'#66717d',size:11},
    hovermode:'x unified',
    hoverlabel:{bgcolor:'#17212b',bordercolor:'#17212b',font:{size:12,color:'#ffffff'}},
    xaxis:{
      type:'date',automargin:true,gridcolor:'#edf0f3',linecolor:'#dfe5ea',
      tickcolor:'#dfe5ea',tickfont:{size:10},hoverformat:'%Y-%m-%d',
      rangeslider:{visible:false}
    },
    yaxis:{
      title:{text:normalized?'区间起点 = 100':'总份额（亿份）',font:{color:'#66717d',size:11},standoff:8},
      gridcolor:'#edf0f3',linecolor:'#dfe5ea',tickcolor:'#dfe5ea',
      tickfont:{color:'#66717d',size:10},zeroline:false,automargin:true,
      tickformat:normalized?',.0f':',.1f'
    },
    yaxis2:{
      title:{text:'上证指数（点）',font:{color:'#c43d3d',size:11},standoff:8},
      overlaying:'y',side:'right',visible:!normalized,showgrid:false,
      tickfont:{color:'#c43d3d',size:10},zeroline:false,automargin:true,tickformat:',.0f'
    },
    legend:{orientation:'h',x:0,y:1.08,font:{size:10},visible:visibleEtfs().length<=4},
    margin:{t:30,r:normalized?28:82,b:48,l:66},
    dragmode:'zoom'
  };
}
function drawChart(){
  var chart=document.getElementById('chart');
  document.getElementById('chart-title').textContent=
    state.etf==='ALL'?'ETF 总份额走势':'单只 ETF 总份额走势';
  document.getElementById('chart-note').textContent=state.mode==='ABS'
    ?'左轴：亿份 · 右轴：上证指数点位'
    :'所有序列以所选区间首个有效值归一为 100';
  if(typeof Plotly==='undefined'){
    chart.innerHTML='<div class="empty">图表组件加载失败，请检查网络后刷新页面。</div>';
    return;
  }
  var config={
    responsive:true,displayModeBar:true,displaylogo:false,
    modeBarButtonsToRemove:['sendDataToCloud','editInChartStudio','lasso2d','select2d']
  };
  Plotly.react('chart',chartData(),layoutConfig(),config);
}
function rowStats(t){
  var y=t.y;
  var latestIdx=-1,prevIdx=-1,rangeStart=currentStart(),baseIdx=firstValid(y,rangeStart);
  for(var i=y.length-1;i>=0;i--){
    if(y[i]!==null&&y[i]!==undefined&&Number.isFinite(Number(y[i]))){
      latestIdx=i;break;
    }
  }
  if(latestIdx<0) return null;
  for(var j=latestIdx-1;j>=0;j--){
    if(y[j]!==null&&y[j]!==undefined&&Number.isFinite(Number(y[j]))){
      prevIdx=j;break;
    }
  }
  var latest=Number(y[latestIdx]);
  var prev=prevIdx>=0?Number(y[prevIdx]):null;
  var delta=prev!==null?latest-prev:null;
  var deltaPct=prev?delta/prev*100:null;
  var rangeBase=baseIdx>=0?Number(y[baseIdx]):null;
  var rangePct=rangeBase?latest/rangeBase*100-100:null;
  return {trace:t,latest:latest,delta:delta,deltaPct:deltaPct,rangePct:rangePct};
}
function drawTable(){
  var rows=visibleEtfs().map(rowStats).filter(Boolean);
  rows.sort(function(a,b){
    var av,bv;
    if(sortState.key==='name'){av=a.trace.short_name;bv=b.trace.short_name;}
    else if(sortState.key==='exchange'){av=a.trace.exchange;bv=b.trace.exchange;}
    else {av=a[sortState.key];bv=b[sortState.key];}
    if(typeof av==='string') return sortState.direction==='asc'?av.localeCompare(bv,'zh-CN'):bv.localeCompare(av,'zh-CN');
    if(av===null||av===undefined) return 1;
    if(bv===null||bv===undefined) return -1;
    return sortState.direction==='asc'?av-bv:bv-av;
  });
  var body=document.getElementById('detail-body');
  body.innerHTML=rows.map(function(row){
    var t=row.trace;
    var deltaClass=row.delta>=0?'positive':'negative';
    var pctClass=row.deltaPct>=0?'positive':'negative';
    var rangeClass=row.rangePct>=0?'positive':'negative';
    return '<tr>'+
      '<td><div class="etf-cell"><strong>'+t.short_name+'</strong><span class="etf-code">'+t.code+'</span></div></td>'+
      '<td><span class="exchange-tag '+t.exchange+'">'+(t.exchange==='SSE'?'沪市':'深市')+'</span></td>'+
      '<td><strong>'+fmtNumber(row.latest,2)+'</strong></td>'+
      '<td class="'+deltaClass+'">'+fmtSigned(row.delta,2)+'</td>'+
      '<td class="'+pctClass+'">'+fmtPct(row.deltaPct)+'</td>'+
      '<td class="'+rangeClass+'">'+fmtPct(row.rangePct)+'</td>'+
      '</tr>';
  }).join('');
  if(!rows.length) body.innerHTML='<tr><td colspan="6"><div class="empty">当前筛选条件下暂无数据</div></td></tr>';
  var rangeName={M1:'近 1 月',M3:'近 3 月',M6:'近 6 月',Y1:'近 1 年',ALL:'全部区间'}[state.range];
  document.getElementById('table-meta').textContent=(dates.length?dates[dates.length-1]:'')+' · '+rangeName+' · 按当前条件展示';
}
function updateAll(){
  drawChart();
  drawTable();
}
document.querySelectorAll('.segmented button').forEach(function(button){
  button.addEventListener('click',function(){
    var parent=button.closest('.segmented');
    parent.querySelectorAll('button').forEach(function(item){item.classList.remove('active');});
    button.classList.add('active');
    if(parent.id==='exchange-control'){
      state.exchange=button.dataset.value;
      fillSelect();
    } else if(parent.id==='range-control'){
      state.range=button.dataset.value;
    } else if(parent.id==='mode-control'){
      state.mode=button.dataset.value;
    }
    updateAll();
  });
});
document.getElementById('etf-select').addEventListener('change',function(event){
  state.etf=event.target.value;
  updateAll();
});
document.getElementById('index-toggle').addEventListener('change',function(event){
  state.indexOn=event.target.checked;
  drawChart();
});
document.querySelectorAll('thead button').forEach(function(button){
  button.addEventListener('click',function(){
    var key=button.dataset.sort;
    if(sortState.key===key) sortState.direction=sortState.direction==='desc'?'asc':'desc';
    else {sortState.key=key;sortState.direction=key==='name'?'asc':'desc';}
    drawTable();
  });
});
window.addEventListener('resize',function(){
  if(typeof Plotly!=='undefined') Plotly.Plots.resize(document.getElementById('chart'));
});
fillSelect();
updateAll();
 </script>
__DUAL_THEME_TAIL__
 </body>
</html>"""

    html = (TMPL
        .replace('__DUAL_THEME_JS__',   DUAL_THEME_JS)
        .replace('__DUAL_THEME_CSS__',  DUAL_THEME_CSS)
        .replace('__DUAL_THEME_TAIL__', DUAL_THEME_TAIL)
        .replace('__TITLE__',     f'沪深宽基 ETF 份额看板{status_tag}')
        .replace('__DATE_FROM__', date_from)
        .replace('__DATE_TO__',   date_to)
        .replace('__DAY_COUNT__', f'{day_count:,}')
        .replace('__STATUS_TEXT__', status_text)
        .replace('__ETF_COUNT__', str(len(all_etf)))
        .replace('__SSE_CNT__',   str(sse_cnt))
        .replace('__SZSE_CNT__',  str(szse_cnt))
        .replace('__LATEST_TOTAL__', latest_total_str)
        .replace('__DELTA_CLASS__', delta_class)
        .replace('__TOTAL_DELTA__', total_delta_str)
        .replace('__TOTAL_DELTA_PCT__', total_delta_pct_str)
        .replace('__WEEK_CLASS__', week_class)
        .replace('__WEEK_DELTA__', week_delta_str)
        .replace('__WEEK_DELTA_PCT__', week_delta_pct_str)
        .replace('__MONTH_CLASS__', month_class)
        .replace('__MONTH_DELTA__', month_delta_str)
        .replace('__MONTH_DELTA_PCT__', month_delta_pct_str)
        .replace('__IDX_LATEST__', idx_latest_str)
        .replace('__IDX_CLASS__', idx_class)
        .replace('__IDX_CHG__', idx_change_str)
        .replace('__MAX_VAL__', f'{top_value:,.1f}' if top else '暂无')
        .replace('__MAX_NAME__', top_name)
        .replace('__MOVE_UP__', f'{move_up_value:,.2f}' if move_up_trace else '暂无')
        .replace('__MOVE_DOWN__', f'{move_down_value:,.2f}' if move_down_trace else '暂无')
        .replace('__MOVE_UP_NAME__', move_up_name)
        .replace('__MOVE_DOWN_NAME__', move_down_name)
        .replace('__RAW_DATA__',  RAW_JSON)
    )

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'✅ HTML 已生成：{output_path}')


# ── 11. 主流程 ─────────────────────────────────────────────────────────────────

def main():
    print('=' * 60)
    print('  沪深两市宽基 ETF 份额监控')
    print('=' * 60)

    # v1 断点混入了周末数据和不可靠的深交所备用字段，先做一次性修复。
    if read_checkpoint_version() < CHECKPOINT_VERSION:
        old_results = read_all_results()
        if old_results:
            refresh_szse_history(old_results)

    today          = today_str()
    existing_dates = get_existing_dates()
    earliest       = min(existing_dates) if existing_dates else None
    latest         = max(existing_dates) if existing_dates else None
    history_done   = (earliest is not None and earliest <= CUTOFF_DATE.strftime('%Y-%m-%d'))

    print(f'📅 当前日期        : {today}')
    print(f'📂 已有数据范围    : {earliest} ~ {latest}（{len(existing_dates)} 个交易日）' if existing_dates else '📂 暂无本地数据')
    print(f'📚 历史已补全至2022: {"是" if history_done else "否"}')
    print()

    # 增量更新
    need_increment = (latest is None or latest < today)
    if need_increment:
        print(f'▶  Step 1：增量更新')
        results, new_count = incremental_update()
        if new_count > 0:
            print(f'   本次新增 {new_count} 个交易日。')
        existing_dates = get_existing_dates()
        earliest       = min(existing_dates) if existing_dates else None
        history_done   = (earliest is not None and earliest <= CUTOFF_DATE.strftime('%Y-%m-%d'))
    else:
        print(f'▶  Step 1：增量更新 — {today} 数据已存在，跳过。')
        results = read_all_results()

    # 历史补全
    if not history_done:
        print(f'\n▶  Step 2：历史补全')
        results, completed = collect_history()
    else:
        print(f'\n▶  Step 2：历史已补全至 2022-01-01，跳过。')
        completed = True

    if not results:
        print('\n❌ 无数据，请检查网络后重试。')
        return

    print(f'\n共 {len(results)} 个交易日，生成输出文件...\n')

    # 获取上证指数数据
    print('▶  获取上证指数历史数据...')
    index_prices = get_shindex_data()

    # 生成图表数据
    plot_data = build_plot_data(results, index_prices=index_prices)

    # 生成输出文件
    generate_html(plot_data, OUTPUT_HTML, completed=completed, index_prices=index_prices)

    if completed:
        webbrowser.open(OUTPUT_HTML)
        print('\n🌐 已在浏览器打开 HTML 看板。')
    else:
        print('\n📊 已生成当前数据预览。')


if __name__ == '__main__':
    main()
