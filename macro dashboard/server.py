#!/usr/bin/env python3
"""No-dependency macro shock radar server.

The server does three jobs:
1. Serves the static dashboard from ./public.
2. Proxies public macro data sources so the browser avoids CORS.
3. Computes historical percentiles, robust z-scores, momentum shocks, and
   scenario-level risk summaries.
"""

import concurrent.futures
import csv
import datetime as dt
import hashlib
import html
import io
import json
import math
import mimetypes
import os
import posixpath
import random
import re
import socket
import statistics
import sys
import time
import traceback
import urllib.parse
import urllib.request
import bisect
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


ROOT = os.path.abspath(os.path.dirname(__file__))
PUBLIC_DIR = os.path.join(ROOT, "public")
DATA_DIR = os.path.join(ROOT, "data")
CACHE_DIR = os.path.join(ROOT, ".cache")
HTTP_CACHE_DIR = os.path.join(CACHE_DIR, "http")
CATALOG_PATH = os.path.join(DATA_DIR, "catalog.json")
SNAPSHOT_PATH = os.path.join(DATA_DIR, "bootstrap_monitor.json")
PRIVATE_STATIC_SEGMENTS = {
    ".cache",
    ".deps",
    ".git",
    "__pycache__",
    "data",
    "report",
    "reports",
    "scripts",
}

CACHE_TTL_SECONDS = int(os.environ.get("MACRO_CACHE_TTL_SECONDS", str(6 * 60 * 60)))
DEFAULT_PORT = int(os.environ.get("PORT", "8787"))
DEFAULT_HOST = os.environ.get("HOST", "127.0.0.1")
MAX_WORKERS = int(os.environ.get("MACRO_FETCH_WORKERS", "10"))
REQUEST_TIMEOUT = int(os.environ.get("MACRO_REQUEST_TIMEOUT", "22"))
USER_AGENT = "macro-shock-radar/1.0 (+local research dashboard)"


def ensure_dirs():
    for path in (CACHE_DIR, HTTP_CACHE_DIR):
        os.makedirs(path, exist_ok=True)


def load_catalog():
    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        catalog = json.load(f)
    return expand_catalog(catalog)


def format_country_template(value, country):
    if not isinstance(value, str):
        return value
    return value.format(
        code=country.get("code", ""),
        code_lower=country.get("code", "").lower(),
        country=country.get("name", ""),
        short=country.get("short", country.get("code", "")),
    )


def expand_catalog(catalog):
    """Expand compact country templates into regular metric definitions."""
    countries = catalog.get("countries", [])
    templates = catalog.get("countryMetricTemplates", [])
    if not countries or not templates:
        return catalog

    metrics = list(catalog.get("metrics", []))
    for country in countries:
        country_code = country.get("code")
        if not country_code:
            continue
        for template in templates:
            source_key = template.get("sourceKey")
            if source_key and not country.get(source_key):
                continue

            metric = {}
            for key, value in template.items():
                if key in ("sourceKey", "sourceIdTemplate"):
                    continue
                if isinstance(value, list):
                    metric[key] = [format_country_template(item, country) for item in value]
                else:
                    metric[key] = format_country_template(value, country)

            metric["id"] = "%s_%s" % (country_code.lower(), template["id"])
            metric["region"] = country.get("name")
            metric["country"] = country_code
            metric["countryName"] = country.get("name")
            metric["countryShort"] = country.get("short", country_code)
            metric["countryRegion"] = country.get("region")

            if source_key:
                metric["sourceId"] = country[source_key]
            elif template.get("sourceIdTemplate"):
                metric["sourceId"] = format_country_template(template["sourceIdTemplate"], country)

            tags = list(metric.get("tags", []))
            tags.extend(["country", "country_%s" % country_code.lower()])
            if country.get("region"):
                tags.append("region_%s" % re.sub(r"[^a-z0-9]+", "_", country["region"].lower()).strip("_"))
            metric["tags"] = list(dict.fromkeys(tags))
            metrics.append(metric)

    expanded = dict(catalog)
    expanded["metrics"] = metrics
    return expanded


def load_snapshot():
    if not os.path.exists(SNAPSHOT_PATH):
        return None
    with open(SNAPSHOT_PATH, "r", encoding="utf-8") as f:
        payload = json.load(f)
    payload["snapshot"] = True
    return payload


def utc_now_iso():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def parse_date(value):
    if not value:
        return None
    value = str(value).strip()
    try:
        return dt.date.fromisoformat(value[:10])
    except Exception:
        for fmt in ("%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y", "%b %d, %Y", "%B %d, %Y"):
            try:
                return dt.datetime.strptime(value, fmt).date()
            except Exception:
                pass
    return None


def date_to_iso(value):
    if isinstance(value, dt.date):
        return value.isoformat()
    return value


def parse_number(value):
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in ("", ".", "NaN", "nan", "NA", "N/A", "null", "None"):
        return None
    try:
        number = float(text)
    except Exception:
        match = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", text)
        if not match:
            return None
        try:
            number = float(match.group(0))
        except Exception:
            return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def safe_div(a, b):
    if b in (None, 0):
        return None
    try:
        value = a / b
    except Exception:
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def clamp(value, low, high):
    return max(low, min(high, value))


def round_or_none(value, digits=4):
    if value is None:
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return round(value, digits)


def metric_signal_family(metric):
    return metric.get("signalFamily") or metric.get("id")


def unique_best_by_family(items, score_getter):
    ranked = sorted(items, key=lambda item: score_getter(item) or 0, reverse=True)
    seen = set()
    unique = []
    for item in ranked:
        family = metric_signal_family(item)
        if family in seen:
            continue
        seen.add(family)
        unique.append(item)
    return unique


def public_path_parts(path):
    parsed = urllib.parse.urlparse(path)
    normalized = posixpath.normpath(urllib.parse.unquote(parsed.path))
    if normalized == "/":
        normalized = "/index.html"
    return [part for part in normalized.split("/") if part and part not in (".", "..")]


def is_private_static_request(path):
    parts = public_path_parts(path)
    return any(part in PRIVATE_STATIC_SEGMENTS or part.startswith(".") for part in parts)


def cache_file_for(url):
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return os.path.join(HTTP_CACHE_DIR, digest + ".json")


def fetch_url(url, refresh=False):
    """Return text plus source status. Falls back to stale cache if live fails."""
    ensure_dirs()
    cache_path = cache_file_for(url)
    now = time.time()
    if not refresh and os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                cached = json.load(f)
            age = now - float(cached.get("fetchedAtEpoch", 0))
            if age < CACHE_TTL_SECONDS:
                return cached.get("text", ""), "cached", cached.get("fetchedAt")
        except Exception:
            pass

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/csv,application/json,text/plain,*/*",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            raw = response.read()
            encoding = response.headers.get_content_charset() or "utf-8"
            text = raw.decode(encoding, errors="replace")
        payload = {
            "url": url,
            "fetchedAt": utc_now_iso(),
            "fetchedAtEpoch": now,
            "text": text,
        }
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        return text, "live", payload["fetchedAt"]
    except Exception as exc:
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                return cached.get("text", ""), "stale-cache", cached.get("fetchedAt")
            except Exception:
                pass
        raise RuntimeError("fetch failed: %s" % exc)


def fred_url(series_id):
    return "https://fred.stlouisfed.org/graph/fredgraph.csv?id=%s" % urllib.parse.quote(series_id)


def yahoo_url(symbol):
    encoded = urllib.parse.quote(symbol, safe="")
    return "https://query1.finance.yahoo.com/v8/finance/chart/%s?range=max&interval=1d" % encoded


def multpl_url(slug):
    return "https://www.multpl.com/%s/table/by-month" % urllib.parse.quote(slug, safe="-")


def siblis_url(slug):
    return "https://siblisresearch.com/data/%s/" % urllib.parse.quote(slug, safe="-")


def worldbank_url(country_code, indicator):
    country = urllib.parse.quote(country_code, safe="")
    indicator = urllib.parse.quote(indicator, safe=".")
    return "https://api.worldbank.org/v2/country/%s/indicator/%s?format=json&per_page=20000" % (country, indicator)


def parse_fred_csv(text, metric):
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or len(reader.fieldnames) < 2:
        raise ValueError("FRED CSV has no data columns")
    fields = [field.strip() for field in reader.fieldnames]
    date_col = fields[0]
    for candidate in fields:
        lower = candidate.lower()
        if lower in ("date", "observation_date"):
            date_col = candidate
            break
    value_col = None
    source_id = metric.get("sourceId", "").upper()
    for candidate in fields:
        if candidate == date_col:
            continue
        if candidate.upper() == source_id:
            value_col = candidate
            break
    if value_col is None:
        for candidate in fields:
            if candidate != date_col:
                value_col = candidate
                break
    if value_col is None:
        raise ValueError("FRED CSV missing value column")

    divisor = parse_number(metric.get("valueDivisor")) or 1.0
    points = []
    for row in reader:
        date_value = parse_date(row.get(date_col))
        numeric = parse_number(row.get(value_col))
        if date_value is None or numeric is None:
            continue
        points.append({"date": date_value, "value": numeric / divisor})
    return dedupe_sort(points)


def parse_yahoo_json(text, metric):
    data = json.loads(text)
    chart = data.get("chart", {})
    if chart.get("error"):
        raise ValueError(str(chart["error"]))
    result = (chart.get("result") or [None])[0]
    if not result:
        raise ValueError("Yahoo chart response missing result")
    timestamps = result.get("timestamp") or []
    quote = (((result.get("indicators") or {}).get("quote") or [{}])[0])
    closes = quote.get("close") or []
    points = []
    for ts, close in zip(timestamps, closes):
        numeric = parse_number(close)
        if numeric is None:
            continue
        date_value = dt.datetime.utcfromtimestamp(int(ts)).date()
        points.append({"date": date_value, "value": numeric})
    return dedupe_sort(points)


def clean_html_cell(value):
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = html.unescape(value)
    return " ".join(value.replace("\u2002", " ").split())


def parse_multpl_html(text, metric):
    rows = re.findall(r"<tr[^>]*>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>\s*</tr>", text, re.I | re.S)
    points = []
    for date_cell, value_cell in rows:
        date_value = parse_date(clean_html_cell(date_cell))
        raw_value = clean_html_cell(value_cell).replace("%", "").replace("x", "")
        numeric = parse_number(raw_value)
        if date_value is None or numeric is None:
            continue
        points.append({"date": date_value, "value": numeric})
    if not points:
        raise ValueError("Multpl HTML table had no parseable observations")
    return dedupe_sort(points)


def parse_siblis_html(text, metric):
    headers = [
        clean_html_cell(value)
        for value in re.findall(r"<th[^>]*data-original-value=\"([^\"]*)\"[^>]*>", text, re.I | re.S)
    ]
    column_name = metric.get("column")
    if not column_name:
        raise ValueError("Siblis metric missing column")
    header_lookup = {header.lower(): idx for idx, header in enumerate(headers)}
    column_idx = header_lookup.get(column_name.lower())
    if column_idx is None:
        raise ValueError("Siblis column not found: %s" % column_name)

    tbody_match = re.search(r"<tbody>(.*?)</tbody>", text, re.I | re.S)
    if not tbody_match:
        raise ValueError("Siblis table missing tbody")
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", tbody_match.group(1), re.I | re.S)
    points = []
    for row in rows:
        cells = [
            clean_html_cell(value)
            for value in re.findall(r"<td[^>]*data-original-value=\"([^\"]*)\"[^>]*>", row, re.I | re.S)
        ]
        if len(cells) <= column_idx:
            continue
        date_value = parse_date(cells[0])
        numeric = parse_number(cells[column_idx])
        if date_value is None or numeric is None:
            continue
        points.append({"date": date_value, "value": numeric})
    if not points:
        raise ValueError("Siblis table had no parseable observations for %s" % column_name)
    return dedupe_sort(points)


def parse_worldbank_json(text, metric):
    data = json.loads(text)
    if isinstance(data, dict) and data.get("message"):
        raise ValueError("World Bank error: %s" % data.get("message"))
    if not isinstance(data, list) or len(data) < 2 or not isinstance(data[1], list):
        raise ValueError("World Bank response missing observation list")

    points = []
    for row in data[1]:
        numeric = parse_number(row.get("value"))
        year = parse_number(row.get("date"))
        if numeric is None or year is None:
            continue
        year_int = int(year)
        if year_int < 1800 or year_int > 2200:
            continue
        points.append({"date": dt.date(year_int, 12, 31), "value": numeric})
    if not points:
        raise ValueError("World Bank series had no numeric observations")
    return dedupe_sort(points)


def compute_derived_formula(formula, values, scale=1.0):
    names = list(values.keys())
    if len(names) < 2 and formula not in ("inverse_pct",):
        return None
    a = values.get("a", values.get(names[0]) if names else None)
    b = values.get("b", values.get(names[1]) if len(names) > 1 else None)
    if a is None:
        return None
    if formula == "ratio":
        return safe_div(a, b)
    if formula == "ratio_pct":
        ratio = safe_div(a, b)
        return None if ratio is None else ratio * 100.0
    if formula == "ratio_pct_millions_to_billions":
        if b is None:
            return None
        ratio = safe_div(a, b * 1000.0)
        return None if ratio is None else ratio * 100.0
    if formula == "ratio_millions_to_millions":
        return safe_div(a, b)
    if formula == "spread":
        if b is None:
            return None
        return a - b
    if formula == "sum":
        return sum(values.values())
    if formula == "product":
        if b is None:
            return None
        return a * b * scale
    if formula == "inverse_pct":
        return safe_div(100.0, a)
    raise ValueError("unsupported derived formula: %s" % formula)


def aggregate_status(statuses):
    if any(status == "live" for status in statuses):
        return "live"
    if any(status == "stale-cache" for status in statuses):
        return "stale-cache"
    if any(status == "cached" for status in statuses):
        return "cached"
    return statuses[0] if statuses else "unknown"


def fetch_derived_points(metric, refresh=False):
    components = metric.get("components") or []
    if not components:
        raise ValueError("derived metric missing components")

    fetched_components = []
    statuses = []
    fetched_times = []
    urls = []
    for idx, component in enumerate(components):
        comp_metric = dict(component)
        comp_metric.setdefault("provider", "fred")
        comp_metric.setdefault("frequency", metric.get("frequency"))
        comp_metric.setdefault("transform", "level")
        comp_metric.setdefault("id", component.get("name") or "component_%s" % idx)
        raw_points, status, fetched_at, source_url = fetch_raw_points(comp_metric, refresh=refresh)
        points = apply_transform(raw_points, comp_metric)
        if not points:
            raise ValueError("derived component has no points: %s" % comp_metric.get("sourceId"))
        fetched_components.append(
            {
                "name": component.get("name") or ("c%s" % idx),
                "points": points,
                "dates": [point["date"] for point in points],
            }
        )
        statuses.append(status)
        if fetched_at:
            fetched_times.append(fetched_at)
        if source_url:
            urls.append(source_url)

    base_name = metric.get("baseComponent")
    base_component = fetched_components[0]
    if base_name:
        for component in fetched_components:
            if component["name"] == base_name:
                base_component = component
                break

    formula = metric.get("formula", "ratio")
    scale = float(metric.get("formulaScale") or 1.0)
    points = []
    for base_point in base_component["points"]:
        date_value = base_point["date"]
        values = {}
        missing = False
        for component in fetched_components:
            point = get_point_before_index(component["points"], component["dates"], date_value)
            if not point:
                missing = True
                break
            values[component["name"]] = point["value"]
        if missing:
            continue
        numeric = compute_derived_formula(formula, values, scale=scale)
        if numeric is None or math.isnan(numeric) or math.isinf(numeric):
            continue
        points.append({"date": date_value, "value": numeric})
    if not points:
        raise ValueError("derived metric produced no observations")
    return dedupe_sort(points), aggregate_status(statuses), (max(fetched_times) if fetched_times else None), (urls[0] if urls else None)


def fetch_raw_points(metric, refresh=False):
    provider = metric.get("provider", "fred").lower()
    if provider == "fred":
        url = fred_url(metric["sourceId"])
        text, status, fetched_at = fetch_url(url, refresh=refresh)
        return parse_fred_csv(text, metric), status, fetched_at, url
    if provider == "multpl":
        url = multpl_url(metric["sourceId"])
        text, status, fetched_at = fetch_url(url, refresh=refresh)
        return parse_multpl_html(text, metric), status, fetched_at, url
    if provider == "siblis":
        url = siblis_url(metric["sourceId"])
        text, status, fetched_at = fetch_url(url, refresh=refresh)
        return parse_siblis_html(text, metric), status, fetched_at, url
    if provider == "worldbank":
        indicator = metric.get("indicator") or metric.get("sourceId")
        country_code = metric.get("country") or metric.get("countryCode")
        if not indicator or not country_code:
            raise ValueError("World Bank metric missing country or indicator")
        url = worldbank_url(country_code, indicator)
        text, status, fetched_at = fetch_url(url, refresh=refresh)
        return parse_worldbank_json(text, metric), status, fetched_at, url
    if provider == "yahoo":
        url = yahoo_url(metric["sourceId"])
        text, status, fetched_at = fetch_url(url, refresh=refresh)
        return parse_yahoo_json(text, metric), status, fetched_at, url
    if provider == "derived":
        return fetch_derived_points(metric, refresh=refresh)
    raise ValueError("unsupported provider: %s" % provider)


def dedupe_sort(points):
    by_date = {}
    for point in points:
        by_date[point["date"]] = point["value"]
    return [{"date": key, "value": by_date[key]} for key in sorted(by_date.keys())]


def infer_scale(metric):
    frequency = metric.get("frequency", "").lower()
    if frequency == "daily":
        return 252.0
    if frequency == "weekly":
        return 52.0
    if frequency == "monthly":
        return 12.0
    if frequency == "quarterly":
        return 4.0
    if frequency == "semiannual":
        return 2.0
    if frequency == "annual":
        return 1.0
    return 1.0


def apply_transform(raw_points, metric):
    transform = metric.get("transform", "level")
    if transform == "level":
        return list(raw_points)

    periods = int(metric.get("periods") or 1)
    scale = float(metric.get("scale") or infer_scale(metric))
    transformed = []
    for idx, point in enumerate(raw_points):
        if idx < periods:
            continue
        base = raw_points[idx - periods]["value"]
        current = point["value"]
        value = None
        if base is None or current is None:
            continue
        if transform == "yoy_pct" or transform == "pct_change":
            if base != 0:
                value = (current / base - 1.0) * 100.0
        elif transform == "annualized_pct_change":
            ratio = safe_div(current, base)
            if ratio is not None and ratio > 0:
                value = (math.pow(ratio, scale / float(periods)) - 1.0) * 100.0
        elif transform == "diff":
            value = current - base
        else:
            value = current
        if value is None or math.isnan(value) or math.isinf(value):
            continue
        transformed.append({"date": point["date"], "value": value})
    return transformed


def percentile_rank(values, x):
    if not values:
        return None
    below_or_equal = 0
    for value in values:
        if value <= x:
            below_or_equal += 1
    return below_or_equal / float(len(values))


def quantile(sorted_values, q):
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    idx = q * (len(sorted_values) - 1)
    low = int(math.floor(idx))
    high = int(math.ceil(idx))
    if low == high:
        return sorted_values[low]
    weight = idx - low
    return sorted_values[low] * (1 - weight) + sorted_values[high] * weight


def mean_std(values):
    if not values:
        return None, None
    mean = sum(values) / float(len(values))
    if len(values) < 2:
        return mean, None
    variance = sum((value - mean) ** 2 for value in values) / float(len(values) - 1)
    std = math.sqrt(max(variance, 0.0))
    if std == 0:
        std = None
    return mean, std


def robust_z(values, x):
    if len(values) < 5:
        return None
    median = statistics.median(values)
    deviations = [abs(value - median) for value in values]
    mad = statistics.median(deviations)
    if mad == 0:
        return None
    return (x - median) / (1.4826 * mad)


def get_point_before_index(points, dates, target_date):
    idx = bisect.bisect_right(dates, target_date) - 1
    if idx < 0:
        return None
    return points[idx]


def get_point_before(points, target_date):
    return get_point_before_index(points, [point["date"] for point in points], target_date)


def change_mode(metric):
    unit = metric.get("unit", "")
    if unit in ("%", "pp", "% ann."):
        return "diff"
    return metric.get("changeMode", "pct")


def compute_change(current, previous, mode):
    if current is None or previous is None:
        return None
    if mode == "pct":
        if previous == 0:
            return None
        return (current / previous - 1.0) * 100.0
    return current - previous


def latest_change(points, days, mode):
    if len(points) < 2:
        return None
    latest = points[-1]
    target = latest["date"] - dt.timedelta(days=days)
    dates = [point["date"] for point in points]
    base = get_point_before_index(points, dates, target)
    if not base:
        return None
    return compute_change(latest["value"], base["value"], mode)


def rolling_changes(points, days, mode, max_points=4000):
    changes = []
    if len(points) < 3:
        return changes
    dates = [point["date"] for point in points]
    iterable = points
    if len(points) > max_points:
        stride = max(1, len(points) // max_points)
        iterable = points[::stride]
    for point in iterable:
        base = get_point_before_index(points, dates, point["date"] - dt.timedelta(days=days))
        if not base:
            continue
        value = compute_change(point["value"], base["value"], mode)
        if value is not None and not math.isnan(value) and not math.isinf(value):
            changes.append(value)
    return changes


def score_tail(tail):
    if tail is None:
        return 0.0
    return clamp((tail - 0.80) / 0.20 * 100.0, 0.0, 100.0)


def score_z(z_value, cap=4.0):
    if z_value is None:
        return 0.0
    return clamp(abs(z_value) / cap * 100.0, 0.0, 100.0)


def score_directed_z(z_value, direction, cap=4.0):
    if z_value is None:
        return 0.0
    if direction == "high":
        directed = z_value
    elif direction == "low":
        directed = -z_value
    else:
        directed = abs(z_value)
    return clamp(max(directed, 0.0) / cap * 100.0, 0.0, 100.0)


def risk_tail_from_percentile(percentile, direction):
    if percentile is None:
        return None
    if direction == "high":
        return percentile
    if direction == "low":
        return 1.0 - percentile
    return max(percentile, 1.0 - percentile)


def severity(score):
    if score is None:
        return "unknown"
    if score >= 90:
        return "extreme"
    if score >= 76:
        return "shock"
    if score >= 62:
        return "elevated"
    if score >= 45:
        return "watch"
    return "normal"


def stale_days_for(metric):
    frequency = metric.get("frequency", "").lower()
    if frequency == "daily":
        return 10
    if frequency == "weekly":
        return 35
    if frequency == "monthly":
        return 75
    if frequency == "quarterly":
        return 135
    if frequency == "semiannual":
        return 240
    if frequency == "annual":
        return 1100
    return 45


def point_to_wire(point):
    return {"date": date_to_iso(point["date"]), "value": round_or_none(point["value"], 6)}


def sample_points(points, limit):
    if len(points) <= limit:
        return [point_to_wire(point) for point in points]
    stride = max(1, int(math.ceil(len(points) / float(limit))))
    sampled = points[::stride]
    if sampled[-1]["date"] != points[-1]["date"]:
        sampled.append(points[-1])
    return [point_to_wire(point) for point in sampled]


def make_note(metric, stats):
    notes = []
    short = metric.get("short") or metric.get("name")
    percentile = stats.get("percentile")
    latest = stats.get("latest")
    direction = metric.get("riskDirection", "two-sided")
    if percentile is not None:
        if percentile >= 0.98:
            notes.append("%s is in the top %.1f%% of its available history." % (short, (1.0 - percentile) * 100.0))
        elif percentile <= 0.02:
            notes.append("%s is in the bottom %.1f%% of its available history." % (short, percentile * 100.0))
        elif percentile >= 0.90:
            notes.append("%s is historically elevated at the %.0fth percentile." % (short, percentile * 100.0))
        elif percentile <= 0.10:
            notes.append("%s is historically depressed at the %.0fth percentile." % (short, percentile * 100.0))
    if stats.get("robustZ") is not None and abs(stats["robustZ"]) >= 2:
        notes.append("Robust z-score is %.1f, so the level is far from its median regime." % stats["robustZ"])
    if stats.get("momentumZ") is not None and abs(stats["momentumZ"]) >= 2:
        notes.append("Recent 1-month move is unusual versus its own history (momentum z %.1f)." % stats["momentumZ"])
    if stats.get("daysLag") is not None and stats.get("stale"):
        notes.append("Latest observation is %s days old; treat the signal as stale until the source updates." % stats["daysLag"])
    if direction == "high" and stats.get("riskScore", 0) >= 70:
        notes.append("Risk direction is high: elevated readings are the dangerous side of this indicator.")
    elif direction == "low" and stats.get("riskScore", 0) >= 70:
        notes.append("Risk direction is low: compressed readings are the dangerous side of this indicator.")
    elif direction == "two-sided" and stats.get("riskScore", 0) >= 70:
        notes.append("This is a two-sided shock indicator; extremes in either direction can matter.")
    if not notes:
        notes.append("No historical extreme is firing; watch the latest change and related scenario drivers.")
    if latest is not None and stats.get("historicalMax") and stats.get("historicalMin"):
        pass
    return notes[:5]


def make_demo_points(metric):
    """Deterministic synthetic data for UI development when live network is absent."""
    rng = random.Random(metric.get("id", "demo"))
    frequency = metric.get("frequency", "monthly")
    if frequency == "daily":
        step = dt.timedelta(days=1)
    elif frequency == "weekly":
        step = dt.timedelta(days=7)
    else:
        step = dt.timedelta(days=30)
    start = dt.date(2000, 1, 3)
    end = dt.date.today()
    group = metric.get("group", "")
    base = 100.0
    amp = 10.0
    if group == "Rates":
        base, amp = 2.8, 1.8
    elif group == "Inflation":
        base, amp = 2.5, 1.2
    elif group == "Growth":
        base, amp = 80.0, 18.0
    elif group == "Credit":
        base, amp = 1.6, 1.2
    elif group == "Liquidity":
        base, amp = 800.0, 300.0
    elif group == "FX & Carry":
        base, amp = 110.0, 20.0
    elif group == "Commodities":
        base, amp = 75.0, 35.0
    elif group == "Markets":
        base, amp = 2500.0, 900.0
    if metric.get("short") == "VIX":
        base, amp = 18.0, 12.0
    if metric.get("transform") in ("yoy_pct", "annualized_pct_change", "pct_change"):
        base, amp = 100.0, 8.0
    if metric.get("transform") == "level" and metric.get("unit") in ("%", "pp", "% ann."):
        base, amp = 3.0, 1.4

    points = []
    day = start
    i = 0
    drift = 0.00008 if group in ("Markets", "Liquidity") else 0.0
    while day <= end:
        cyc = math.sin(i / 46.0) + 0.55 * math.sin(i / 173.0)
        noise = rng.gauss(0, amp * 0.05)
        value = base + amp * cyc + noise + base * drift * i
        for event_date, shock in (
            (dt.date(2008, 9, 15), 1.8),
            (dt.date(2020, 3, 16), 2.2),
            (dt.date(2022, 6, 13), 1.4),
            (dt.date(2024, 8, 5), 1.5),
        ):
            distance = abs((day - event_date).days)
            if distance < 60:
                direction = 1.0
                if metric.get("riskDirection") == "low":
                    direction = -1.0
                value += direction * amp * shock * (1 - distance / 60.0)
        value = max(value, 0.01)
        points.append({"date": day, "value": value})
        day += step
        i += 1
    return points


def score_metric_points(metric, points, source_status, fetched_at, source_url, include_points=False, as_of_date=None):
    if as_of_date:
        points = [point for point in points if point["date"] <= as_of_date]
    if not points:
        raise ValueError("no usable observations after transform")

    history_start = parse_date(metric.get("historyStart") or "1990-01-01")
    history = [point for point in points if history_start is None or point["date"] >= history_start]
    if len(history) < 8:
        history = points
    values = [point["value"] for point in history]
    sorted_values = sorted(values)
    latest_point = points[-1]
    latest_value = latest_point["value"]
    percentile = percentile_rank(values, latest_value)
    mean, std = mean_std(values)
    z_value = None
    if std:
        z_value = (latest_value - mean) / std
    rz_value = robust_z(values, latest_value)
    active_z = rz_value if rz_value is not None else z_value

    mode = change_mode(metric)
    ch_1m = latest_change(points, 30, mode)
    ch_3m = latest_change(points, 90, mode)
    ch_1y = latest_change(points, 365, mode)
    changes_1m = rolling_changes(points, 30, mode)
    ch_mean, ch_std = mean_std(changes_1m)
    momentum_z = None
    if ch_1m is not None and ch_std:
        momentum_z = (ch_1m - ch_mean) / ch_std

    direction = metric.get("riskDirection", "two-sided")
    any_tail = max(percentile, 1.0 - percentile) if percentile is not None else None
    risk_tail = risk_tail_from_percentile(percentile, direction)
    any_tail_score = score_tail(any_tail)
    risk_tail_score = score_tail(risk_tail)
    z_any_score = score_z(active_z)
    z_risk_score = score_directed_z(active_z, direction)
    momentum_any_score = score_z(momentum_z, cap=4.0)
    momentum_risk_score = score_directed_z(momentum_z, direction, cap=4.0)

    anomaly_score = max(
        any_tail_score * 0.52 + z_any_score * 0.30 + momentum_any_score * 0.18,
        momentum_any_score * 0.80,
    )
    risk_score = max(
        risk_tail_score * 0.54 + z_risk_score * 0.31 + momentum_risk_score * 0.15,
        momentum_risk_score * 0.78,
    )
    alert_score = max(risk_score, anomaly_score * 0.75)

    min_point = min(history, key=lambda p: p["value"])
    max_point = max(history, key=lambda p: p["value"])
    reference_day = as_of_date or dt.date.today()
    days_lag = (reference_day - latest_point["date"]).days
    stale = days_lag > stale_days_for(metric)

    stats = {
        "id": metric["id"],
        "sourceId": metric.get("sourceId"),
        "provider": metric.get("provider"),
        "name": metric.get("name"),
        "short": metric.get("short"),
        "group": metric.get("group"),
        "region": metric.get("region"),
        "country": metric.get("country"),
        "countryName": metric.get("countryName"),
        "countryShort": metric.get("countryShort"),
        "countryRegion": metric.get("countryRegion"),
        "countryRole": metric.get("countryRole"),
        "frequency": metric.get("frequency"),
        "unit": metric.get("unit"),
        "transform": metric.get("transform"),
        "riskDirection": direction,
        "signalFamily": metric_signal_family(metric),
        "tags": metric.get("tags", []),
        "why": metric.get("why"),
        "latest": round_or_none(latest_value, 6),
        "asOf": date_to_iso(latest_point["date"]),
        "analysisAsOf": date_to_iso(as_of_date) if as_of_date else None,
        "daysLag": days_lag,
        "stale": stale,
        "observations": len(points),
        "historyObservations": len(history),
        "historyStart": date_to_iso(history[0]["date"]) if history else None,
        "percentile": round_or_none(percentile, 5),
        "tailPercentile": round_or_none(any_tail, 5),
        "riskTailPercentile": round_or_none(risk_tail, 5),
        "zScore": round_or_none(z_value, 4),
        "robustZ": round_or_none(rz_value, 4),
        "momentumZ": round_or_none(momentum_z, 4),
        "changeMode": mode,
        "change1m": round_or_none(ch_1m, 5),
        "change3m": round_or_none(ch_3m, 5),
        "change1y": round_or_none(ch_1y, 5),
        "p05": round_or_none(quantile(sorted_values, 0.05), 6),
        "p25": round_or_none(quantile(sorted_values, 0.25), 6),
        "p50": round_or_none(quantile(sorted_values, 0.50), 6),
        "p75": round_or_none(quantile(sorted_values, 0.75), 6),
        "p95": round_or_none(quantile(sorted_values, 0.95), 6),
        "historicalMin": {"date": date_to_iso(min_point["date"]), "value": round_or_none(min_point["value"], 6)},
        "historicalMax": {"date": date_to_iso(max_point["date"]), "value": round_or_none(max_point["value"], 6)},
        "anomalyScore": int(round(clamp(anomaly_score, 0, 100))),
        "riskScore": int(round(clamp(risk_score, 0, 100))),
        "alertScore": int(round(clamp(alert_score, 0, 100))),
        "severity": severity(alert_score),
        "sourceStatus": source_status,
        "sourceUrl": source_url,
        "fetchedAt": fetched_at,
        "ok": True,
    }
    stats["notes"] = make_note(metric, stats)
    if include_points:
        stats["points"] = [point_to_wire(point) for point in points]
    else:
        stats["spark"] = sample_points(points[-600:], 140)
    return stats


def analyze_metric(metric, refresh=False, include_points=False, demo=False):
    try:
        if demo:
            raw_points = make_demo_points(metric)
            source_status = "demo"
            fetched_at = utc_now_iso()
            source_url = "demo://%s" % metric.get("id")
        else:
            raw_points, source_status, fetched_at, source_url = fetch_raw_points(metric, refresh=refresh)

        points = apply_transform(raw_points, metric)
        return score_metric_points(metric, points, source_status, fetched_at, source_url, include_points=include_points)
    except Exception as exc:
        return {
            "id": metric.get("id"),
            "sourceId": metric.get("sourceId"),
            "provider": metric.get("provider"),
            "name": metric.get("name"),
            "short": metric.get("short"),
            "group": metric.get("group"),
            "region": metric.get("region"),
            "country": metric.get("country"),
            "countryName": metric.get("countryName"),
            "countryShort": metric.get("countryShort"),
            "countryRegion": metric.get("countryRegion"),
            "countryRole": metric.get("countryRole"),
            "unit": metric.get("unit"),
            "riskDirection": metric.get("riskDirection", "two-sided"),
            "signalFamily": metric_signal_family(metric),
            "tags": metric.get("tags", []),
            "why": metric.get("why"),
            "ok": False,
            "error": str(exc),
            "alertScore": 0,
            "riskScore": 0,
            "anomalyScore": 0,
            "severity": "unavailable",
            "sourceStatus": "error",
        }


def latest_role_metric(metrics, role):
    candidates = [item for item in metrics if item.get("countryRole") == role and item.get("ok")]
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item.get("asOf") or "", item.get("alertScore", 0)), reverse=True)
    item = candidates[0]
    return {
        "id": item.get("id"),
        "short": item.get("short"),
        "latest": item.get("latest"),
        "unit": item.get("unit"),
        "asOf": item.get("asOf"),
        "severity": item.get("severity"),
        "riskScore": item.get("riskScore"),
        "alertScore": item.get("alertScore"),
    }


def country_score(country, metrics):
    selected = [item for item in metrics if item.get("country") == country.get("code") and item.get("ok")]
    if not selected:
        return {
            "code": country.get("code"),
            "name": country.get("name"),
            "short": country.get("short"),
            "region": country.get("region"),
            "score": 0,
            "severity": "unavailable",
            "available": 0,
            "summary": {},
            "drivers": [],
        }

    selected.sort(key=lambda item: item.get("riskScore", item.get("alertScore", 0)), reverse=True)
    top = selected[:5]
    top_scores = [item.get("riskScore", 0) for item in top]
    max_score = max(top_scores)
    avg_top = sum(top_scores) / float(len(top_scores))
    score = int(round(clamp(max_score * 0.50 + avg_top * 0.50, 0, 100)))
    summary = {
        role: latest_role_metric(selected, role)
        for role in ("debt", "yield10y", "spread", "inflation", "growth", "unemployment", "currentAccount")
    }
    return {
        "code": country.get("code"),
        "name": country.get("name"),
        "short": country.get("short"),
        "region": country.get("region"),
        "score": score,
        "severity": severity(score),
        "available": len(selected),
        "summary": summary,
        "drivers": [
            {
                "id": item.get("id"),
                "short": item.get("short"),
                "name": item.get("name"),
                "role": item.get("countryRole"),
                "riskScore": item.get("riskScore"),
                "alertScore": item.get("alertScore"),
                "latest": item.get("latest"),
                "unit": item.get("unit"),
                "asOf": item.get("asOf"),
                "severity": item.get("severity"),
            }
            for item in top[:4]
        ],
    }


def build_country_matrix(catalog, metrics):
    countries = [country_score(country, metrics) for country in catalog.get("countries", [])]
    countries.sort(key=lambda item: item.get("score", 0), reverse=True)
    return countries


def analyze_metric_as_of(metric, as_of_date, refresh=False, demo=False):
    try:
        if demo:
            raw_points = make_demo_points(metric)
            source_status = "demo"
            fetched_at = utc_now_iso()
            source_url = "demo://%s" % metric.get("id")
        else:
            raw_points, source_status, fetched_at, source_url = fetch_raw_points(metric, refresh=refresh)
        points = apply_transform(raw_points, metric)
        return score_metric_points(
            metric,
            points,
            source_status,
            fetched_at,
            source_url,
            include_points=False,
            as_of_date=as_of_date,
        )
    except Exception as exc:
        return {
            "id": metric.get("id"),
            "short": metric.get("short"),
            "name": metric.get("name"),
            "group": metric.get("group"),
            "unit": metric.get("unit"),
            "riskDirection": metric.get("riskDirection", "two-sided"),
            "signalFamily": metric_signal_family(metric),
            "ok": False,
            "error": str(exc),
            "alertScore": 0,
            "riskScore": 0,
            "anomalyScore": 0,
            "severity": "unavailable",
        }


def case_metric_payload(metric_id, historical, current, threshold, pillar=None):
    case_risk = historical.get("riskScore", 0) if historical.get("ok") else 0
    now_risk = current.get("riskScore", 0) if current and current.get("ok") else 0
    case_flag = case_risk >= threshold
    now_flag = now_risk >= threshold
    if case_flag and now_flag:
        status = "matched"
    elif case_flag:
        status = "not_yet"
    elif now_flag:
        status = "now_only"
    else:
        status = "quiet"
    return {
        "id": metric_id,
        "short": historical.get("short") or (current or {}).get("short"),
        "name": historical.get("name") or (current or {}).get("name"),
        "group": historical.get("group") or (current or {}).get("group"),
        "riskDirection": historical.get("riskDirection") or (current or {}).get("riskDirection"),
        "signalFamily": historical.get("signalFamily") or (current or {}).get("signalFamily") or metric_id,
        "why": (current or historical).get("why"),
        "pillar": pillar,
        "status": status,
        "caseFlag": case_flag,
        "nowFlag": now_flag,
        "case": {
            "ok": historical.get("ok"),
            "latest": historical.get("latest"),
            "unit": historical.get("unit"),
            "asOf": historical.get("asOf"),
            "percentile": historical.get("percentile"),
            "riskScore": historical.get("riskScore"),
            "alertScore": historical.get("alertScore"),
            "severity": historical.get("severity"),
            "error": historical.get("error"),
        },
        "now": {
            "ok": (current or {}).get("ok", False),
            "latest": (current or {}).get("latest"),
            "unit": (current or historical).get("unit"),
            "asOf": (current or {}).get("asOf"),
            "percentile": (current or {}).get("percentile"),
            "riskScore": (current or {}).get("riskScore"),
            "alertScore": (current or {}).get("alertScore"),
            "severity": (current or {}).get("severity"),
        },
    }


def build_case_studies(catalog, current_by_id, refresh=False, demo=False):
    metric_configs = {metric.get("id"): metric for metric in catalog.get("metrics", [])}
    studies = []
    for study in catalog.get("caseStudies", []):
        as_of_date = parse_date(study.get("asOf"))
        if not as_of_date:
            continue
        threshold = int(study.get("threshold") or 45)
        pillars_config = study.get("pillars") or []
        metric_to_pillar = {}
        ordered_metric_ids = []
        if pillars_config:
            for pillar in pillars_config:
                for metric_id in pillar.get("metricIds", []):
                    if metric_id not in ordered_metric_ids:
                        ordered_metric_ids.append(metric_id)
                    metric_to_pillar.setdefault(
                        metric_id,
                        {
                            "id": pillar.get("id"),
                            "name": pillar.get("name"),
                        },
                    )
        for metric_id in study.get("metricIds", []):
            if metric_id not in ordered_metric_ids:
                ordered_metric_ids.append(metric_id)

        rows = []
        for metric_id in ordered_metric_ids:
            metric_config = metric_configs.get(metric_id)
            if not metric_config:
                continue
            historical = analyze_metric_as_of(metric_config, as_of_date, refresh=refresh, demo=demo)
            current = current_by_id.get(metric_id)
            rows.append(case_metric_payload(metric_id, historical, current, threshold, metric_to_pillar.get(metric_id)))

        case_flags = [row for row in rows if row.get("caseFlag")]
        matched = [row for row in rows if row.get("status") == "matched"]
        not_yet = [row for row in rows if row.get("status") == "not_yet"]
        now_only = [row for row in rows if row.get("status") == "now_only"]
        unique_case_flags = unique_best_by_family(
            case_flags, lambda row: row.get("case", {}).get("riskScore", 0)
        )
        unique_matched = unique_best_by_family(
            matched, lambda row: row.get("case", {}).get("riskScore", 0)
        )
        unique_not_yet = unique_best_by_family(
            not_yet, lambda row: row.get("case", {}).get("riskScore", 0)
        )
        unique_now_only = unique_best_by_family(
            now_only, lambda row: row.get("now", {}).get("riskScore", 0)
        )
        if unique_case_flags:
            weighted = [
                clamp((row["now"].get("riskScore") or 0) / float(max(row["case"].get("riskScore") or 1, 1)) * 100.0, 0.0, 100.0)
                for row in unique_case_flags
            ]
            match_score = int(round(sum(weighted) / float(len(weighted))))
        else:
            match_score = 0

        rows.sort(
            key=lambda row: (
                {"matched": 0, "not_yet": 1, "now_only": 2, "quiet": 3}.get(row.get("status"), 4),
                -(row.get("case", {}).get("riskScore") or 0),
                -(row.get("now", {}).get("riskScore") or 0),
            )
        )

        pillars = []
        for pillar in pillars_config:
            pillar_ids = set(pillar.get("metricIds", []))
            pillar_rows = [row for row in rows if row.get("id") in pillar_ids]
            pillar_case_flags = [row for row in pillar_rows if row.get("caseFlag")]
            unique_pillar_case_flags = unique_best_by_family(
                pillar_case_flags, lambda row: row.get("case", {}).get("riskScore", 0)
            )
            unique_pillar_matched = unique_best_by_family(
                [row for row in pillar_rows if row.get("status") == "matched"],
                lambda row: row.get("case", {}).get("riskScore", 0),
            )
            unique_pillar_not_yet = unique_best_by_family(
                [row for row in pillar_rows if row.get("status") == "not_yet"],
                lambda row: row.get("case", {}).get("riskScore", 0),
            )
            unique_pillar_now_only = unique_best_by_family(
                [row for row in pillar_rows if row.get("status") == "now_only"],
                lambda row: row.get("now", {}).get("riskScore", 0),
            )
            if unique_pillar_case_flags:
                pillar_weighted = [
                    clamp((row["now"].get("riskScore") or 0) / float(max(row["case"].get("riskScore") or 1, 1)) * 100.0, 0.0, 100.0)
                    for row in unique_pillar_case_flags
                ]
                pillar_match_score = int(round(sum(pillar_weighted) / float(len(pillar_weighted))))
            else:
                pillar_match_score = 0
            pillars.append(
                {
                    "id": pillar.get("id"),
                    "name": pillar.get("name"),
                    "historicalRead": pillar.get("historicalRead"),
                    "todayRead": pillar.get("todayRead"),
                    "matchScore": pillar_match_score,
                    "caseAlertCount": len(unique_pillar_case_flags),
                    "matchedCount": len(unique_pillar_matched),
                    "notYetCount": len(unique_pillar_not_yet),
                    "nowOnlyCount": len(unique_pillar_now_only),
                    "metricIds": list(pillar.get("metricIds", [])),
                }
            )

        studies.append(
            {
                "id": study.get("id"),
                "name": study.get("name"),
                "asOf": date_to_iso(as_of_date),
                "shockDate": study.get("shockDate"),
                "market": study.get("market"),
                "summary": study.get("summary"),
                "lesson": study.get("lesson"),
                "frameworkBasis": study.get("frameworkBasis"),
                "currentVerification": study.get("currentVerification"),
                "threshold": threshold,
                "matchScore": match_score,
                "caseAlertCount": len(unique_case_flags),
                "matchedCount": len(unique_matched),
                "notYetCount": len(unique_not_yet),
                "nowOnlyCount": len(unique_now_only),
                "pillars": pillars,
                "metrics": rows,
            }
        )
    studies.sort(key=lambda item: item.get("asOf") or "")
    return studies


def scenario_score(scenario, metrics_by_id):
    selected = []
    metric_ids = set(scenario.get("metricIds", []))
    tags = set(scenario.get("tags", []))
    for metric in metrics_by_id.values():
        if not metric.get("ok"):
            continue
        if metric["id"] in metric_ids or tags.intersection(metric.get("tags", [])):
            selected.append(metric)
    if not selected:
        return {
            "id": scenario["id"],
            "name": scenario["name"],
            "description": scenario.get("description"),
            "score": 0,
            "severity": "unavailable",
            "drivers": [],
            "available": 0,
        }
    selected.sort(key=lambda item: item.get("riskScore", item.get("alertScore", 0)), reverse=True)
    unique_selected = unique_best_by_family(
        selected, lambda item: item.get("riskScore", item.get("alertScore", 0))
    )
    top = unique_selected[:6]
    top_scores = [item.get("riskScore", 0) for item in top]
    max_score = max(top_scores)
    avg_top = sum(top_scores) / float(len(top_scores))
    score = int(round(clamp(max_score * 0.45 + avg_top * 0.55, 0, 100)))
    return {
        "id": scenario["id"],
        "name": scenario["name"],
        "description": scenario.get("description"),
        "score": score,
        "severity": severity(score),
        "drivers": [
            {
                "id": item["id"],
                "short": item.get("short"),
                "name": item.get("name"),
                "riskScore": item.get("riskScore"),
                "alertScore": item.get("alertScore"),
                "latest": item.get("latest"),
                "unit": item.get("unit"),
                "asOf": item.get("asOf"),
                "severity": item.get("severity"),
                "signalFamily": item.get("signalFamily"),
            }
            for item in top[:5]
        ],
        "available": len(selected),
        "uniqueAvailable": len(unique_selected),
    }


def build_monitor(refresh=False, demo=False):
    catalog = load_catalog()
    metrics = catalog.get("metrics", [])
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(analyze_metric, metric, refresh, False, demo): metric for metric in metrics
        }
        for future in concurrent.futures.as_completed(future_map):
            results.append(future.result())

    order = {metric["id"]: idx for idx, metric in enumerate(metrics)}
    results.sort(key=lambda item: order.get(item.get("id"), 9999))
    metrics_by_id = {item["id"]: item for item in results if item.get("id")}
    scenarios = [scenario_score(item, metrics_by_id) for item in catalog.get("scenarios", [])]
    scenarios.sort(key=lambda item: item.get("score", 0), reverse=True)
    countries = build_country_matrix(catalog, results)
    case_studies = build_case_studies(catalog, metrics_by_id, refresh=refresh, demo=demo)

    ok = [item for item in results if item.get("ok")]
    unavailable = [item for item in results if not item.get("ok")]
    ranked = sorted(ok, key=lambda item: item.get("alertScore", 0), reverse=True)
    extremes = [item for item in ranked if item.get("alertScore", 0) >= 62][:12]
    source_counts = {}
    for item in results:
        status = item.get("sourceStatus", "unknown")
        source_counts[status] = source_counts.get(status, 0) + 1

    return {
        "generatedAt": utc_now_iso(),
        "demo": demo,
        "cacheTtlSeconds": CACHE_TTL_SECONDS,
        "sourceCounts": source_counts,
        "coverage": {"ok": len(ok), "unavailable": len(unavailable), "total": len(results)},
        "sources": catalog.get("sources", []),
        "events": catalog.get("events", []),
        "scenarios": scenarios,
        "countries": countries,
        "caseStudies": case_studies,
        "extremes": [
            {
                "id": item["id"],
                "short": item.get("short"),
                "name": item.get("name"),
                "group": item.get("group"),
                "latest": item.get("latest"),
                "unit": item.get("unit"),
                "asOf": item.get("asOf"),
                "percentile": item.get("percentile"),
                "riskScore": item.get("riskScore"),
                "alertScore": item.get("alertScore"),
                "severity": item.get("severity"),
                "notes": item.get("notes", [])[:2],
            }
            for item in extremes
        ],
        "metrics": results,
    }


def build_series(metric_id, refresh=False, demo=False):
    catalog = load_catalog()
    for metric in catalog.get("metrics", []):
        if metric.get("id") == metric_id:
            result = analyze_metric(metric, refresh=refresh, include_points=True, demo=demo)
            result["events"] = catalog.get("events", [])
            return result
    return None


def json_response(handler, status, payload):
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.end_headers()
    handler.wfile.write(body)


class MacroHandler(SimpleHTTPRequestHandler):
    server_version = "MacroShockRadar/1.0"

    def log_message(self, fmt, *args):
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

    def translate_path(self, path):
        parts = public_path_parts(path)
        candidate = os.path.realpath(os.path.join(PUBLIC_DIR, *parts))
        public_root = os.path.realpath(PUBLIC_DIR)
        if candidate != public_root and not candidate.startswith(public_root + os.sep):
            return os.path.join(public_root, "__blocked__")
        return candidate

    def end_headers(self):
        self.send_header("X-Content-Type-Options", "nosniff")
        super().end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        try:
            if parsed.path == "/api/health":
                json_response(self, 200, {"ok": True, "generatedAt": utc_now_iso()})
                return
            if parsed.path == "/api/catalog":
                catalog = load_catalog()
                json_response(
                    self,
                    200,
                    {
                        "sources": catalog.get("sources", []),
                        "scenarios": catalog.get("scenarios", []),
                        "events": catalog.get("events", []),
                        "metrics": catalog.get("metrics", []),
                    },
                )
                return
            if parsed.path == "/api/snapshot":
                payload = load_snapshot()
                if payload is None:
                    json_response(self, 404, {"ok": False, "error": "snapshot unavailable"})
                else:
                    json_response(self, 200, payload)
                return
            if parsed.path == "/api/monitor":
                refresh = query.get("refresh", ["0"])[0] in ("1", "true", "yes")
                demo = query.get("demo", ["0"])[0] in ("1", "true", "yes")
                payload = build_monitor(refresh=refresh, demo=demo)
                json_response(self, 200, payload)
                return
            if parsed.path == "/api/series":
                metric_id = query.get("id", [""])[0]
                refresh = query.get("refresh", ["0"])[0] in ("1", "true", "yes")
                demo = query.get("demo", ["0"])[0] in ("1", "true", "yes")
                payload = build_series(metric_id, refresh=refresh, demo=demo)
                if payload is None:
                    json_response(self, 404, {"ok": False, "error": "unknown metric id"})
                else:
                    json_response(self, 200, payload)
                return
        except Exception as exc:
            json_response(
                self,
                500,
                {
                    "ok": False,
                    "error": str(exc),
                    "trace": traceback.format_exc().splitlines()[-6:],
                },
            )
            return

        path = self.translate_path(self.path)
        if is_private_static_request(self.path):
            self.send_error(404, "Not found")
            return
        if not os.path.exists(path):
            self.send_error(404, "Not found")
            return
        ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
        try:
            with open(path, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except OSError:
            self.send_error(404, "Not found")


def make_server(start_port, host=DEFAULT_HOST):
    last_error = None
    for port in range(start_port, start_port + 30):
        try:
            httpd = ThreadingHTTPServer((host, port), MacroHandler)
            httpd.daemon_threads = True
            return httpd, port
        except OSError as exc:
            last_error = exc
    raise last_error


def main():
    ensure_dirs()
    try:
        socket.gethostbyname("fred.stlouisfed.org")
    except Exception:
        pass
    httpd, port = make_server(DEFAULT_PORT, DEFAULT_HOST)
    display_host = "127.0.0.1" if DEFAULT_HOST in ("0.0.0.0", "") else DEFAULT_HOST
    print("Macro Shock Radar running at http://%s:%s" % (display_host, port))
    print("Live monitor: http://%s:%s" % (display_host, port))
    print("Demo mode:    http://%s:%s/?demo=1" % (display_host, port))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down")
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
