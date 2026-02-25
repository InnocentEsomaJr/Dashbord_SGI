import calendar
import base64
import json
import os
import re
import socket
import tomllib
import unicodedata
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from plotly.subplots import make_subplots
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


st.set_page_config(page_title="Dashboard SGI DHIS2", layout="wide")

SGI_LIST = ["MPOX", "MVE", "CHOLERA"]

SECTION_TABS = [
    ("vue_ensemble", "Vue d'ensemble"),
    ("surveillance", "Surveillance"),
    ("inrb", "INRB"),
    ("pev", "PEV"),
]

SECTION_ALIASES = {
    "vue_ensemble": ["vue_ensemble", "vue", "overview", "global"],
    "surveillance": ["surveillance", "epi", "epidemiologie"],
    "inrb": ["inrb", "labo", "laboratoire", "lab"],
    "pev": ["pev", "vaccin", "vaccination", "immunisation"],
}

SECTION_KEYWORDS = {
    "vue_ensemble": ["cas", "suspect", "confirme", "deces", "letalite"],
    "surveillance": ["surveillance", "alerte", "notification", "investigation", "cas"],
    "inrb": ["inrb", "labo", "laboratoire", "test", "echantillon", "positif", "negatif", "pcr"],
    "pev": ["pev", "vaccin", "vaccination", "immunisation", "dose", "couverture"],
}

FRENCH_MONTHS = [
    "Janvier",
    "Fevrier",
    "Mars",
    "Avril",
    "Mai",
    "Juin",
    "Juillet",
    "Aout",
    "Septembre",
    "Octobre",
    "Novembre",
    "Decembre",
]

DEFAULT_OU_SCOPE = "USER_ORGUNIT_DESCENDANTS"
ANALYTICS_OU_SCOPE_FALLBACKS = [
    "USER_ORGUNIT_DESCENDANTS",
    "USER_ORGUNIT_GRANDCHILDREN",
    "USER_ORGUNIT_CHILDREN",
    "USER_ORGUNIT",
]

DEFAULT_DATA_SOURCES = {
    "EZD": "https://ezd.snisrdc.com/dhis",
}

AGE_GROUP_PATTERNS = [
    (
        "0-11 mois",
        ["0-11", "0 a 11", "0 a11", "0-11 mois", "0_11", "0 11 mois", "<1 an", "moins d1 an", "moins de 1 an"],
    ),
    ("12-59 mois", ["12-59", "12 a 59", "12 a59", "12-59 mois", "12_59", "12 59 mois", "1-4 ans", "1 a 4 ans"]),
    ("< 5 Ans", ["<5", "< 5", "moins de 5", "0-4", "0 a 4", "0_4", "0 a4"]),
    ("5 - 14 Ans", ["5-14", "5 a 14", "5 a14", "5_14"]),
    ("15 - 19 Ans", ["15-19", "15 a 19", "15 a19", "15_19"]),
    ("20 - 40 Ans", ["20-40", "20 a 40", "20 a40", "20_40"]),
    ("> 40 Ans", [">40", "> 40", "plus de 40", "40+", ">40 ans", "40 et plus"]),
    ("N/A", ["n/a", "na", "non precise", "non renseigne", "inconnu"]),
]

AGE_GROUP_ORDER = [
    "0-11 mois",
    "12-59 mois",
    "< 5 Ans",
    "5 - 14 Ans",
    "15 - 19 Ans",
    "20 - 40 Ans",
    "> 40 Ans",
    "N/A",
]

CHART_COLORS = {
    "cases": "#2a9d8f",
    "deaths": "#e76f51",
    "lethality": "#c44536",
    "positivity": "#577590",
    "positive": "#3a7d44",
    "negative": "#d1495b",
    "confirmed": "#f4a261",
    "suspects": "#4d908e",
}

PIE_COLORS_MAIN = ["#4d908e", "#f4a261", "#f28482", "#84a59d", "#577590", "#e9c46a"]
PIE_COLORS_ALT = ["#577590", "#f4a261", "#e76f51", "#2a9d8f", "#84a59d", "#9c89b8"]

ICON_BY_TOPIC = {
    "cas": "&#128101;",
    "suspect": "&#128101;",
    "deces": "&#9760;",
    "letalite": "&#128200;",
    "zone": "&#128506;",
    "dps": "&#128205;",
    "province": "&#128205;",
    "labo": "&#129514;",
    "test": "&#129514;",
    "vaccin": "&#128137;",
    "dose": "&#128137;",
    "alerte": "&#9888;",
}

PROVINCE_COORDS = {
    "kinshasa": ("Kinshasa", -4.325, 15.322),
    "kongo central": ("Kongo Central", -5.252, 14.865),
    "kwango": ("Kwango", -6.200, 17.483),
    "kwilu": ("Kwilu", -5.040, 18.817),
    "mai ndombe": ("Mai-Ndombe", -2.250, 18.300),
    "kasai": ("Kasai", -6.000, 21.500),
    "kasai central": ("Kasai Central", -6.150, 23.600),
    "kasai oriental": ("Kasai Oriental", -6.120, 23.590),
    "lomami": ("Lomami", -6.140, 24.480),
    "sankuru": ("Sankuru", -2.850, 23.430),
    "maniema": ("Maniema", -2.950, 26.200),
    "sud kivu": ("Sud-Kivu", -3.000, 28.100),
    "nord kivu": ("Nord-Kivu", -0.780, 29.250),
    "ituri": ("Ituri", 1.550, 30.250),
    "hautele": ("Haut-Uele", 3.250, 27.600),
    "basuele": ("Bas-Uele", 3.650, 24.950),
    "tshopo": ("Tshopo", 0.520, 25.200),
    "mongala": ("Mongala", 2.300, 21.300),
    "equateur": ("Equateur", 0.100, 19.800),
    "sud ubangi": ("Sud-Ubangi", 3.300, 19.000),
    "nord ubangi": ("Nord-Ubangi", 4.000, 22.000),
    "tshuapa": ("Tshuapa", -0.100, 22.700),
    "tanganyika": ("Tanganyika", -6.500, 27.450),
    "haut lomami": ("Haut-Lomami", -7.800, 24.500),
    "lualaba": ("Lualaba", -10.700, 25.300),
    "haut katanga": ("Haut-Katanga", -11.670, 27.480),
}


def _load_toml_file(path):
    try:
        return tomllib.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_local_secrets():
    script_dir = Path(__file__).resolve().parent
    candidates = [
        script_dir / ".streamlit" / "secrets.toml",
        Path.cwd() / ".streamlit" / "secrets.toml",
    ]
    merged = {}
    for candidate in candidates:
        if candidate.exists():
            merged.update(_load_toml_file(candidate))
    return merged


def _load_env_file_values():
    script_dir = Path(__file__).resolve().parent
    candidates = [
        script_dir / ".streamlit" / ".env",
        Path.cwd() / ".streamlit" / ".env",
        script_dir.parent / "Extraction_Favori" / ".streamlit" / ".env",
    ]
    values = {}
    for path in candidates:
        if not path.exists():
            continue
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                clean = line.strip()
                if not clean or clean.startswith("#") or "=" not in clean:
                    continue
                key, raw_val = clean.split("=", 1)
                key = key.strip()
                val = raw_val.strip().strip('"').strip("'")
                if key and val and key not in values:
                    values[key] = val
        except Exception:
            continue
    return values


def _candidate_base_urls(base_url):
    root = str(base_url or "").strip().rstrip("/")
    if not root:
        return []

    candidates = [root]
    if not root.lower().endswith("/dhis"):
        candidates.append(f"{root}/dhis")

    deduped = []
    seen = set()
    for candidate in candidates:
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _looks_like_valid_me_response(response):
    if response.status_code in (401, 403):
        return True
    if response.status_code != 200:
        return False
    content_type = str(response.headers.get("Content-Type", "")).lower()
    if "application/json" not in content_type:
        return False
    try:
        payload = response.json()
    except Exception:
        return False
    return isinstance(payload, Mapping) and (
        "username" in payload or "id" in payload or "displayName" in payload
    )


def resolve_dhis2_base_url(base_url, username=None, password=None):
    candidates = _candidate_base_urls(base_url)
    if not candidates:
        return ""

    auth_tuple = None
    if username and password:
        auth_tuple = (username, password)

    for candidate in candidates:
        for suffix in ("/api/me", "/api/me.json"):
            test_url = f"{candidate}{suffix}"
            try:
                response = requests.get(
                    test_url,
                    auth=auth_tuple,
                    timeout=10,
                    allow_redirects=False,
                )
            except Exception:
                continue
            if _looks_like_valid_me_response(response):
                return candidate
    return candidates[0]


def apply_blue_theme():
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Nunito+Sans:wght@400;600;700;800&display=swap');
        :root {
            --bsq-dark: #0f1724;
            --bsq-mid: #1b2638;
            --bsq-accent: #f4a261;
            --bsq-red: #d1495b;
            --bsq-soft: #0f1a2b;
            --bsq-border: #2a3850;
            --bsq-card: #111d2f;
            --bsq-bg: #0a1220;
            --bsq-text: #c9d5e4;
            --bsq-text-soft: #9eb0c6;
            --bsq-title: #e8eef6;
        }
        html, body, [data-testid="stAppViewContainer"], [data-testid="stSidebar"], .stMarkdown, .stTextInput, .stSelectbox {
            font-family: 'Nunito Sans', 'Segoe UI', Tahoma, sans-serif;
        }
        [data-testid="stAppViewContainer"], .stApp {
            background: radial-gradient(circle at 20% 0%, #14233a 0%, #0a1220 45%, #08111f 100%);
            color: var(--bsq-text);
        }
        .stApp {
            color: var(--bsq-text);
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #1e2331 0%, #161b27 100%);
            border-right: 1px solid #30394d;
        }
        [data-testid="stSidebar"] * {
            color: #d5deea !important;
        }
        [data-testid="stSidebar"] .stTextInput input,
        [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div,
        [data-testid="stSidebar"] [data-baseweb="input"] > div {
            background: #0b1320 !important;
            border: 1px solid #36465f !important;
            color: #d5deea !important;
        }
        [data-testid="stSidebar"] .stButton button {
            background: #2a3346;
            border: 1px solid #42526b;
            color: #e6edf7;
            border-radius: 10px;
        }
        [data-testid="stSidebar"] .stButton button:hover {
            background: #35425a;
            border-color: #5a6d8a;
            color: #ffffff;
        }
        header[data-testid="stHeader"] {
            display: none;
        }
        [data-testid="stToolbar"] {
            display: none !important;
        }
        [data-testid="stDecoration"] {
            display: none;
        }
        #MainMenu {
            visibility: hidden;
        }
        footer {
            visibility: hidden;
        }
        .main .block-container {
            padding-top: 0.6rem;
            max-width: 1280px;
        }
        .bsq-head {
            background: #ececec;
            border: 1px solid #cccccc;
            border-radius: 0;
            padding: 7px 10px;
            margin-bottom: 8px;
            display: flex;
            align-items: center;
            justify-content: flex-start;
            gap: 16px;
        }
        .bsq-logos {
            display: flex;
            gap: 10px;
            align-items: center;
            flex-shrink: 0;
        }
        .bsq-logo-wrap {
            width: 94px;
            height: 94px;
            background: #f4f4f4;
            border: 1px solid #c8c8c8;
            border-radius: 4px;
            display: flex;
            align-items: center;
            justify-content: center;
            overflow: hidden;
            box-sizing: border-box;
        }
        .bsq-logo-img {
            width: 100%;
            height: 100%;
            object-fit: contain;
            display: block;
        }
        .bsq-logo-fallback {
            width: 100%;
            height: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.78rem;
            font-weight: 700;
            color: #666;
        }
        .bsq-ministry {
            min-height: 80px;
            padding: 6px 8px 6px 12px;
            border-left: 4px solid #2f6da6;
            position: relative;
            color: #4f4f4f;
            font-size: 0.66rem;
            font-weight: 800;
            text-transform: uppercase;
            line-height: 1.1;
            display: flex;
            align-items: center;
        }
        .bsq-ministry::before {
            content: "";
            position: absolute;
            left: -8px;
            top: 0;
            height: 100%;
            width: 2px;
            background: #c94949;
        }
        .bsq-head-main {
            flex: 1;
            min-width: 0;
        }
        .bsq-head-main h2 {
            margin: 0;
            font-size: 2.45rem;
            font-weight: 700;
            font-family: "Georgia", "Times New Roman", serif;
            color: #7e7e7e;
            line-height: 1.05;
        }
        .bsq-head-main p {
            margin: 2px 0 0 0;
            color: #7f7f7f;
            font-size: 1.02rem;
            font-weight: 700;
            font-family: "Georgia", "Times New Roman", serif;
        }
        .bsq-filter-bar {
            background: #17273f;
            border-radius: 0 0 12px 12px;
            padding: 8px 12px;
            margin-bottom: 10px;
            color: var(--bsq-text);
            border-left: 1px solid var(--bsq-border);
            border-right: 1px solid var(--bsq-border);
            border-bottom: 1px solid var(--bsq-border);
        }
        .bsq-filter-grid {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr 1fr auto;
            gap: 10px;
            align-items: center;
        }
        .bsq-filter-label {
            font-size: 0.72rem;
            color: var(--bsq-text-soft);
            text-transform: none;
            margin-bottom: 2px;
            font-weight: 700;
        }
        .bsq-filter-value {
            font-size: 0.88rem;
            font-weight: 600;
            color: var(--bsq-text);
        }
        .bsq-source {
            text-align: right;
            color: #f4b183;
            font-size: 0.78rem;
            font-weight: 600;
            line-height: 1.25;
        }
        .bsq-title {
            margin: 8px 0 6px 0;
            color: var(--bsq-title);
            font-size: 2rem;
            font-weight: 800;
        }
        .bsq-panel {
            background: var(--bsq-card);
            border: 1px solid var(--bsq-border);
            border-radius: 12px;
            padding: 10px 12px;
            margin-bottom: 10px;
            min-height: 180px;
            box-shadow: 0 6px 14px rgba(4, 10, 18, 0.35);
        }
        .bsq-panel h4 {
            margin: 0;
            font-size: 1.12rem;
            color: #d9e5f3;
            font-weight: 800;
        }
        .bsq-sub {
            margin: 4px 0 12px 0;
            font-size: 0.8rem;
            color: var(--bsq-text-soft);
        }
        .bsq-stat-grid {
            display: grid;
            gap: 8px 12px;
            grid-template-columns: repeat(3, minmax(0, 1fr));
        }
        .bsq-stat-item {
            display: flex;
            gap: 8px;
            align-items: flex-start;
            min-height: 52px;
        }
        .bsq-icon {
            width: 34px;
            height: 34px;
            border: 1px solid #314867;
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            background: #182842;
            font-size: 1.02rem;
        }
        .bsq-stat-label {
            font-size: 0.84rem;
            color: #a7bbd3;
            line-height: 1.05;
        }
        .bsq-stat-value {
            font-size: 2rem;
            color: #f1f5fb;
            font-weight: 800;
            line-height: 1.0;
            margin-top: 1px;
        }
        .bsq-compare {
            margin-top: 2px;
            margin-bottom: 8px;
        }
        .bsq-compare h5 {
            margin: 0 0 8px 0;
            color: #bfd0e4;
            font-size: 1.05rem;
        }
        .bsq-foot {
            margin-top: 6px;
            color: #8fa6be;
            font-size: 0.82rem;
            border-top: 1px solid #2a3e58;
            padding-top: 7px;
        }
        div[data-testid="stMetric"] {
            background: #13233a;
            border: 1px solid var(--bsq-border);
            border-radius: 10px;
            padding: 6px 8px;
        }
        .stAlert {
            background: #172941;
            border: 1px solid #2e4461;
            color: #d3dfed;
        }
        [data-testid="stDataFrame"] {
            background: #111d2f;
            border: 1px solid #2a3d57;
            border-radius: 10px;
        }
        @media (max-width: 980px) {
            .bsq-head {
                flex-direction: column;
                align-items: flex-start;
                gap: 8px;
            }
            .bsq-logo-wrap {
                width: 72px;
                height: 72px;
            }
            .bsq-head-main h2 {
                font-size: 1.9rem;
            }
            .bsq-head-main p {
                font-size: 0.88rem;
            }
            .bsq-filter-grid {
                grid-template-columns: 1fr 1fr;
            }
            .bsq-source {
                text-align: left;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def normalize_text(value):
    text = "" if value is None else str(value)
    text = (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
        .strip()
    )
    return " ".join(text.split())


def read_config_value(key, default=None):
    value = None
    try:
        value = st.secrets.get(key)
    except Exception:
        value = None
    if value is None or (isinstance(value, str) and value.strip() == ""):
        local_secrets = _load_local_secrets()
        value = local_secrets.get(key)
    if value is None or (isinstance(value, str) and value.strip() == ""):
        value = os.getenv(key)
    if value is None or (isinstance(value, str) and value.strip() == ""):
        env_values = _load_env_file_values()
        value = env_values.get(key)
    if value is None:
        return default
    return value


@st.cache_data(ttl=86400)
def image_to_data_uri(path):
    try:
        image_path = Path(path)
        if not image_path.exists():
            return ""
        ext = image_path.suffix.lower().lstrip(".")
        mime = "image/png" if ext in {"png", "jpg", "jpeg", "webp"} else "image/png"
        if ext == "svg":
            mime = "image/svg+xml"
        encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
        return f"data:{mime};base64,{encoded}"
    except Exception:
        return ""


def read_int_config_value(key, default_value):
    raw_value = read_config_value(key)
    if raw_value is None:
        return default_value
    try:
        parsed = int(str(raw_value).strip())
        return parsed if parsed > 0 else default_value
    except Exception:
        return default_value


def read_json_config(key, default=None):
    raw = read_config_value(key)
    if raw is None:
        return default
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, list):
        return raw
    raw_text = str(raw).strip()
    if not raw_text:
        return default
    try:
        return json.loads(raw_text)
    except Exception:
        return default


def normalize_url_key(url):
    return str(url or "").strip().rstrip("/").lower()


def sanitize_ids(values):
    clean = []
    for value in values or []:
        text = str(value).strip()
        if not text:
            continue
        if text.lower() in {"nan", "<na>", "none", "null"}:
            continue
        clean.append(text)
    return clean


def resolve_data_sources():
    mapped_ezd_url = ""
    raw_sources = read_json_config("DHIS2_DATA_SOURCES", default={}) or {}
    if isinstance(raw_sources, Mapping):
        mapped_ezd_url = str(dict_get_ci(raw_sources, "EZD") or "").strip()
        if not mapped_ezd_url and len(raw_sources) == 1:
            mapped_ezd_url = str(next(iter(raw_sources.values()))).strip()

    ezd_url = str(
        mapped_ezd_url
        or read_config_value("DHIS2_URL_EZD")
        or read_config_value("DHIS2_URL")
        or DEFAULT_DATA_SOURCES["EZD"]
    ).strip().rstrip("/")
    if not ezd_url:
        return {}
    return {"EZD": ezd_url}


def as_list(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def is_truthy(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "oui", "on"}


def dict_get_ci(mapping, key):
    if not isinstance(mapping, Mapping):
        return None
    wanted = str(key).strip().lower()
    for k, v in mapping.items():
        if str(k).strip().lower() == wanted:
            return v
    return None


def format_period_label(period_code):
    code = str(period_code)
    if len(code) == 6 and code.isdigit():
        year = code[:4]
        month = int(code[4:])
        month_label = FRENCH_MONTHS[month - 1] if 1 <= month <= 12 else calendar.month_name[month]
        return f"{month_label} {year}"
    return code


def build_period_catalog(years_back=4):
    today = date.today()
    first_year = today.year - years_back
    codes = []
    for year in range(first_year, today.year + 1):
        last_month = 12 if year < today.year else today.month
        for month in range(1, last_month + 1):
            codes.append(f"{year}{month:02d}")
    return codes


def build_period_range(start_code, end_code, period_catalog):
    if not period_catalog:
        return []
    if start_code not in period_catalog or end_code not in period_catalog:
        return [period_catalog[-1]]
    start_idx = period_catalog.index(start_code)
    end_idx = period_catalog.index(end_code)
    if start_idx > end_idx:
        start_idx, end_idx = end_idx, start_idx
    return period_catalog[start_idx : end_idx + 1]


def extract_http_error_message(exc):
    response = getattr(exc, "response", None)
    if response is None:
        return str(exc)

    details = []
    try:
        payload = response.json()
    except Exception:
        payload = None

    if isinstance(payload, Mapping):
        for key in [
            "message",
            "description",
            "httpStatus",
            "status",
            "response",
            "error",
        ]:
            value = payload.get(key)
            if value:
                details.append(str(value).strip())

        if "response" in payload and isinstance(payload.get("response"), Mapping):
            nested = payload.get("response") or {}
            for key in ["message", "description", "status", "error"]:
                value = nested.get(key)
                if value:
                    details.append(str(value).strip())

    body_text = str(response.text or "").strip()
    if body_text and not details:
        details.append(body_text[:500])

    clean_details = [item for item in details if item]
    suffix = f" - {' | '.join(clean_details[:2])}" if clean_details else ""
    return f"HTTP {response.status_code}{suffix}"


def is_http_409(exc):
    response = getattr(exc, "response", None)
    return response is not None and int(response.status_code) == 409


def build_analytics_ou_scopes(primary_scope):
    scopes = []
    first = str(primary_scope or "").strip().upper()
    if first:
        scopes.append(first)

    configured = read_json_config("DHIS2_ANALYTICS_OU_SCOPE_FALLBACKS", default=[]) or []
    for scope in as_list(configured):
        clean_scope = str(scope or "").strip().upper()
        if clean_scope and clean_scope not in scopes:
            scopes.append(clean_scope)

    for fallback_scope in ANALYTICS_OU_SCOPE_FALLBACKS:
        if fallback_scope not in scopes:
            scopes.append(fallback_scope)
    return scopes


def build_analytics_ou_dimensions(primary_scope, explicit_ou_ids=None):
    dimensions = build_analytics_ou_scopes(primary_scope)

    clean_ids = sanitize_ids(explicit_ou_ids or [])
    if not clean_ids:
        return dimensions

    per_dimension = read_int_config_value("DHIS2_OU_ID_CHUNK_SIZE", 60)
    per_dimension = max(5, min(200, per_dimension))
    for idx in range(0, len(clean_ids), per_dimension):
        chunk = clean_ids[idx : idx + per_dimension]
        token = ";".join(chunk)
        if token and token not in dimensions:
            dimensions.append(token)
    return dimensions


def query_analytics_chunk(client, dx_chunk, pe_list, ou_dimension_value):
    params = [
        ("dimension", f"dx:{';'.join(dx_chunk)}"),
        ("dimension", f"pe:{';'.join(pe_list)}"),
        ("dimension", f"ou:{ou_dimension_value}"),
        ("displayProperty", "NAME"),
        ("skipMeta", "false"),
        ("paging", "false"),
    ]
    payload = client.get("analytics.json", params=params)
    return parse_analytics_payload(payload)


class DHIS2Client:
    def __init__(
        self,
        base_url,
        username,
        password,
        timeout_connect=10,
        timeout_read=90,
        retries=2,
    ):
        self.base_url = str(base_url).rstrip("/")
        self.timeout = (timeout_connect, timeout_read)
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update({"Accept": "application/json"})

        retry_strategy = Retry(
            total=retries,
            connect=retries,
            read=retries,
            status=retries,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "OPTIONS"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get(self, endpoint, params=None):
        cleaned_endpoint = str(endpoint).lstrip("/")
        if not cleaned_endpoint.startswith("api/"):
            cleaned_endpoint = f"api/{cleaned_endpoint}"
        url = f"{self.base_url}/{cleaned_endpoint}"
        response = self.session.get(url, params=params, timeout=self.timeout)
        if response.status_code in (401, 403):
            raise RuntimeError("Acces DHIS2 refuse (identifiants ou permissions).")
        response.raise_for_status()
        if not response.text:
            return {}
        return response.json()


def test_dhis2_credentials(base_url, username, password):
    resolved_base_url = resolve_dhis2_base_url(base_url, username=username, password=password)
    host = urlparse(str(resolved_base_url)).hostname or ""
    if host:
        try:
            socket.getaddrinfo(host, 443)
        except Exception:
            return (
                False,
                f"Nom d'hote introuvable: {host}. Verifie l'URL de la source DHIS2 (ou DNS/VPN).",
            )

    try:
        response = requests.get(
            f"{resolved_base_url.rstrip('/')}/api/me",
            auth=(username, password),
            timeout=10,
            allow_redirects=False,
        )
    except requests.exceptions.ConnectionError as exc:
        message = str(exc)
        if "NameResolutionError" in message or "getaddrinfo failed" in message:
            return (
                False,
                f"Nom d'hote introuvable: {host or resolved_base_url}. Verifie l'URL de la source DHIS2.",
            )
        return False, f"Erreur de connexion: {exc}"
    except Exception as exc:
        return False, f"Erreur de connexion: {exc}"

    if response.status_code == 200:
        return True, None
    if response.status_code in (401, 403):
        return False, "Identifiants incorrects ou acces non autorise."
    return False, f"Connexion DHIS2 en echec (HTTP {response.status_code})."


@st.cache_resource
def build_dhis2_client(base_url, username, password, timeout_connect, timeout_read, retries):
    return DHIS2Client(
        base_url=base_url,
        username=username,
        password=password,
        timeout_connect=timeout_connect,
        timeout_read=timeout_read,
        retries=retries,
    )


def get_client():
    configured_base_url = str(st.session_state.get("dhis2_base_url", "")).strip()
    if not configured_base_url:
        configured_base_url = read_config_value("DHIS2_URL")
    username = str(st.session_state.get("dhis2_user", "")).strip()
    password = str(st.session_state.get("dhis2_pass", "")).strip()
    if not configured_base_url or not username or not password:
        raise RuntimeError("Configuration DHIS2 incomplete.")

    base_url = resolve_dhis2_base_url(configured_base_url, username=username, password=password)

    timeout_connect = read_int_config_value("DHIS2_TIMEOUT_CONNECT", 10)
    timeout_read = read_int_config_value("DHIS2_TIMEOUT_READ", 90)
    retries = read_int_config_value("DHIS2_HTTP_RETRIES", 2)

    return build_dhis2_client(
        base_url,
        username,
        password,
        timeout_connect,
        timeout_read,
        retries,
    )


@st.cache_data(ttl=900)
def get_me(cache_identity):
    _ = cache_identity
    client = get_client()
    fields = (
        "id,username,displayName,"
        "userGroups[id,displayName],"
        "organisationUnits[id,displayName,level],"
        "dataViewOrganisationUnits[id,displayName,level],"
        "userCredentials[userRoles[id,displayName]]"
    )
    try:
        return client.get("me", params={"fields": fields})
    except Exception:
        fallback_fields = (
            "id,username,displayName,"
            "userGroups[id,displayName],"
            "organisationUnits[id,displayName,level],"
            "dataViewOrganisationUnits[id,displayName,level]"
        )
        return client.get("me", params={"fields": fallback_fields})


@st.cache_data(ttl=3600)
def get_indicator_catalog(cache_identity):
    _ = cache_identity
    client = get_client()
    response = client.get(
        "indicators",
        params={
            "fields": "id,displayName",
            "paging": "false",
        },
    )
    return response.get("indicators", [])


@st.cache_data(ttl=3600)
def get_org_units_hierarchy(cache_identity, org_unit_ids):
    _ = cache_identity
    clean_ids = []
    for ou_id in org_unit_ids or []:
        raw = str(ou_id).strip()
        if not raw:
            continue
        clean_ids.append(raw)
    unique_ids = sorted(set(clean_ids))
    if not unique_ids:
        return {}

    client = get_client()
    hierarchy = {}
    chunk_size = 60

    for start_idx in range(0, len(unique_ids), chunk_size):
        chunk = unique_ids[start_idx : start_idx + chunk_size]
        params = {
            "fields": "id,displayName,level,parent[id,displayName,level],ancestors[id,displayName,level]",
            "filter": f"id:in:[{','.join(chunk)}]",
            "paging": "false",
        }
        try:
            response = client.get("organisationUnits", params=params)
            units = response.get("organisationUnits", []) or []
        except Exception:
            units = []

        for ou in units:
            ou_id = str(ou.get("id", "")).strip()
            if not ou_id:
                continue

            ou_name = str(ou.get("displayName", "")).strip()
            try:
                ou_level = int(ou.get("level")) if ou.get("level") is not None else None
            except Exception:
                ou_level = None

            ancestors = [a for a in (ou.get("ancestors") or []) if isinstance(a, Mapping) and a.get("id")]
            ancestors_sorted = sorted(
                ancestors,
                key=lambda item: int(item.get("level") or 0),
            )

            chain = []
            seen = set()
            for node in ancestors_sorted + [{"id": ou_id, "displayName": ou_name, "level": ou_level}]:
                node_id = str(node.get("id", "")).strip()
                if not node_id or node_id in seen:
                    continue
                seen.add(node_id)
                chain.append(node)

            def pick_by_level(level):
                for node in chain:
                    try:
                        if int(node.get("level") or -1) == level:
                            return node
                    except Exception:
                        continue
                return None

            province_node = pick_by_level(2) or (chain[1] if len(chain) > 1 else (chain[0] if chain else None))
            zone_node = pick_by_level(3) or (chain[2] if len(chain) > 2 else province_node)
            aire_node = pick_by_level(4)
            if aire_node is None and ou_level is not None and ou_level >= 4:
                aire_node = {"id": ou_id, "displayName": ou_name, "level": ou_level}

            hierarchy[ou_id] = {
                "ou_name": ou_name,
                "ou_level": ou_level,
                "province_name": str((province_node or {}).get("displayName", "")).strip(),
                "province_id": str((province_node or {}).get("id", "")).strip(),
                "zone_name": str((zone_node or {}).get("displayName", "")).strip(),
                "zone_id": str((zone_node or {}).get("id", "")).strip(),
                "aire_name": str((aire_node or {}).get("displayName", "")).strip(),
                "aire_id": str((aire_node or {}).get("id", "")).strip(),
            }

    return hierarchy


@st.cache_data(ttl=3600)
def get_zone_health_areas(cache_identity, zone_ids):
    _ = cache_identity
    unique_ids = []
    for zone_id in sanitize_ids(zone_ids):
        if zone_id not in unique_ids:
            unique_ids.append(zone_id)
    if not unique_ids:
        return {}

    preferred_level = read_int_config_value("DHIS2_AIRE_LEVEL", 4)
    client = get_client()
    result = {}

    for zone_id in unique_ids:
        try:
            payload = client.get(
                f"organisationUnits/{zone_id}",
                params={"fields": "id,displayName,children[id,displayName,level]"},
            )
            children = payload.get("children", []) or []
        except Exception:
            children = []

        parsed_children = []
        for child in children:
            child_id = str(child.get("id", "")).strip()
            child_name = str(child.get("displayName", "")).strip()
            if not child_id or not child_name:
                continue
            try:
                child_level = int(child.get("level")) if child.get("level") is not None else None
            except Exception:
                child_level = None
            parsed_children.append({"id": child_id, "name": child_name, "level": child_level})

        preferred = [c for c in parsed_children if c.get("level") == preferred_level]
        selected_children = preferred or parsed_children
        selected_children.sort(key=lambda item: normalize_text(item.get("name", "")))
        result[zone_id] = selected_children

    return result


def resolve_allowed_sgis(me_data, sgi_group_mapping):
    user_groups = me_data.get("userGroups", []) or []
    group_ids = {str(g.get("id", "")).strip() for g in user_groups if g.get("id")}
    group_names_norm = {
        normalize_text(g.get("displayName", ""))
        for g in user_groups
        if g.get("displayName")
    }

    allowed = []
    for sgi in SGI_LIST:
        explicit_rules = dict_get_ci(sgi_group_mapping, sgi)
        if explicit_rules is not None:
            matched = False
            for token in as_list(explicit_rules):
                raw_token = str(token).strip()
                if not raw_token:
                    continue
                token_norm = normalize_text(raw_token)
                if raw_token in group_ids or token_norm in group_names_norm:
                    matched = True
                    break
            if matched:
                allowed.append(sgi)
            continue

        sgi_norm = normalize_text(sgi)
        if any(sgi_norm in group_name for group_name in group_names_norm):
            allowed.append(sgi)

    if allowed:
        return allowed

    allow_all = is_truthy(
        read_config_value("DHIS2_ALLOW_ALL_IF_NO_GROUP_MATCH", "true"),
        default=True,
    )
    return SGI_LIST.copy() if allow_all else []


def get_user_org_units(me_data):
    data_view_units = me_data.get("dataViewOrganisationUnits") or me_data.get("dataViewOrganizationUnits") or []
    if data_view_units:
        return data_view_units
    return me_data.get("organisationUnits", []) or []


def get_user_levels(org_units):
    levels = []
    for ou in org_units:
        level = ou.get("level")
        if level is None:
            continue
        try:
            levels.append(int(level))
        except Exception:
            continue
    return levels


def parse_metric_entries(raw_entries):
    entries = []
    if raw_entries is None:
        return entries

    source_list = raw_entries if isinstance(raw_entries, list) else [raw_entries]
    for item in source_list:
        metric_id = ""
        metric_label = ""

        if isinstance(item, Mapping):
            metric_id = str(
                item.get("id") or item.get("uid") or item.get("dx") or ""
            ).strip()
            metric_label = str(
                item.get("label") or item.get("displayName") or item.get("name") or metric_id
            ).strip()
        else:
            text = str(item).strip()
            if not text:
                continue
            if "|" in text:
                metric_id, metric_label = [part.strip() for part in text.split("|", 1)]
            else:
                metric_id = text
                metric_label = text

        if not metric_id:
            continue
        entries.append({"id": metric_id, "label": metric_label, "source": "config"})

    seen = set()
    deduped = []
    for entry in entries:
        metric_id = entry["id"]
        if metric_id in seen:
            continue
        seen.add(metric_id)
        deduped.append(entry)
    return deduped


def get_metrics_from_config(metrics_config, sgi, section_key):
    if not isinstance(metrics_config, Mapping):
        return []
    sgi_cfg = dict_get_ci(metrics_config, sgi)
    if not isinstance(sgi_cfg, Mapping):
        return []

    aliases = SECTION_ALIASES.get(section_key, [section_key])
    section_cfg = None
    for alias in aliases:
        section_cfg = dict_get_ci(sgi_cfg, alias)
        if section_cfg is not None:
            break
    return parse_metric_entries(section_cfg)


def discover_metrics(catalog, sgi, section_key, limit=8):
    if not catalog:
        return []

    sgi_norm = normalize_text(sgi)
    keywords = [normalize_text(x) for x in SECTION_KEYWORDS.get(section_key, [])]
    scored = []

    for metric in catalog:
        metric_id = str(metric.get("id", "")).strip()
        metric_name = str(metric.get("displayName", "")).strip()
        if not metric_id or not metric_name:
            continue

        metric_name_norm = normalize_text(metric_name)
        if sgi_norm not in metric_name_norm:
            continue

        score = sum(1 for kw in keywords if kw and kw in metric_name_norm)
        if section_key != "vue_ensemble" and score == 0:
            continue
        scored.append((score, metric_name_norm, metric_id, metric_name))

    if not scored:
        for metric in catalog:
            metric_id = str(metric.get("id", "")).strip()
            metric_name = str(metric.get("displayName", "")).strip()
            if not metric_id or not metric_name:
                continue
            metric_name_norm = normalize_text(metric_name)
            if sgi_norm in metric_name_norm:
                scored.append((0, metric_name_norm, metric_id, metric_name))

    scored.sort(key=lambda item: (-item[0], item[1]))
    result = []
    seen = set()
    for _, _, metric_id, metric_name in scored:
        if metric_id in seen:
            continue
        seen.add(metric_id)
        result.append({"id": metric_id, "label": metric_name, "source": "auto"})
        if len(result) >= limit:
            break
    return result


def get_metrics_for_section(metrics_config, indicator_catalog, sgi, section_key):
    configured = get_metrics_from_config(metrics_config, sgi, section_key)
    if configured:
        return configured
    return discover_metrics(indicator_catalog, sgi, section_key, limit=8)


def discover_auxiliary_metrics(indicator_catalog, sgi, limit=80):
    if not indicator_catalog:
        return []

    sgi_norm = normalize_text(sgi)
    keywords = [
        "age",
        "ans",
        "mois",
        "positif",
        "negative",
        "negatif",
        "test",
        "echantillon",
        "suspect",
        "confirme",
        "deces",
        "letalite",
        "probable",
        "semaine",
        "epi",
    ]

    scored = []
    for metric in indicator_catalog:
        metric_id = str(metric.get("id", "")).strip()
        metric_name = str(metric.get("displayName", "")).strip()
        if not metric_id or not metric_name:
            continue
        name_norm = normalize_text(metric_name)
        if sgi_norm not in name_norm:
            continue
        score = sum(1 for kw in keywords if kw in name_norm)
        if score <= 0:
            continue
        scored.append((score, name_norm, metric_id, metric_name))

    if not scored:
        return []

    scored.sort(key=lambda item: (-item[0], item[1]))
    result = []
    seen = set()
    for _, _, metric_id, metric_name in scored:
        if metric_id in seen:
            continue
        seen.add(metric_id)
        result.append({"id": metric_id, "label": metric_name, "source": "aux"})
        if len(result) >= limit:
            break
    return result


def parse_analytics_payload(payload):
    headers = [h.get("name", h.get("column", "")) for h in payload.get("headers", [])]
    header_index = {name: idx for idx, name in enumerate(headers)}
    required = {"dx", "pe", "ou", "value"}
    if not required.issubset(header_index.keys()):
        return pd.DataFrame()

    metadata_items = payload.get("metaData", {}).get("items", {}) or {}
    rows = []

    for row in payload.get("rows", []):
        dx = str(row[header_index["dx"]]).strip()
        pe = str(row[header_index["pe"]]).strip()
        ou = str(row[header_index["ou"]]).strip()
        raw_value = str(row[header_index["value"]]).replace(",", ".").strip()
        value = pd.to_numeric(raw_value, errors="coerce")
        if pd.isna(value):
            continue

        rows.append(
            {
                "dx": dx,
                "dx_name": metadata_items.get(dx, {}).get("name", dx),
                "pe": pe,
                "pe_name": metadata_items.get(pe, {}).get("name", format_period_label(pe)),
                "ou": ou,
                "ou_name": metadata_items.get(ou, {}).get("name", ou),
                "value": float(value),
            }
        )

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


@st.cache_data(ttl=900)
def fetch_analytics(cache_identity, dx_ids, period_ids, ou_dimension, ou_explicit_ids=()):
    _ = cache_identity
    if not dx_ids or not period_ids:
        return pd.DataFrame()

    client = get_client()
    all_frames = []
    chunk_size = 20

    dx_list = list(dx_ids)
    pe_list = list(period_ids)
    ou_dimensions = build_analytics_ou_dimensions(ou_dimension, explicit_ou_ids=ou_explicit_ids)
    error_messages = []

    for idx in range(0, len(dx_list), chunk_size):
        chunk = dx_list[idx : idx + chunk_size]
        chunk_loaded = False

        for ou_value in ou_dimensions:
            try:
                frame = query_analytics_chunk(client, chunk, pe_list, ou_value)
                if not frame.empty:
                    all_frames.append(frame)
                chunk_loaded = True
                break
            except requests.HTTPError as exc:
                if is_http_409(exc):
                    error_messages.append(
                        f"409 sur chunk ({len(chunk)} indicateurs) avec OU {ou_value}: {extract_http_error_message(exc)}"
                    )
                    continue
                raise RuntimeError(f"Erreur analytics DHIS2: {extract_http_error_message(exc)}") from exc
            except Exception as exc:
                raise RuntimeError(f"Erreur analytics DHIS2: {exc}") from exc

        if chunk_loaded:
            continue

        # Fallback de secours: isoler les indicateurs qui provoquent 409.
        for dx_id in chunk:
            for ou_value in ou_dimensions:
                try:
                    frame = query_analytics_chunk(client, [dx_id], pe_list, ou_value)
                    if not frame.empty:
                        all_frames.append(frame)
                    break
                except requests.HTTPError as exc:
                    if is_http_409(exc):
                        continue
                    raise RuntimeError(f"Erreur analytics DHIS2: {extract_http_error_message(exc)}") from exc
                except Exception as exc:
                    raise RuntimeError(f"Erreur analytics DHIS2: {exc}") from exc

    if not all_frames:
        if error_messages:
            return pd.DataFrame()
        return pd.DataFrame()

    return pd.concat(all_frames, ignore_index=True)


def sort_period_frame(frame):
    if frame.empty or "pe" not in frame.columns:
        return frame

    out = frame.copy()
    out["__period_sort"] = pd.to_numeric(out["pe"], errors="coerce")
    out = out.sort_values(
        by=["__period_sort", "pe"],
        ascending=[True, True],
        na_position="last",
    )
    return out.drop(columns="__period_sort")


def format_number(value):
    try:
        num = float(value)
    except Exception:
        return str(value)
    if abs(num) >= 100:
        return f"{num:,.0f}".replace(",", " ")
    return f"{num:,.1f}".replace(",", " ")


def format_delta(value):
    try:
        num = float(value)
    except Exception:
        return None
    sign = "+" if num >= 0 else ""
    return f"{sign}{format_number(num)}"


def build_metric_totals(df):
    if df.empty:
        return pd.DataFrame(columns=["dx", "dx_name", "value"])
    totals = (
        df.groupby(["dx", "dx_name"], as_index=False)["value"]
        .sum()
        .sort_values("value", ascending=False)
    )
    return totals


def metric_delta(df, dx_id):
    metric_df = df[df["dx"] == dx_id]
    if metric_df.empty:
        return None
    trend = metric_df.groupby("pe", as_index=False)["value"].sum()
    trend = sort_period_frame(trend)
    if len(trend) < 2:
        return None
    return float(trend.iloc[-1]["value"] - trend.iloc[-2]["value"])


def render_data_table(df):
    if df.empty:
        st.info("Aucune table detaillee a afficher.")
        return

    table = df.pivot_table(
        index="ou_name",
        columns="dx_name",
        values="value",
        aggfunc="sum",
        fill_value=0.0,
    )
    if table.empty:
        st.info("La table est vide apres agregation.")
        return

    table["Total"] = table.sum(axis=1)
    table = table.sort_values("Total", ascending=False)
    if len(table) > 200:
        table = table.head(200)
    st.dataframe(table, use_container_width=True)

def choose_icon(label):
    name = normalize_text(label)
    for keyword, icon_html in ICON_BY_TOPIC.items():
        if keyword in name:
            return icon_html
    return "&#128202;"


def find_metric_row(totals, keywords, used_ids=None):
    used_ids = used_ids or set()
    for row in totals.itertuples(index=False):
        if row.dx in used_ids:
            continue
        name_norm = normalize_text(row.dx_name)
        if any(keyword in name_norm for keyword in keywords):
            return {"dx": row.dx, "dx_name": row.dx_name, "value": float(row.value)}
    return None


def select_metric_context(df):
    totals = build_metric_totals(df)
    if totals.empty:
        return {"totals": totals, "cases": None, "deaths": None, "lethality": None}

    used = set()
    cases = find_metric_row(totals, ["cas", "suspect", "confirm"], used)
    if cases:
        used.add(cases["dx"])

    deaths = find_metric_row(totals, ["deces", "mort", "death"], used)
    if deaths:
        used.add(deaths["dx"])

    lethality = find_metric_row(totals, ["letalit", "fatalit", "cfr"], used)
    if lethality:
        used.add(lethality["dx"])

    top_extra = []
    for row in totals.itertuples(index=False):
        if row.dx in used:
            continue
        top_extra.append({"dx": row.dx, "dx_name": row.dx_name, "value": float(row.value)})
        if len(top_extra) >= 4:
            break

    return {
        "totals": totals,
        "cases": cases,
        "deaths": deaths,
        "lethality": lethality,
        "extra": top_extra,
    }


def get_latest_period_df(df):
    if df.empty:
        return df
    sorted_frame = sort_period_frame(df[["pe", "pe_name"]].drop_duplicates())
    latest_pe = str(sorted_frame.iloc[-1]["pe"])
    return df[df["pe"].astype(str) == latest_pe].copy()


def metric_value(df, metric_id):
    if not metric_id:
        return 0.0
    metric_df = df[df["dx"] == metric_id]
    return float(metric_df["value"].sum()) if not metric_df.empty else 0.0


def count_covered_provinces(df):
    if df.empty:
        return 0
    found = set()
    for ou_name in df["ou_name"].dropna().astype(str).unique():
        norm_name = normalize_text(ou_name)
        for key, (label, _, _) in PROVINCE_COORDS.items():
            if key in norm_name:
                found.add(label)
                break
    return len(found)


def build_general_items(df, context):
    cases = metric_value(df, context["cases"]["dx"] if context["cases"] else None)
    deaths = metric_value(df, context["deaths"]["dx"] if context["deaths"] else None)

    if context["lethality"]:
        lethality_value = metric_value(df, context["lethality"]["dx"])
        lethality_text = f"{lethality_value:.2f}%"
    else:
        lethality_value = (deaths / cases * 100.0) if cases > 0 else 0.0
        lethality_text = f"{lethality_value:.2f}%"

    items = [
        {"label": "Cas suspects", "value": format_number(cases), "icon": choose_icon("cas")},
        {"label": "Deces", "value": format_number(deaths), "icon": choose_icon("deces")},
        {"label": "Letalite", "value": lethality_text, "icon": choose_icon("letalite")},
        {"label": "DPS touchees", "value": str(count_covered_provinces(df)), "icon": choose_icon("dps")},
        {"label": "ZS touchees", "value": str(df["ou_name"].nunique()), "icon": choose_icon("zone")},
    ]

    for metric in context.get("extra", []):
        if len(items) >= 6:
            break
        items.append(
            {
                "label": metric["dx_name"],
                "value": format_number(metric["value"]),
                "icon": choose_icon(metric["dx_name"]),
            }
        )

    return items[:6]


def build_weekly_items(df_latest, context):
    cases = metric_value(df_latest, context["cases"]["dx"] if context["cases"] else None)
    deaths = metric_value(df_latest, context["deaths"]["dx"] if context["deaths"] else None)

    if context["lethality"]:
        lethality_value = metric_value(df_latest, context["lethality"]["dx"])
        lethality_text = f"{lethality_value:.2f}%"
    else:
        lethality_value = (deaths / cases * 100.0) if cases > 0 else 0.0
        lethality_text = f"{lethality_value:.2f}%"

    return [
        {"label": "Cas suspects", "value": format_number(cases), "icon": choose_icon("cas")},
        {"label": "Deces", "value": format_number(deaths), "icon": choose_icon("deces")},
        {"label": "Letalite", "value": lethality_text, "icon": choose_icon("letalite")},
        {"label": "ZS touchees", "value": str(df_latest["ou_name"].nunique()), "icon": choose_icon("zone")},
    ]


def enrich_with_hierarchy(df, hierarchy_by_ou):
    if df.empty:
        return df
    out = df.copy()
    out["OU Level"] = out["ou"].map(lambda x: hierarchy_by_ou.get(str(x), {}).get("ou_level"))
    out["Province ID"] = out["ou"].map(lambda x: hierarchy_by_ou.get(str(x), {}).get("province_id", "")).replace("", pd.NA)
    out["Zone ID"] = out["ou"].map(lambda x: hierarchy_by_ou.get(str(x), {}).get("zone_id", "")).replace("", pd.NA)
    out["Aire ID"] = out["ou"].map(lambda x: hierarchy_by_ou.get(str(x), {}).get("aire_id", "")).replace("", pd.NA)
    out["Province"] = out["ou"].map(lambda x: hierarchy_by_ou.get(str(x), {}).get("province_name", "")).replace("", pd.NA)
    out["Zone de Sante"] = out["ou"].map(lambda x: hierarchy_by_ou.get(str(x), {}).get("zone_name", "")).replace("", pd.NA)
    out["Aire de Sante"] = out["ou"].map(lambda x: hierarchy_by_ou.get(str(x), {}).get("aire_name", "")).replace("", pd.NA)

    out["Province"] = out["Province"].fillna(out["ou_name"])
    out["Zone de Sante"] = out["Zone de Sante"].fillna(out["ou_name"])
    out["Province ID"] = out["Province ID"].fillna(out["ou"])
    out["Zone ID"] = out["Zone ID"].fillna(out["ou"])
    out["Aire ID"] = out["Aire ID"].fillna("")
    out["Aire de Sante"] = out["Aire de Sante"].fillna("")
    return out


def keep_granular_org_level(df):
    if df.empty or "OU Level" not in df.columns:
        return df

    work = df.copy()
    work["__ou_level_num"] = pd.to_numeric(work["OU Level"], errors="coerce")
    if work["__ou_level_num"].dropna().empty:
        return df

    keep_index = set()
    grouped = work.dropna(subset=["__ou_level_num"]).groupby("dx")
    for _dx, frame in grouped:
        levels = sorted({int(v) for v in frame["__ou_level_num"].tolist()})
        deep_levels = [level for level in levels if level >= 4]
        target_level = deep_levels[0] if deep_levels else max(levels)
        keep_index.update(frame[frame["__ou_level_num"] == target_level].index.tolist())

    filtered = work.loc[sorted(keep_index)].drop(columns="__ou_level_num")
    return filtered if not filtered.empty else df


def render_org_filters(df, sgi, widget_key, source_label, cache_identity):
    if df.empty:
        return df, {}

    period_df = sort_period_frame(df[["pe", "pe_name"]].drop_duplicates())
    period_options = period_df["pe"].astype(str).tolist()

    default_range = None
    if len(period_options) >= 2:
        default_range = (period_options[0], period_options[-1])

    col1, col2, col3, col4, col5 = st.columns([1.0, 1.0, 1.0, 1.0, 1.4], gap="small")
    with col1:
        st.markdown("<div class='bsq-filter-label'>Maladie</div>", unsafe_allow_html=True)
        _ = st.selectbox(
            "Maladie",
            [sgi],
            index=0,
            key=f"{widget_key}_maladie",
            label_visibility="collapsed",
        )

    province_options = sorted([str(v) for v in df["Province"].dropna().astype(str).unique().tolist()])
    with col2:
        st.markdown("<div class='bsq-filter-label'>Province</div>", unsafe_allow_html=True)
        selected_province = st.selectbox(
            "Province",
            ["(Tout)"] + province_options,
            key=f"{widget_key}_province",
            label_visibility="collapsed",
        )

    province_df = df if selected_province == "(Tout)" else df[df["Province"] == selected_province]
    zone_options = sorted([str(v) for v in province_df["Zone de Sante"].dropna().astype(str).unique().tolist()])
    with col3:
        st.markdown("<div class='bsq-filter-label'>Zone de Sante</div>", unsafe_allow_html=True)
        selected_zone = st.selectbox(
            "Zone de Sante",
            ["(Tout)"] + zone_options,
            key=f"{widget_key}_zone",
            label_visibility="collapsed",
        )

    zone_df = province_df if selected_zone == "(Tout)" else province_df[province_df["Zone de Sante"] == selected_zone]
    aire_df = zone_df.copy()

    aire_name_to_ids = {}
    if "Aire de Sante" in aire_df.columns:
        for aire_name_raw, aire_id_raw in aire_df[["Aire de Sante", "Aire ID"]].fillna("").values.tolist():
            aire_name = str(aire_name_raw).strip()
            aire_id = str(aire_id_raw).strip()
            if not aire_name:
                continue
            aire_name_to_ids.setdefault(aire_name, set())
            if aire_id:
                aire_name_to_ids[aire_name].add(aire_id)

    aire_options = sorted([name for name in aire_name_to_ids.keys() if name.strip() != ""])
    used_metadata_aires = False
    selected_zone_ids = []
    if selected_zone != "(Tout)" and "Zone ID" in province_df.columns:
        selected_zone_ids = sorted(
            set(
                sanitize_ids(
                    province_df.loc[province_df["Zone de Sante"] == selected_zone, "Zone ID"].astype(str).tolist()
                )
            )
        )

    if selected_zone != "(Tout)" and not aire_options and selected_zone_ids:
        metadata_map = get_zone_health_areas(
            cache_identity=f"{cache_identity}|{selected_zone}",
            zone_ids=tuple(selected_zone_ids),
        )
        for _zone_id, children in metadata_map.items():
            for child in children:
                aire_name = str(child.get("name", "")).strip()
                aire_id = str(child.get("id", "")).strip()
                if not aire_name:
                    continue
                aire_name_to_ids.setdefault(aire_name, set())
                if aire_id:
                    aire_name_to_ids[aire_name].add(aire_id)
        aire_options = sorted([name for name in aire_name_to_ids.keys() if name.strip() != ""])
        used_metadata_aires = bool(aire_options)

    with col4:
        st.markdown("<div class='bsq-filter-label'>Aire de Sante</div>", unsafe_allow_html=True)
        selected_aire = st.selectbox(
            "Aire de Sante",
            ["(Tout)"] + aire_options,
            key=f"{widget_key}_aire",
            label_visibility="collapsed",
        )
        if used_metadata_aires:
            st.caption("Aires chargees depuis la hierarchie DHIS2.")

    with col5:
        st.markdown("<div class='bsq-filter-label'>Date de collecte</div>", unsafe_allow_html=True)
        if default_range:
            selected_range = st.select_slider(
                "Date de collecte",
                options=period_options,
                value=default_range,
                key=f"{widget_key}_period",
                format_func=format_period_label,
                label_visibility="collapsed",
            )
        elif period_options:
            selected_range = (period_options[0], period_options[0])
            st.caption(format_period_label(period_options[0]))
        else:
            selected_range = None
            st.caption("N/A")
        today_stamp = date.today().strftime("%Y-%m-%d")
        st.markdown(
            (
                "<div class='bsq-source'>"
                f"Source des donnees: {source_label}<br>"
                f"Derniere mise a jour: {today_stamp}"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

    filtered = df.copy()
    if selected_province != "(Tout)":
        filtered = filtered[filtered["Province"] == selected_province]
    if selected_zone != "(Tout)":
        filtered = filtered[filtered["Zone de Sante"] == selected_zone]

    selected_aire_ids = []
    if selected_aire != "(Tout)":
        selected_aire_ids = sorted(aire_name_to_ids.get(selected_aire, set()))
        by_name = filtered[filtered["Aire de Sante"] == selected_aire]
        if by_name.empty and selected_aire_ids and "Aire ID" in filtered.columns:
            by_name = filtered[filtered["Aire ID"].astype(str).isin(selected_aire_ids)]
        filtered = by_name

    selected_start = None
    selected_end = None
    if selected_range and period_options:
        start_code, end_code = selected_range
        start_idx = period_options.index(start_code)
        end_idx = period_options.index(end_code)
        if start_idx > end_idx:
            start_idx, end_idx = end_idx, start_idx
        valid_periods = set(period_options[start_idx : end_idx + 1])
        filtered = filtered[filtered["pe"].astype(str).isin(valid_periods)]
        selected_start = start_code
        selected_end = end_code

    selection = {
        "province": selected_province,
        "zone": selected_zone,
        "aire": selected_aire,
        "zone_ids": selected_zone_ids,
        "aire_ids": selected_aire_ids,
        "used_metadata_aires": used_metadata_aires,
        "period_start": selected_start,
        "period_end": selected_end,
    }
    return filtered, selection


def apply_selected_filters(df, selection):
    if df.empty:
        return df
    out = df.copy()
    province = selection.get("province", "(Tout)")
    zone = selection.get("zone", "(Tout)")
    aire = selection.get("aire", "(Tout)")
    zone_ids = sanitize_ids(selection.get("zone_ids", []))
    aire_ids = sanitize_ids(selection.get("aire_ids", []))
    pstart = selection.get("period_start")
    pend = selection.get("period_end")

    if province != "(Tout)":
        out = out[out["Province"] == province]
    if zone != "(Tout)":
        by_zone = out[out["Zone de Sante"] == zone]
        if by_zone.empty and zone_ids and "Zone ID" in out.columns:
            by_zone = out[out["Zone ID"].astype(str).isin(zone_ids)]
        out = by_zone
    if aire != "(Tout)":
        by_aire = out[out["Aire de Sante"] == aire]
        if by_aire.empty and aire_ids and "Aire ID" in out.columns:
            by_aire = out[out["Aire ID"].astype(str).isin(aire_ids)]
        out = by_aire

    if pstart and pend:
        try:
            start_int = int(str(pstart))
            end_int = int(str(pend))
            if start_int > end_int:
                start_int, end_int = end_int, start_int
            pe_int = pd.to_numeric(out["pe"], errors="coerce")
            out = out[(pe_int >= start_int) & (pe_int <= end_int)]
        except Exception:
            pass
    return out


def detect_age_group(metric_name):
    name = normalize_text(metric_name)
    compact_name = re.sub(r"[^a-z0-9]+", " ", name).strip()
    for label, patterns in AGE_GROUP_PATTERNS:
        if any(pattern in name or pattern in compact_name for pattern in patterns):
            return label

    range_match = re.search(r"(\d{1,2})\s*[-a]\s*(\d{1,2})", compact_name)
    if range_match:
        start = range_match.group(1)
        end = range_match.group(2)
        return f"{start} - {end} Ans"
    return None


def detect_metric_family(metric_name):
    name = normalize_text(metric_name)
    if "letalit" in name or "fatalit" in name or "cfr" in name:
        return "letalite"
    if "deces" in name or "mort" in name or "death" in name:
        return "deces"
    if "confir" in name:
        return "confirmes"
    if "suspect" in name:
        return "suspects"
    if "positif" in name or "positive" in name:
        return "positif"
    if "negatif" in name or "negative" in name:
        return "negatif"
    if "test" in name or "echantillon" in name or "analyse" in name:
        return "tests"
    return None


def build_age_family_df(df):
    if df.empty:
        return pd.DataFrame(columns=["age_group", "family", "value"])
    records = []
    for row in df.itertuples(index=False):
        age_group = detect_age_group(row.dx_name)
        if not age_group:
            continue
        family = detect_metric_family(row.dx_name)
        if not family:
            continue
        records.append(
            {
                "age_group": age_group,
                "family": family,
                "value": float(row.value),
            }
        )
    if not records:
        return pd.DataFrame(columns=["age_group", "family", "value"])
    out = pd.DataFrame(records)
    return out.groupby(["age_group", "family"], as_index=False)["value"].sum()


def extract_family_totals(df):
    totals = {}
    for row in build_metric_totals(df).itertuples(index=False):
        family = detect_metric_family(row.dx_name)
        if not family:
            continue
        totals[family] = totals.get(family, 0.0) + float(row.value)
    return totals


def compute_global_positivity(df):
    totals = extract_family_totals(df)
    positives = float(totals.get("positif", 0.0))
    negatives = float(totals.get("negatif", 0.0))
    tests = float(totals.get("tests", 0.0))
    denominator = positives + negatives
    if denominator <= 0 and tests > 0:
        denominator = tests
    if denominator <= 0:
        return None, positives, negatives
    return positives / denominator * 100.0, positives, max(denominator - positives, negatives)


def choose_proxy_org_column(df):
    candidates = [
        ("Aire de Sante", "aire de sante"),
        ("Zone de Sante", "zone de sante"),
        ("Province", "province"),
        ("ou_name", "unite"),
    ]

    for column_name, label in candidates:
        if column_name not in df.columns:
            continue
        values = df[column_name].fillna("").astype(str).str.strip()
        non_empty = values[values != ""]
        if non_empty.nunique() > 1:
            return column_name, label

    for column_name, label in candidates:
        if column_name in df.columns:
            return column_name, label

    return None, ""


def sort_age_group_frame(frame, column="age_group"):
    if frame.empty or column not in frame.columns:
        return frame
    order_map = {name: idx for idx, name in enumerate(AGE_GROUP_ORDER)}
    out = frame.copy()
    out["__age_sort"] = out[column].map(lambda value: order_map.get(str(value), 999))
    out = out.sort_values(by=["__age_sort", column], ascending=[True, True])
    return out.drop(columns="__age_sort")


def apply_chart_style(fig, rotate_x=False, legend_y=-0.16, hovermode="x unified"):
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#0f1a2b",
        font=dict(color="#c9d5e4", size=12, family="Nunito Sans, Segoe UI, sans-serif"),
        title=dict(x=0.01, xanchor="left", font=dict(size=18, color="#f4b183")),
        legend=dict(
            orientation="h",
            y=legend_y,
            x=0.0,
            xanchor="left",
            font=dict(size=11),
        ),
        hoverlabel=dict(bgcolor="#16243a", font=dict(color="#e4ecf5", size=11)),
        hovermode=hovermode,
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="#22354b",
        linecolor="#2f4863",
        tickangle=-55 if rotate_x else 0,
        automargin=True,
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="#22354b",
        linecolor="#2f4863",
        automargin=True,
    )


def add_bar_value_labels(fig, orientation="v", fmt=",.0f"):
    if orientation == "h":
        fig.update_traces(
            selector=dict(type="bar"),
            texttemplate=f"%{{x:{fmt}}}",
            textposition="outside",
            cliponaxis=False,
            textfont=dict(color="#d8e4f2", size=11),
        )
    else:
        fig.update_traces(
            selector=dict(type="bar"),
            texttemplate=f"%{{y:{fmt}}}",
            textposition="outside",
            cliponaxis=False,
            textfont=dict(color="#d8e4f2", size=11),
        )


def render_age_proportions(df, widget_key):
    age_df = build_age_family_df(df)
    if age_df.empty:
        # Fallback proxy when no age-coded metrics are available.
        proxy_col, proxy_label = choose_proxy_org_column(df)
        if not proxy_col:
            st.info("Aucune metrique par tranche d'age detectee.")
            return

        left_fb, right_fb = st.columns(2, gap="medium")
        with left_fb:
            proxy = (
                df.groupby(proxy_col, as_index=False)["value"]
                .sum()
                .sort_values("value", ascending=False)
                .head(6)
            )
            if proxy.empty:
                st.info("Aucune metrique par tranche d'age detectee.")
            else:
                fig = px.pie(
                    proxy,
                    names=proxy_col,
                    values="value",
                    hole=0.58,
                    color_discrete_sequence=PIE_COLORS_MAIN,
                )
                fig.update_layout(
                    title=f"Proportion de cas suspects (proxy par {proxy_label})",
                    margin=dict(l=0, r=0, t=42, b=0),
                )
                fig.update_traces(
                    textposition="outside",
                    texttemplate="%{value:,.0f} (%{percent})",
                    marker=dict(line=dict(color="#ffffff", width=1)),
                )
                apply_chart_style(fig, legend_y=-0.2, hovermode=False)
                st.plotly_chart(fig, use_container_width=True, key=f"{widget_key}_age_suspect_proxy")
        with right_fb:
            death_proxy = df[df["dx_name"].map(detect_metric_family) == "deces"]
            if death_proxy.empty:
                death_proxy = df.copy()
            proxy2 = (
                death_proxy.groupby(proxy_col, as_index=False)["value"]
                .sum()
                .sort_values("value", ascending=False)
                .head(6)
            )
            if proxy2.empty:
                st.info("Proportion des deces indisponible.")
            else:
                fig2 = px.pie(
                    proxy2,
                    names=proxy_col,
                    values="value",
                    hole=0.58,
                    color_discrete_sequence=PIE_COLORS_ALT,
                )
                fig2.update_layout(
                    title=f"Proportion de deces (proxy par {proxy_label})",
                    margin=dict(l=0, r=0, t=42, b=0),
                )
                fig2.update_traces(
                    textposition="outside",
                    texttemplate="%{value:,.0f} (%{percent})",
                    marker=dict(line=dict(color="#ffffff", width=1)),
                )
                apply_chart_style(fig2, legend_y=-0.2, hovermode=False)
                st.plotly_chart(fig2, use_container_width=True, key=f"{widget_key}_age_death_proxy")
        return

    left, right = st.columns(2, gap="medium")

    with left:
        suspects = sort_age_group_frame(age_df[age_df["family"] == "suspects"][["age_group", "value"]])
        if suspects.empty:
            st.info("Proportion des cas suspects indisponible.")
        else:
            fig = px.pie(
                suspects,
                names="age_group",
                values="value",
                hole=0.58,
                color_discrete_sequence=PIE_COLORS_MAIN,
            )
            fig.update_layout(title="Proportion de cas suspects par tranches d'ages", margin=dict(l=0, r=0, t=42, b=0))
            fig.update_traces(
                textposition="outside",
                texttemplate="%{value:,.0f} (%{percent})",
                marker=dict(line=dict(color="#ffffff", width=1)),
            )
            apply_chart_style(fig, legend_y=-0.2, hovermode=False)
            st.plotly_chart(fig, use_container_width=True, key=f"{widget_key}_age_suspect_pie")

    with right:
        deaths = sort_age_group_frame(age_df[age_df["family"] == "deces"][["age_group", "value"]])
        if deaths.empty:
            st.info("Proportion des deces indisponible.")
        else:
            fig = px.pie(
                deaths,
                names="age_group",
                values="value",
                hole=0.58,
                color_discrete_sequence=PIE_COLORS_ALT,
            )
            fig.update_layout(title="Proportion de deces par tranches d'ages", margin=dict(l=0, r=0, t=42, b=0))
            fig.update_traces(
                textposition="outside",
                texttemplate="%{value:,.0f} (%{percent})",
                marker=dict(line=dict(color="#ffffff", width=1)),
            )
            apply_chart_style(fig, legend_y=-0.2, hovermode=False)
            st.plotly_chart(fig, use_container_width=True, key=f"{widget_key}_age_death_pie")


def render_weekly_epi_situation(df, context, widget_key):
    case_metric = context["cases"]["dx"] if context.get("cases") else None
    death_metric = context["deaths"]["dx"] if context.get("deaths") else None
    leth_metric = context["lethality"]["dx"] if context.get("lethality") else None
    if not case_metric:
        st.info("Situation hebdomadaire indisponible.")
        return

    cases = df[df["dx"] == case_metric].groupby(["pe", "pe_name"], as_index=False)["value"].sum().rename(columns={"value": "cas"})
    cases = sort_period_frame(cases)
    if cases.empty:
        st.info("Situation hebdomadaire indisponible.")
        return

    if death_metric:
        deaths = df[df["dx"] == death_metric].groupby(["pe", "pe_name"], as_index=False)["value"].sum().rename(columns={"value": "deces"})
        deaths = sort_period_frame(deaths)
    else:
        deaths = pd.DataFrame(columns=["pe", "deces"])

    weekly = cases.merge(deaths[["pe", "deces"]] if not deaths.empty else pd.DataFrame(columns=["pe", "deces"]), on="pe", how="left")
    weekly["deces"] = weekly["deces"].fillna(0.0)

    if leth_metric:
        leth = df[df["dx"] == leth_metric].groupby(["pe", "pe_name"], as_index=False)["value"].sum().rename(columns={"value": "letalite"})
        leth = sort_period_frame(leth)
        weekly = weekly.merge(leth[["pe", "letalite"]], on="pe", how="left")
    if "letalite" not in weekly.columns:
        weekly["letalite"] = weekly.apply(
            lambda row: (row["deces"] / row["cas"] * 100.0) if row["cas"] else 0.0,
            axis=1,
        )
    weekly["letalite"] = weekly["letalite"].fillna(0.0)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(
            x=weekly["pe_name"],
            y=weekly["cas"],
            name="Cas suspects",
            marker_color=CHART_COLORS["cases"],
            opacity=0.9,
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=weekly["pe_name"],
            y=weekly["letalite"],
            name="Letalite",
            mode="lines+markers",
            line=dict(color=CHART_COLORS["lethality"], width=2.2),
            marker=dict(size=6),
        ),
        secondary_y=True,
    )
    fig.update_layout(
        title="Situation hebdomadaire de l'epidemie par semaine epidemiologique",
        margin=dict(l=10, r=10, t=45, b=8),
    )
    fig.update_yaxes(title_text="Cas", secondary_y=False)
    fig.update_yaxes(title_text="Letalite (%)", secondary_y=True)
    add_bar_value_labels(fig, orientation="v")
    apply_chart_style(fig, rotate_x=True)
    st.plotly_chart(fig, use_container_width=True, key=f"{widget_key}_weekly_epi")


def render_positivity_panels(df, widget_key):
    ratio, positives, negatives = compute_global_positivity(df)
    age_df = build_age_family_df(df)

    left, right = st.columns(2, gap="medium")

    with left:
        if ratio is None:
            st.info("Taux de positivite global indisponible.")
        else:
            donut_df = pd.DataFrame(
                {
                    "statut": ["Positif", "Negatif"],
                    "value": [max(positives, 0.0), max(negatives, 0.0)],
                }
            )
            fig = px.pie(
                donut_df,
                names="statut",
                values="value",
                hole=0.58,
                color="statut",
                color_discrete_map={"Positif": CHART_COLORS["positive"], "Negatif": CHART_COLORS["negative"]},
            )
            fig.update_layout(
                title=f"Taux de positivite global ({ratio:.1f}%)",
                margin=dict(l=0, r=0, t=45, b=0),
                showlegend=True,
            )
            fig.update_traces(
                textposition="outside",
                texttemplate="%{value:,.0f} (%{percent})",
                marker=dict(line=dict(color="#ffffff", width=1)),
            )
            apply_chart_style(fig, legend_y=-0.2, hovermode=False)
            st.plotly_chart(fig, use_container_width=True, key=f"{widget_key}_positivity_global")

    with right:
        if age_df.empty:
            proxy_col, proxy_label = choose_proxy_org_column(df)
            if not proxy_col:
                st.info("Taux de positivite par tranche d'age indisponible.")
            else:
                proxy_df = df.copy()
                proxy_df["family"] = proxy_df["dx_name"].map(detect_metric_family)
                proxy_family = (
                    proxy_df.dropna(subset=["family"])
                    .groupby([proxy_col, "family"], as_index=False)["value"]
                    .sum()
                )
                if proxy_family.empty:
                    st.info("Taux de positivite par tranche d'age indisponible.")
                else:
                    pivot = proxy_family.pivot_table(
                        index=proxy_col,
                        columns="family",
                        values="value",
                        aggfunc="sum",
                        fill_value=0.0,
                    ).reset_index()

                    def _proxy_ratio(row):
                        pos = float(row.get("positif", 0.0))
                        neg = float(row.get("negatif", 0.0))
                        tests = float(row.get("tests", 0.0))
                        den = pos + neg
                        if den <= 0 and tests > 0:
                            den = tests
                        if den <= 0:
                            sus = float(row.get("suspects", 0.0))
                            conf = float(row.get("confirmes", 0.0))
                            den = sus
                            return (conf / den * 100.0) if den > 0 else 0.0
                        return (pos / den * 100.0) if den > 0 else 0.0

                    pivot["ratio"] = pivot.apply(_proxy_ratio, axis=1)
                    pivot = pivot.sort_values("ratio", ascending=True).tail(10)
                    fig = px.bar(
                        pivot,
                        x="ratio",
                        y=proxy_col,
                        orientation="h",
                        color_discrete_sequence=[CHART_COLORS["positivity"]],
                    )
                    fig.update_layout(
                        title=f"Taux de positivite (proxy par {proxy_label})",
                        xaxis_title="Positivite (%)",
                        yaxis_title=proxy_col,
                        margin=dict(l=0, r=0, t=45, b=0),
                    )
                    fig.update_traces(texttemplate="%{x:.1f}%", textposition="outside", cliponaxis=False)
                    max_ratio = float(pivot["ratio"].max()) if not pivot.empty else 100.0
                    fig.update_xaxes(range=[0, max(100.0, max_ratio * 1.15)])
                    apply_chart_style(fig, hovermode="y unified")
                    st.plotly_chart(fig, use_container_width=True, key=f"{widget_key}_positivity_org_proxy")
        else:
            age_pos = age_df.pivot_table(index="age_group", columns="family", values="value", aggfunc="sum", fill_value=0.0).reset_index()
            age_pos = sort_age_group_frame(age_pos)
            if "positif" not in age_pos.columns:
                st.info("Taux de positivite par tranche d'age indisponible.")
            else:
                def _age_ratio(row):
                    pos = float(row.get("positif", 0.0))
                    neg = float(row.get("negatif", 0.0))
                    tests = float(row.get("tests", 0.0))
                    den = pos + neg
                    if den <= 0 and tests > 0:
                        den = tests
                    return (pos / den * 100.0) if den > 0 else 0.0

                age_pos["ratio"] = age_pos.apply(_age_ratio, axis=1)
                age_pos = age_pos.sort_values("ratio", ascending=True)
                fig = px.bar(
                    age_pos,
                    x="ratio",
                    y="age_group",
                    orientation="h",
                    color_discrete_sequence=[CHART_COLORS["positivity"]],
                )
                fig.update_layout(
                    title="Taux de positivite par tranche d'age",
                    xaxis_title="Positivite (%)",
                    yaxis_title="Tranche d'age",
                    margin=dict(l=0, r=0, t=45, b=0),
                )
                fig.update_traces(texttemplate="%{x:.1f}%", textposition="outside", cliponaxis=False)
                max_ratio = float(age_pos["ratio"].max()) if not age_pos.empty else 100.0
                fig.update_xaxes(range=[0, max(100.0, max_ratio * 1.15)])
                apply_chart_style(fig, hovermode="y unified")
                st.plotly_chart(fig, use_container_width=True, key=f"{widget_key}_positivity_age")


def render_positivity_trend(df, widget_key):
    if df.empty:
        st.info("Evolution du taux de positivite indisponible.")
        return

    frame = df.copy()
    frame["family"] = frame["dx_name"].map(detect_metric_family)
    by_period_family = (
        frame.dropna(subset=["family"])
        .groupby(["pe", "pe_name", "family"], as_index=False)["value"]
        .sum()
    )
    if by_period_family.empty:
        st.info("Evolution du taux de positivite indisponible.")
        return

    pivot = by_period_family.pivot_table(
        index=["pe", "pe_name"],
        columns="family",
        values="value",
        aggfunc="sum",
        fill_value=0.0,
    ).reset_index()
    pivot = sort_period_frame(pivot)

    def _ratio_row(row):
        pos = float(row.get("positif", 0.0))
        neg = float(row.get("negatif", 0.0))
        tests = float(row.get("tests", 0.0))
        den = pos + neg
        if den <= 0 and tests > 0:
            den = tests
        if den <= 0:
            # Fallback proxy: confirmes / suspects
            sus = float(row.get("suspects", 0.0))
            conf = float(row.get("confirmes", 0.0))
            den = sus if sus > 0 else 0.0
            return (conf / den * 100.0) if den > 0 else 0.0
        return (pos / den * 100.0) if den > 0 else 0.0

    pivot["ratio"] = pivot.apply(_ratio_row, axis=1)
    if pivot["ratio"].sum() == 0:
        st.info("Evolution du taux de positivite indisponible.")
        return

    fig = px.line(
        pivot,
        x="pe_name",
        y="ratio",
        markers=True,
        color_discrete_sequence=[CHART_COLORS["positivity"]],
    )
    fig.update_layout(
        title="Evolution du taux de positivite",
        yaxis_title="Positivite (%)",
        xaxis_title="Semaine epidemiologique",
        margin=dict(l=0, r=0, t=45, b=0),
    )
    fig.update_traces(line=dict(width=2.2), marker=dict(size=7))
    fig.update_yaxes(range=[0, max(100.0, float(pivot["ratio"].max()) * 1.15)])
    apply_chart_style(fig, rotate_x=True)
    st.plotly_chart(fig, use_container_width=True, key=f"{widget_key}_positivity_trend")


def render_age_distribution(df, widget_key):
    age_df = build_age_family_df(df)
    if age_df.empty:
        # Fallback proxy by selected org level when no age groups are present.
        proxy = df.copy()
        proxy_col, proxy_label = choose_proxy_org_column(proxy)
        if not proxy_col:
            st.info("Repartition par tranche d'age indisponible.")
            return
        proxy["family"] = proxy["dx_name"].map(detect_metric_family)
        proxy = proxy[proxy["family"].isin(["suspects", "confirmes", "deces"])]
        if proxy.empty:
            st.info("Repartition par tranche d'age indisponible.")
            return
        pivot_org = (
            proxy.groupby([proxy_col, "family"], as_index=False)["value"]
            .sum()
            .pivot_table(index=proxy_col, columns="family", values="value", aggfunc="sum", fill_value=0.0)
            .reset_index()
        )
        ordered_cols = [c for c in ["suspects", "confirmes", "deces"] if c in pivot_org.columns]
        fig_proxy = go.Figure()
        color_map = {
            "suspects": CHART_COLORS["suspects"],
            "confirmes": CHART_COLORS["confirmed"],
            "deces": CHART_COLORS["deaths"],
        }
        label_map = {"suspects": "Cas suspects", "confirmes": "Cas confirmes", "deces": "Deces"}
        for col in ordered_cols:
            fig_proxy.add_trace(
                go.Bar(
                    y=pivot_org[proxy_col],
                    x=pivot_org[col],
                    name=label_map.get(col, col),
                    orientation="h",
                    marker_color=color_map.get(col, CHART_COLORS["cases"]),
                )
            )
        fig_proxy.update_layout(
            barmode="group",
            title=f"Repartition des cas (proxy par {proxy_label})",
            margin=dict(l=0, r=0, t=45, b=0),
            xaxis_title="Nombre de cas",
            yaxis_title=proxy_col,
        )
        add_bar_value_labels(fig_proxy, orientation="h")
        apply_chart_style(fig_proxy, hovermode="y unified")
        st.plotly_chart(fig_proxy, use_container_width=True, key=f"{widget_key}_age_distribution_proxy")
        return

    pivot = (
        age_df[age_df["family"].isin(["suspects", "confirmes", "deces"])]
        .pivot_table(index="age_group", columns="family", values="value", aggfunc="sum", fill_value=0.0)
        .reset_index()
    )
    pivot = sort_age_group_frame(pivot)
    if pivot.empty:
        st.info("Repartition par tranche d'age indisponible.")
        return

    ordered_cols = [c for c in ["suspects", "confirmes", "deces"] if c in pivot.columns]
    fig = go.Figure()
    color_map = {
        "suspects": CHART_COLORS["suspects"],
        "confirmes": CHART_COLORS["confirmed"],
        "deces": CHART_COLORS["deaths"],
    }
    label_map = {"suspects": "Cas suspects", "confirmes": "Cas confirmes", "deces": "Deces"}
    for col in ordered_cols:
        fig.add_trace(
            go.Bar(
                y=pivot["age_group"],
                x=pivot[col],
                name=label_map.get(col, col),
                orientation="h",
                marker_color=color_map.get(col, CHART_COLORS["cases"]),
            )
        )
    fig.update_layout(
        barmode="group",
        title="Repartition par tranche d'age: cas suspects, confirmes et deces",
        margin=dict(l=0, r=0, t=45, b=0),
        xaxis_title="Nombre de cas",
        yaxis_title="Tranche d'age",
    )
    add_bar_value_labels(fig, orientation="h")
    apply_chart_style(fig, hovermode="y unified")
    st.plotly_chart(fig, use_container_width=True, key=f"{widget_key}_age_distribution")


def render_stat_panel(title, subtitle, items, columns_count):
    cards_html = "".join(
        [
            (
                "<div class='bsq-stat-item'>"
                f"<div class='bsq-icon'>{item['icon']}</div>"
                "<div>"
                f"<div class='bsq-stat-label'>{item['label']}</div>"
                f"<div class='bsq-stat-value'>{item['value']}</div>"
                "</div>"
                "</div>"
            )
            for item in items
        ]
    )

    st.markdown(
        (
            "<div class='bsq-panel'>"
            f"<h4>{title}</h4>"
            f"<div class='bsq-sub'>{subtitle}</div>"
            f"<div class='bsq-stat-grid' style='grid-template-columns: repeat({columns_count}, minmax(0, 1fr));'>"
            f"{cards_html}"
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def render_evolution_comparison(df, context):
    metric_id = context["cases"]["dx"] if context.get("cases") else None
    if not metric_id:
        totals = context.get("totals")
        if totals is None or totals.empty:
            st.info("Evolution indisponible.")
            return
        metric_id = str(totals.iloc[0]["dx"])

    trend = df[df["dx"] == metric_id].groupby(["pe", "pe_name"], as_index=False)["value"].sum()
    trend = sort_period_frame(trend)
    if len(trend) < 2:
        st.info("Pas assez de periodes pour la comparaison.")
        return

    recent = trend.tail(4).reset_index(drop=True)
    st.markdown("<div class='bsq-compare'><h5>Comparaison de l'evolution</h5></div>", unsafe_allow_html=True)
    cols = st.columns(3)
    for idx in range(1, len(recent)):
        current = float(recent.iloc[idx]["value"])
        previous = float(recent.iloc[idx - 1]["value"])
        delta_pct = ((current - previous) / previous * 100.0) if previous != 0 else 0.0
        label = str(recent.iloc[idx]["pe_name"])
        arrow = "&#8593;" if delta_pct >= 0 else "&#8595;"
        color = "#d1495b" if delta_pct >= 0 else "#2f9e44"
        with cols[min(idx - 1, 2)]:
            st.markdown(
                (
                    "<div style='background:#15253b;border:1px solid #2c4160;border-radius:10px;padding:8px 10px;'>"
                    f"<div style='font-size:0.9rem;color:#a8bdd5'>{label}</div>"
                    f"<div style='font-size:2rem;font-weight:800;color:#e8eff7'>{delta_pct:+.1f}% "
                    f"<span style='color:{color};font-size:1.35rem'>{arrow}</span></div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )


def render_case_map(df, context, widget_key):
    metric_id = context["cases"]["dx"] if context.get("cases") else None
    map_df = df if not metric_id else df[df["dx"] == metric_id]
    if map_df.empty:
        st.info("Cartographie indisponible.")
        return

    rows = []
    for ou_name, value in map_df.groupby("ou_name")["value"].sum().items():
        ou_norm = normalize_text(ou_name)
        for key, (label, lat, lon) in PROVINCE_COORDS.items():
            if key in ou_norm:
                rows.append({"province": label, "lat": lat, "lon": lon, "value": float(value)})
                break

    if not rows:
        fallback = (
            map_df.groupby("ou_name", as_index=False)["value"]
            .sum()
            .sort_values("value", ascending=False)
            .head(10)
        )
        fig = px.bar(
            fallback.sort_values("value", ascending=True),
            x="value",
            y="ou_name",
            orientation="h",
            color="value",
            color_continuous_scale="YlOrRd",
        )
        fig.update_layout(
            title="Cartographie de cas suspects et deces (proxy par OU)",
            xaxis_title="Cas",
            yaxis_title="Org unit",
            margin=dict(l=10, r=10, t=40, b=10),
            coloraxis_showscale=False,
        )
        add_bar_value_labels(fig, orientation="h")
        apply_chart_style(fig, hovermode="y unified")
        st.plotly_chart(fig, use_container_width=True, key=f"{widget_key}_map_fallback")
        return

    province_df = pd.DataFrame(rows).groupby(["province", "lat", "lon"], as_index=False)["value"].sum()
    fig = px.scatter_geo(
        province_df,
        lat="lat",
        lon="lon",
        size="value",
        color="value",
        hover_name="province",
        projection="natural earth",
        color_continuous_scale="YlOrRd",
    )
    fig.update_geos(
        showcountries=True,
        countrycolor="#3a4f67",
        showcoastlines=False,
        lataxis_range=[-14, 6],
        lonaxis_range=[11, 32],
        landcolor="#15263b",
        bgcolor="rgba(0,0,0,0)",
    )
    fig.update_layout(
        title="Cartographie de cas suspects et deces",
        margin=dict(l=0, r=0, t=40, b=0),
        coloraxis_showscale=False,
    )
    apply_chart_style(fig, hovermode=False)
    st.plotly_chart(fig, use_container_width=True, key=f"{widget_key}_map")


def build_donut_source(df, metric_id):
    if not metric_id:
        return pd.DataFrame(columns=["label", "value"])
    source = (
        df[df["dx"] == metric_id]
        .groupby("ou_name", as_index=False)["value"]
        .sum()
        .sort_values("value", ascending=False)
        .head(4)
    )
    if source.empty:
        return pd.DataFrame(columns=["label", "value"])
    source = source.rename(columns={"ou_name": "label", "value": "value"})
    return source


def render_donuts(df, context, widget_key):
    left, right = st.columns(2)

    with left:
        source_1 = build_donut_source(df, context["cases"]["dx"] if context.get("cases") else None)
        if source_1.empty:
            st.info("Donut indisponible.")
        else:
            fig_1 = px.pie(
                source_1,
                names="label",
                values="value",
                hole=0.55,
                color_discrete_sequence=PIE_COLORS_MAIN,
            )
            fig_1.update_layout(
                title="Proportion des cas suspects",
                margin=dict(l=0, r=0, t=42, b=0),
            )
            fig_1.update_traces(
                textposition="outside",
                texttemplate="%{value:,.0f} (%{percent})",
                marker=dict(line=dict(color="#ffffff", width=1)),
            )
            apply_chart_style(fig_1, legend_y=-0.2, hovermode=False)
            st.plotly_chart(fig_1, use_container_width=True, key=f"{widget_key}_donut_cases")

    with right:
        source_2 = build_donut_source(df, context["deaths"]["dx"] if context.get("deaths") else None)
        if source_2.empty:
            fallback_metric = context["extra"][0]["dx"] if context.get("extra") else None
            source_2 = build_donut_source(df, fallback_metric)
        if source_2.empty:
            st.info("Donut indisponible.")
        else:
            fig_2 = px.pie(
                source_2,
                names="label",
                values="value",
                hole=0.55,
                color_discrete_sequence=PIE_COLORS_ALT,
            )
            fig_2.update_layout(
                title="Proportion des deces",
                margin=dict(l=0, r=0, t=42, b=0),
            )
            fig_2.update_traces(
                textposition="outside",
                texttemplate="%{value:,.0f} (%{percent})",
                marker=dict(line=dict(color="#ffffff", width=1)),
            )
            apply_chart_style(fig_2, legend_y=-0.2, hovermode=False)
            st.plotly_chart(fig_2, use_container_width=True, key=f"{widget_key}_donut_deaths")


def render_trend_combo(df, context, widget_key):
    case_metric = context["cases"]["dx"] if context.get("cases") else None
    if not case_metric:
        st.info("Evolution indisponible.")
        return

    cases = df[df["dx"] == case_metric].groupby(["pe", "pe_name"], as_index=False)["value"].sum()
    cases = sort_period_frame(cases)
    if cases.empty:
        st.info("Evolution indisponible.")
        return

    line_series = pd.DataFrame()
    if context.get("lethality"):
        line_series = df[df["dx"] == context["lethality"]["dx"]].groupby(["pe", "pe_name"], as_index=False)["value"].sum()
        line_series = sort_period_frame(line_series).rename(columns={"value": "letalite"})
    elif context.get("deaths"):
        deaths = df[df["dx"] == context["deaths"]["dx"]].groupby(["pe", "pe_name"], as_index=False)["value"].sum()
        deaths = sort_period_frame(deaths).rename(columns={"value": "deces"})
        line_series = cases.merge(deaths[["pe", "deces"]], on="pe", how="left").fillna(0.0)
        line_series["letalite"] = line_series.apply(
            lambda row: (row["deces"] / row["value"] * 100.0) if row["value"] else 0.0, axis=1
        )

    merged = cases.rename(columns={"value": "cas"}).copy()
    if not line_series.empty:
        merged = merged.merge(line_series[["pe", "letalite"]], on="pe", how="left")
    else:
        merged["letalite"] = 0.0
    merged["letalite"] = merged["letalite"].fillna(0.0)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(
            x=merged["pe_name"],
            y=merged["cas"],
            name="Cas suspects",
            marker_color=CHART_COLORS["cases"],
            opacity=0.9,
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=merged["pe_name"],
            y=merged["letalite"],
            name="Letalite",
            mode="lines+markers",
            line=dict(color=CHART_COLORS["lethality"], width=2.2),
            marker=dict(size=6),
        ),
        secondary_y=True,
    )
    fig.update_layout(
        title="Evolution du nombre de cas suspects et de la letalite",
        margin=dict(l=10, r=10, t=42, b=8),
    )
    fig.update_yaxes(title_text="Cas", secondary_y=False)
    fig.update_yaxes(title_text="Letalite (%)", secondary_y=True)
    add_bar_value_labels(fig, orientation="v")
    apply_chart_style(fig, rotate_x=True)
    st.plotly_chart(fig, use_container_width=True, key=f"{widget_key}_combo")


def render_section_panel(
    section_key,
    section_label,
    sgi,
    data_frame,
    aux_frame,
    widget_prefix,
    hierarchy_by_ou,
    source_label,
    cache_identity,
):
    if data_frame.empty:
        st.info("Aucune donnee retournee par DHIS2 pour les filtres selectionnes.")
        return

    enriched_df = keep_granular_org_level(enrich_with_hierarchy(data_frame, hierarchy_by_ou))
    aux_enriched = (
        keep_granular_org_level(enrich_with_hierarchy(aux_frame, hierarchy_by_ou))
        if aux_frame is not None and not aux_frame.empty
        else pd.DataFrame()
    )

    st.markdown(f"<div class='bsq-title'>{sgi.title()} - {section_label}</div>", unsafe_allow_html=True)
    filtered_df, selected_filters = render_org_filters(
        enriched_df,
        sgi=sgi,
        widget_key=f"{widget_prefix}_{section_key}",
        source_label=source_label,
        cache_identity=cache_identity,
    )

    if filtered_df.empty:
        fallback_selection = dict(selected_filters)
        if fallback_selection.get("aire") != "(Tout)":
            fallback_selection["aire"] = "(Tout)"
            fallback_selection["aire_ids"] = []
            fallback_df = apply_selected_filters(enriched_df, fallback_selection)
            if not fallback_df.empty:
                st.warning(
                    "Aucune donnee directe pour l'aire selectionnee. Affichage agrege au niveau Zone de Sante."
                )
                filtered_df = fallback_df
                selected_filters = fallback_selection

        if filtered_df.empty:
            aux_fallback = apply_selected_filters(aux_enriched, selected_filters) if not aux_enriched.empty else pd.DataFrame()
            if aux_fallback.empty:
                st.warning("Aucune donnee apres application des filtres Province / Zone / Aire / Date.")
                return
            st.warning(
                "Aucune metrique principale disponible pour ce filtre. Affichage base sur les metriques auxiliaires."
            )
            filtered_df = aux_fallback
            aux_enriched = pd.DataFrame()

    filtered_aux = apply_selected_filters(aux_enriched, selected_filters) if not aux_enriched.empty else pd.DataFrame()
    combined_df = (
        pd.concat([filtered_df, filtered_aux], ignore_index=True)
        .drop_duplicates(subset=["dx", "pe", "ou", "value"])
        if not filtered_aux.empty
        else filtered_df
    )

    context = select_metric_context(filtered_df)
    latest_df = get_latest_period_df(filtered_df)

    period_caption = (
        f"{format_period_label(selected_filters['period_start'])} au {format_period_label(selected_filters['period_end'])}"
        if selected_filters.get("period_start") and selected_filters.get("period_end")
        else "Periode non definie"
    )
    latest_caption = format_period_label(str(latest_df["pe"].iloc[0])) if not latest_df.empty else "N/A"

    top_left, top_right = st.columns([1.02, 1.02], gap="medium")
    with top_left:
        render_stat_panel(
            title="Situation generale de l'epidemie",
            subtitle=period_caption,
            items=build_general_items(filtered_df, context),
            columns_count=3,
        )
    with top_right:
        render_stat_panel(
            title="Situation hebdomadaire de l'epidemie",
            subtitle=latest_caption,
            items=build_weekly_items(latest_df, context),
            columns_count=2,
        )

    mid_left, mid_right = st.columns([1.0, 1.0], gap="medium")
    with mid_left:
        render_evolution_comparison(filtered_df, context)
        render_case_map(filtered_df, context, widget_key=f"{widget_prefix}_{section_key}")

    with mid_right:
        render_weekly_epi_situation(filtered_df, context, widget_key=f"{widget_prefix}_{section_key}")
        render_trend_combo(filtered_df, context, widget_key=f"{widget_prefix}_{section_key}")

    render_age_proportions(combined_df, widget_key=f"{widget_prefix}_{section_key}")
    render_positivity_panels(combined_df, widget_key=f"{widget_prefix}_{section_key}")
    render_positivity_trend(combined_df, widget_key=f"{widget_prefix}_{section_key}")
    render_age_distribution(combined_df, widget_key=f"{widget_prefix}_{section_key}")

    with st.expander("Table brute"):
        render_data_table(filtered_df)


def render_header():
    logo_path = Path(__file__).resolve().parent / "assets" / "logo_insp_cousp.png"
    logo_uri = image_to_data_uri(str(logo_path))
    logo_html = (
        f"<img src='{logo_uri}' alt='Logo INSP/COUSP' class='bsq-logo-img' />"
        if logo_uri
        else "<div class='bsq-logo-fallback'>INSP</div>"
    )

    st.markdown(
        f"""
        <div class="bsq-head">
            <div class="bsq-logos">
                <div class="bsq-logo-wrap">
                    {logo_html}
                </div>
                <div class="bsq-ministry">
                    <span>
                        Ministere<br>
                        de la Sante Publique,<br>
                        Hygiene et Prevoyance<br>
                        Sociale
                    </span>
                </div>
            </div>
            <div class="bsq-head-main">
                <h2>Institut National de Sante Publique</h2>
                <p>
                    Centre des Operations d'Urgence de Sante Publique (COUSP)
                </p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_login_sidebar(data_sources):
    source_items = list((data_sources or {}).items())
    if not source_items:
        with st.sidebar:
            st.error("Aucune source DHIS2 configuree.")
        return

    source_labels = [label for label, _ in source_items]
    source_by_label = {label: url for label, url in source_items}
    single_source = len(source_items) == 1

    with st.sidebar:
        st.subheader("Connexion DHIS2")
        st.caption("Connexion directe au serveur DHIS2.")

        current_label = str(st.session_state.get("dhis2_source_label", "")).strip()
        current_base = str(st.session_state.get("dhis2_base_url", "")).strip()
        source_index = 0
        if current_label in source_labels:
            source_index = source_labels.index(current_label)
        elif current_base:
            for idx, (_, url) in enumerate(source_items):
                if normalize_url_key(url) == normalize_url_key(current_base):
                    source_index = idx
                    break

        if single_source:
            selected_source_label = source_labels[0]
            st.markdown("**Source de donnees:** EZD")
        else:
            selected_source_label = st.selectbox(
                "Source de donnees",
                source_labels,
                index=source_index,
                key="sgi_source_selector",
            )
        selected_base_url = source_by_label[selected_source_label]
        st.caption(f"URL: {selected_base_url}")
        if normalize_url_key(current_base) != normalize_url_key(selected_base_url):
            st.session_state["dhis2_base_url"] = selected_base_url
            st.session_state["dhis2_source_label"] = selected_source_label
            connected_base = str(st.session_state.get("dhis2_connected_base_url", "")).strip()
            if st.session_state.get("connected_sgi") and (
                normalize_url_key(connected_base) != normalize_url_key(selected_base_url)
            ):
                st.session_state["connected_sgi"] = False
                st.info("Source changee. Clique sur 'Se connecter' pour recharger.")
        else:
            st.session_state["dhis2_source_label"] = selected_source_label

        username_input = st.text_input(
            "Nom d'utilisateur",
            value=str(st.session_state.get("dhis2_user", "")),
            key="sgi_user_input",
        )
        password_input = st.text_input(
            "Mot de passe",
            type="password",
            key="sgi_password_input",
        )

        col_login, col_logout = st.columns(2)
        login_btn = col_login.button("Se connecter", use_container_width=True)
        logout_btn = col_logout.button("Se deconnecter", use_container_width=True)

        if logout_btn:
            for key in [
                "connected_sgi",
                "dhis2_user",
                "dhis2_pass",
                "dhis2_connected_base_url",
                "sgi_user_input",
                "sgi_password_input",
            ]:
                st.session_state.pop(key, None)
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()

        if login_btn:
            if not username_input or not password_input:
                st.session_state["connected_sgi"] = False
                st.warning("Renseigne le nom d'utilisateur et le mot de passe.")
            else:
                ok, error = test_dhis2_credentials(selected_base_url, username_input, password_input)
                if ok:
                    st.session_state["connected_sgi"] = True
                    st.session_state["dhis2_user"] = username_input
                    st.session_state["dhis2_pass"] = password_input
                    st.session_state["dhis2_base_url"] = selected_base_url
                    st.session_state["dhis2_source_label"] = selected_source_label
                    st.session_state["dhis2_connected_base_url"] = selected_base_url
                    st.cache_data.clear()
                    st.cache_resource.clear()
                    st.success(f"Connecte: {username_input} ({selected_source_label})")
                    st.rerun()
                else:
                    st.session_state["connected_sgi"] = False
                    st.error(error)


def main():
    apply_blue_theme()

    data_sources = resolve_data_sources()
    if not data_sources:
        st.error(
            "Aucune source DHIS2 disponible. Configure DHIS2_URL_EZD ou DHIS2_URL."
        )
        st.stop()

    render_login_sidebar(data_sources)

    if not st.session_state.get("connected_sgi"):
        st.info("Connecte-toi dans la barre laterale pour charger le dashboard.")
        st.stop()

    active_base_url = str(st.session_state.get("dhis2_base_url", "")).strip()
    if not active_base_url:
        active_base_url = next(iter(data_sources.values()))
        st.session_state["dhis2_base_url"] = active_base_url

    active_source_label = str(st.session_state.get("dhis2_source_label", "")).strip()
    if not active_source_label:
        active_source_label = next(iter(data_sources.keys()))
        st.session_state["dhis2_source_label"] = active_source_label

    user_identity = f"{active_base_url}|{st.session_state.get('dhis2_user', '')}"
    try:
        me_data = get_me(user_identity)
    except Exception as exc:
        st.error(f"Impossible de recuperer /api/me: {exc}")
        st.stop()

    sgi_group_mapping = read_json_config("DHIS2_SGI_GROUPS", default={}) or {}
    allowed_sgis = resolve_allowed_sgis(me_data, sgi_group_mapping)
    if not allowed_sgis:
        st.error(
            "Aucun SGI autorise pour cet utilisateur. Configure DHIS2_SGI_GROUPS ou active DHIS2_ALLOW_ALL_IF_NO_GROUP_MATCH."
        )
        st.stop()

    user_ou_ids = tuple(
        sorted(
            {
                str(ou.get("id", "")).strip()
                for ou in get_user_org_units(me_data)
                if isinstance(ou, Mapping) and str(ou.get("id", "")).strip()
            }
        )
    )

    with st.sidebar:
        st.divider()
        st.subheader("Filtres")
        years_back = read_int_config_value("DHIS2_PERIOD_YEARS_BACK", 4)
        period_catalog = build_period_catalog(years_back=years_back)
        if not period_catalog:
            st.error("Impossible de construire la liste des periodes.")
            st.stop()

        default_end = period_catalog[-1]
        default_start = period_catalog[max(0, len(period_catalog) - 3)]

        period_start = st.selectbox(
            "Periode debut",
            period_catalog,
            index=period_catalog.index(default_start),
            format_func=format_period_label,
        )
        period_end = st.selectbox(
            "Periode fin",
            period_catalog,
            index=period_catalog.index(default_end),
            format_func=format_period_label,
        )
        selected_periods = build_period_range(period_start, period_end, period_catalog)
        fixed_ou_scope = str(read_config_value("DHIS2_FIXED_OU_SCOPE", DEFAULT_OU_SCOPE)).strip() or DEFAULT_OU_SCOPE

    render_header()

    metrics_config = read_json_config("DHIS2_SGI_METRICS", default={}) or {}

    try:
        indicator_catalog = get_indicator_catalog(user_identity)
    except Exception as exc:
        st.error(f"Impossible de charger le catalogue des indicateurs: {exc}")
        st.stop()

    resolved_base = resolve_dhis2_base_url(
        active_base_url,
        username=str(st.session_state.get("dhis2_user", "")).strip(),
        password=str(st.session_state.get("dhis2_pass", "")).strip(),
    )
    source_host = urlparse(resolved_base).netloc or str(resolved_base)
    source_label = active_source_label
    if source_host:
        source_label = f"{active_source_label} ({source_host})"

    selected_sgi = st.selectbox(
        "Maladie",
        allowed_sgis,
        key="global_selected_sgi",
    )

    section_tabs = st.tabs([section_label for _, section_label in SECTION_TABS])
    for (section_key, section_label), section_tab in zip(SECTION_TABS, section_tabs):
        with section_tab:
            metrics = get_metrics_for_section(
                metrics_config=metrics_config,
                indicator_catalog=indicator_catalog,
                sgi=selected_sgi,
                section_key=section_key,
            )
            dx_ids = tuple(metric["id"] for metric in metrics)
            if not dx_ids:
                st.warning("Aucune metrique detectee pour cet onglet.")
                continue

            aux_metrics = discover_auxiliary_metrics(indicator_catalog, selected_sgi, limit=80)
            aux_dx_ids = tuple(m["id"] for m in aux_metrics if m["id"] not in dx_ids)

            try:
                section_df = fetch_analytics(
                    cache_identity=f"{user_identity}|{selected_sgi}|{section_key}|core",
                    dx_ids=dx_ids,
                    period_ids=tuple(selected_periods),
                    ou_dimension=fixed_ou_scope,
                    ou_explicit_ids=user_ou_ids,
                )
            except Exception as exc:
                st.error(f"Erreur de lecture analytics DHIS2: {exc}")
                section_df = pd.DataFrame()

            try:
                aux_df = (
                    fetch_analytics(
                        cache_identity=f"{user_identity}|{selected_sgi}|{section_key}|aux",
                        dx_ids=aux_dx_ids,
                        period_ids=tuple(selected_periods),
                        ou_dimension=fixed_ou_scope,
                        ou_explicit_ids=user_ou_ids,
                    )
                    if aux_dx_ids
                    else pd.DataFrame()
                )
            except Exception:
                aux_df = pd.DataFrame()

            hierarchy_map = {}
            merged_ou = []
            if not section_df.empty and "ou" in section_df.columns:
                merged_ou.extend(section_df["ou"].astype(str).tolist())
            if not aux_df.empty and "ou" in aux_df.columns:
                merged_ou.extend(aux_df["ou"].astype(str).tolist())
            if merged_ou:
                try:
                    hierarchy_map = get_org_units_hierarchy(
                        cache_identity=f"{user_identity}|hierarchy",
                        org_unit_ids=tuple(sorted(set(merged_ou))),
                    )
                except Exception:
                    hierarchy_map = {}

            render_section_panel(
                section_key=section_key,
                section_label=section_label,
                sgi=selected_sgi,
                data_frame=section_df,
                aux_frame=aux_df,
                widget_prefix=normalize_text(selected_sgi),
                hierarchy_by_ou=hierarchy_map,
                source_label=source_label,
                cache_identity=f"{user_identity}|{selected_sgi}|{section_key}",
            )


if __name__ == "__main__":
    main()
