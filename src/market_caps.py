from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import html
import json
import os
import re
import subprocess
import sys
import textwrap
import zlib
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable
from calendar import month_name
from pathlib import Path
from typing import Any


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"
)
ROOT = Path(__file__).resolve().parent.parent
SP_GLOBAL_BMI_SNAPSHOT_PATH = ROOT / "data" / "sp_global_bmi_country_breakdown.csv"
SP_FACTSHEET_ENDPOINT = "https://www.spglobal.com/spdji/en/idsenhancedfactsheet/file.pdf"
SP_FACTSHEET_HOST_IDENTIFIER = "48190c8c-42c4-46af-8d1a-0cd5db894797"
SP_ALLOW_STALE_SNAPSHOT_ENV = "SP_ALLOW_STALE_SNAPSHOT"
MAX_INDEX_SOURCE_AGE_DAYS = 60
MSCI_ACWI_IMI_PAGE_URL = "https://www.msci.com/visualizing-investment-data/acwi-imi-complete-geographic-breakdown"


@dataclasses.dataclass(frozen=True)
class SourceCheck:
    name: str
    url: str
    expected_terms: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class IndexSource:
    name: str
    url: str
    factsheet_url: str
    factsheet_url_env: str
    factsheet_file_env: str
    index_id: str | None = None
    host_identifier: str = SP_FACTSHEET_HOST_IDENTIFIER


@dataclasses.dataclass(frozen=True)
class CountryCap:
    country: str
    constituents: int | None
    market_cap_usd_millions: float
    index_weight_pct: float | None
    source: str


@dataclasses.dataclass(frozen=True)
class MarketCapDataset:
    country_caps: dict[str, CountryCap]
    denominator_usd_millions: float
    source_label: str
    freshness_label: str
    source_url: str | None = None
    share_basis: str = "market_cap"


@dataclasses.dataclass(frozen=True)
class GroupResult:
    label: str
    ticker: str
    countries: tuple[str, ...]
    market_cap_usd_millions: float
    share_of_global_pct: float
    missing_countries: tuple[str, ...]


ETF_SOURCE_CHECKS = (
    SourceCheck(
        name="XUU.TO",
        url="https://www.blackrock.com/ca/investors/en/products/272104/ishares-core-sp-us-total-market-index-etf",
        expected_terms=("S&P Total Market",),
    ),
    SourceCheck(
        name="VCN.TO",
        url="https://www.vanguard.ca/en/advisor/products/products-group/etfs/VCN",
        expected_terms=("FTSE Canada All Cap",),
    ),
    SourceCheck(
        name="VIU.TO",
        url="https://www.vanguard.ca/en/advisor/products/products-group/etfs/VIU",
        expected_terms=("FTSE Developed All Cap ex North America",),
    ),
    SourceCheck(
        name="XEC.TO",
        url="https://www.blackrock.com/ca/investors/en/products/251423/ishares-msci-emerging-markets-imi-index-etf",
        expected_terms=("MSCI Emerging Markets",),
    ),
)


PROVIDER_SOURCE_CHECKS = (
    SourceCheck(
        name="FTSE Russell equity country classification",
        url="https://www.lseg.com/en/ftse-russell/equity-country-classification",
        expected_terms=("Equity Country Classification", "Developed", "Country Classification"),
    ),
    SourceCheck(
        name="MSCI market classification",
        url="https://www.msci.com/indexes/index-resources/market-classification",
        expected_terms=("Market Classification", "Emerging Markets", "Current Market Classification"),
    ),
)


GLOBAL_INDEX_SOURCE = IndexSource(
    name="S&P Global BMI",
    url="https://www.spglobal.com/spdji/en/indices/equity/sp-global-bmi/",
    factsheet_url=(
        "https://www.spglobal.com/spdji/en/idsenhancedfactsheet/file.pdf"
        "?calcFrequency=M&force_download=true&hostIdentifier=48190c8c-42c4-46af-8d1a-0cd5db894797"
        "&indexId=5457913&languageId=1"
    ),
    factsheet_url_env="SP_GLOBAL_BMI_FACTSHEET_URL",
    factsheet_file_env="SP_GLOBAL_BMI_FACTSHEET_PATH",
    index_id="5457913",
)


INDEX_SOURCES = (
    IndexSource(
        name="S&P Developed BMI",
        url="https://www.spglobal.com/spdji/en/indices/equity/sp-developed-bmi/",
        factsheet_url=(
            "https://www.spglobal.com/spdji/en/idsenhancedfactsheet/file.pdf"
            "?calcFrequency=M&force_download=true&hostIdentifier=48190c8c-42c4-46af-8d1a-0cd5db894797"
            "&indexId=5457924"
        ),
        factsheet_url_env="SP_DEVELOPED_BMI_FACTSHEET_URL",
        factsheet_file_env="SP_DEVELOPED_BMI_FACTSHEET_PATH",
        index_id="5457924",
    ),
    IndexSource(
        name="S&P Emerging BMI",
        url="https://www.spglobal.com/spdji/en/indices/equity/sp-emerging-bmi/",
        factsheet_url=(
            "https://www.spglobal.com/spdji/en/idsenhancedfactsheet/file.pdf"
            "?calcFrequency=M&force_download=true&hostIdentifier=48190c8c-42c4-46af-8d1a-0cd5db894797"
            "&indexId=5457901"
        ),
        factsheet_url_env="SP_EMERGING_BMI_FACTSHEET_URL",
        factsheet_file_env="SP_EMERGING_BMI_FACTSHEET_PATH",
        index_id="5457901",
    ),
)


CMC_BASE = "https://companiesmarketcap.com"
WFE_BASE = "https://focus.world-exchanges.org/issue"
WFE_EXCHANGE_COUNTRIES = {
    "B3 - Brasil Bolsa Balcão": ("Brazil",),
    "Bolsa de Comercio de Santiago": ("Chile",),
    "Bolsa de Valores de Colombia": ("Colombia",),
    "Bolsa de Valores de Lima": ("Peru",),
    "Bolsa Electronica de Chile": ("Chile",),
    "Bolsa Mexicana de Valores": ("Mexico",),
    "BSE India Limited": ("India",),
    "Budapest Stock Exchange": ("Hungary",),
    "BME Spanish Exchanges": ("Spain",),
    "Borsa Istanbul": ("Turkey",),
    "Borsa Italiana": ("Italy",),
    "Boursa Kuwait": ("Kuwait",),
    "Bursa Malaysia": ("Malaysia",),
    "Deutsche Boerse AG": ("Germany",),
    "Dubai Financial Market": ("United Arab Emirates",),
    "Hong Kong Exchanges and Clearing": ("Hong Kong",),
    "Indonesia Stock Exchange": ("Indonesia",),
    "Japan Exchange Group": ("Japan",),
    "Johannesburg Stock Exchange": ("South Africa",),
    "Korea Exchange": ("South Korea",),
    "London Stock Exchange": ("United Kingdom",),
    "Nasdaq - US": ("United States",),
    "NYSE": ("United States",),
    "Philippine Stock Exchange": ("Philippines",),
    "Prague Stock Exchange": ("Czech Republic",),
    "Qatar Stock Exchange": ("Qatar",),
    "Saudi Exchange (Tadawul)": ("Saudi Arabia",),
    "SIX Swiss Exchange": ("Switzerland",),
    "Singapore Exchange": ("Singapore",),
    "Shanghai Stock Exchange": ("China",),
    "Shenzhen Stock Exchange": ("China",),
    "Taiwan Stock Exchange": ("Taiwan",),
    "Tel-Aviv Stock Exchange": ("Israel",),
    "The Egyptian Exchange": ("Egypt",),
    "The Stock Exchange of Thailand": ("Thailand",),
    "TMX Group": ("Canada",),
    "Warsaw Stock Exchange": ("Poland",),
}
COUNTRY_GROUPS = {
    "US": {
        "ticker": "XUU.TO",
        "countries": ("United States",),
    },
    "Canada": {
        "ticker": "VCN.TO",
        "countries": ("Canada",),
    },
    "Developed ex North America": {
        "ticker": "VIU.TO",
        "countries": (
            "Australia",
            "Austria",
            "Belgium",
            "Denmark",
            "Finland",
            "France",
            "Germany",
            "Hong Kong",
            "Ireland",
            "Israel",
            "Italy",
            "Japan",
            "Luxembourg",
            "Netherlands",
            "New Zealand",
            "Norway",
            "Poland",
            "Portugal",
            "Singapore",
            "South Korea",
            "Spain",
            "Sweden",
            "Switzerland",
            "United Kingdom",
        ),
    },
    "Emerging markets": {
        "ticker": "XEC.TO",
        "countries": (
            "Brazil",
            "Chile",
            "China",
            "Colombia",
            "Czech Republic",
            "Egypt",
            "Greece",
            "Hungary",
            "India",
            "Indonesia",
            "Kuwait",
            "Malaysia",
            "Mexico",
            "Peru",
            "Philippines",
            "Poland",
            "Qatar",
            "Saudi Arabia",
            "South Africa",
            "South Korea",
            "Taiwan",
            "Thailand",
            "Turkey",
            "United Arab Emirates",
        ),
    },
}


COUNTRY_ALIASES = {
    "Hong Kong SAR, China": "Hong Kong",
    "Korea": "South Korea",
    "Korea, Republic of": "South Korea",
    "Republic of Korea": "South Korea",
    "UK": "United Kingdom",
    "U.K.": "United Kingdom",
    "USA": "United States",
    "U.S.": "United States",
    "United States of America": "United States",
    "UAE": "United Arab Emirates",
}


COUNTRY_ROW_NAMES = tuple(
    sorted(
        {
            "Hong Kong SAR, China",
            *(country for config in COUNTRY_GROUPS.values() for country in config["countries"]),
        },
        key=len,
        reverse=True,
    )
)


def fetch_text(url: str, timeout: int = 30) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
        return raw.decode(charset, errors="replace")
    except urllib.error.HTTPError as exc:
        if exc.code != 403:
            raise
    except urllib.error.URLError:
        pass

    completed = subprocess.run(
        ["curl", "-L", "-s", "--max-time", str(timeout), "-A", USER_AGENT, url],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout + 5,
    )
    if completed.returncode != 0 or not completed.stdout:
        raise urllib.error.URLError(completed.stderr.strip() or "curl fallback returned no content")
    return completed.stdout


def fetch_bytes(url: str, timeout: int = 30, headers: dict[str, str] | None = None) -> bytes:
    request_headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/pdf,text/html;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    if headers:
        request_headers.update(headers)
    request = urllib.request.Request(
        url,
        headers=request_headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read()
    except urllib.error.HTTPError as exc:
        if exc.code != 403:
            raise
    except urllib.error.URLError:
        pass

    curl_command = ["curl", "-L", "-s", "--max-time", str(timeout), "-A", USER_AGENT]
    for name, value in (headers or {}).items():
        curl_command.extend(["-H", f"{name}: {value}"])
    curl_command.append(url)
    completed = subprocess.run(
        curl_command,
        check=False,
        capture_output=True,
        timeout=timeout + 5,
    )
    if completed.returncode != 0 or not completed.stdout:
        stderr = completed.stderr.decode("utf-8", errors="replace").strip()
        raise urllib.error.URLError(stderr or "curl fallback returned no content")
    return completed.stdout


def candidate_wfe_urls(today: dt.date | None = None) -> list[str]:
    today = today or dt.date.today()
    urls = []
    year = today.year
    month = today.month
    for _ in range(8):
        slug = f"{month_name[month].lower()}-{year}"
        urls.append(f"{WFE_BASE}/{slug}/market-statistics")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return urls


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def strip_markup(document: str) -> str:
    document = re.sub(r"(?is)<(script|style).*?</\1>", " ", document)
    document = re.sub(r"(?is)<br\s*/?>", "\n", document)
    document = re.sub(r"(?is)</(p|div|li|tr|td|th|h[1-6])>", "\n", document)
    document = re.sub(r"(?is)<[^>]+>", " ", document)
    return normalize_space(html.unescape(document))


def parse_source_date(value: str) -> dt.date:
    cleaned = normalize_space(value).replace(",", "")
    for fmt in ("%B %d %Y", "%b %d %Y", "%Y-%m-%d", "%B %Y"):
        try:
            parsed = dt.datetime.strptime(cleaned, fmt).date()
            if fmt == "%B %Y":
                return parsed.replace(day=1)
            return parsed
        except ValueError:
            continue
    raise ValueError(f"Could not parse source date: {value!r}")


def require_fresh_source(as_of: str, source_name: str, max_age_days: int = MAX_INDEX_SOURCE_AGE_DAYS) -> None:
    try:
        source_date = parse_source_date(as_of)
    except ValueError as exc:
        raise RuntimeError(f"{source_name} freshness could not be verified from as-of value {as_of!r}.") from exc
    age_days = (dt.date.today() - source_date).days
    if age_days > max_age_days:
        raise RuntimeError(
            f"{source_name} is stale: as of {as_of} is {age_days} days old; max age is {max_age_days} days."
        )


def build_sp_factsheet_url(index_id: str, host_identifier: str = SP_FACTSHEET_HOST_IDENTIFIER) -> str:
    query = urllib.parse.urlencode(
        {
            "calcFrequency": "M",
            "force_download": "true",
            "hostIdentifier": host_identifier,
            "indexId": index_id,
            "languageId": "1",
        }
    )
    return f"{SP_FACTSHEET_ENDPOINT}?{query}"


def parse_sp_factsheet_url(document: str, base_url: str) -> str | None:
    for match in re.finditer(r"""href=(?P<quote>["'])(?P<href>.*?idsenhancedfactsheet/file\.pdf.*?)\1""", document):
        href = html.unescape(match.group("href")).replace(r"\u0026", "&")
        return urllib.parse.urljoin(base_url, href)
    return None


def resolve_sp_factsheet_url(source: IndexSource) -> str:
    override_url = os.environ.get(source.factsheet_url_env)
    if override_url:
        return override_url

    try:
        factsheet_url = parse_sp_factsheet_url(fetch_text(source.url), source.url)
    except (urllib.error.URLError, TimeoutError):
        factsheet_url = None
    if factsheet_url:
        return factsheet_url

    if source.index_id:
        return build_sp_factsheet_url(source.index_id, source.host_identifier)
    return source.factsheet_url


def canonical_country(country: str) -> str:
    cleaned = normalize_space(country).strip(":-")
    return COUNTRY_ALIASES.get(cleaned, cleaned)


def parse_number(value: str) -> float:
    return float(value.replace(",", "").replace("%", "").strip())


def pdf_literal_to_text(value: bytes) -> str:
    output = bytearray()
    index = 0
    while index < len(value):
        char = value[index]
        if char == 92 and index + 1 < len(value):
            index += 1
            escaped = value[index]
            output.extend(
                {
                    ord("n"): b"\n",
                    ord("r"): b"\n",
                    ord("t"): b"\t",
                    ord("b"): b"\b",
                    ord("f"): b"\f",
                    ord("("): b"(",
                    ord(")"): b")",
                    ord("\\"): b"\\",
                }.get(escaped, bytes([escaped]))
            )
        else:
            output.append(char)
        index += 1
    return output.decode("latin-1", errors="replace")


def extract_pdf_text(pdf: bytes, require_country_breakdown: bool = True) -> str:
    if pdf.lstrip().startswith(b"<"):
        raise RuntimeError("S&P factsheet endpoint returned HTML instead of a PDF.")

    chunks: list[str] = []
    for match in re.finditer(rb"<<(?P<dict>.*?)>>\s*stream\r?\n(?P<body>.*?)\r?\nendstream", pdf, re.DOTALL):
        dictionary = match.group("dict")
        body = match.group("body")
        if b"FlateDecode" in dictionary:
            try:
                body = zlib.decompress(body)
            except zlib.error:
                continue
        elif b"Filter" in dictionary:
            continue

        strings = re.findall(rb"\((?:\\.|[^\\()])*\)\s*Tj", body)
        arrays = re.findall(rb"\[(.*?)\]\s*TJ", body, re.DOTALL)
        for item in strings:
            chunks.append(pdf_literal_to_text(item[1 : item.rfind(b")")]))
            chunks.append("\n")
        for array in arrays:
            for literal in re.findall(rb"\((?:\\.|[^\\()])*\)", array):
                chunks.append(pdf_literal_to_text(literal[1:-1]))
            chunks.append("\n")

    text = normalize_space(" ".join(chunks))
    if require_country_breakdown and "Country/Region Breakdown" not in text and "COUNTRY/REGION" not in text:
        raise RuntimeError("Could not extract country-breakdown text from S&P factsheet PDF.")
    return text


def parse_market_cap_label(value: str) -> float:
    match = re.search(r"\$?\s*([0-9,.]+)\s*([TMB])", value, re.IGNORECASE)
    if not match:
        raise ValueError(f"Could not parse market-cap label: {value!r}")
    number = parse_number(match.group(1))
    suffix = match.group(2).upper()
    multiplier = {"T": 1_000_000, "B": 1_000, "M": 1}[suffix]
    return number * multiplier


def check_source(source: SourceCheck) -> dict[str, Any]:
    try:
        text = strip_markup(fetch_text(source.url))
    except (urllib.error.URLError, TimeoutError) as exc:
        return {
            "name": source.name,
            "url": source.url,
            "ok": False,
            "missing_terms": source.expected_terms,
            "error": str(exc),
        }
    missing_terms = tuple(term for term in source.expected_terms if term.lower() not in text.lower())
    return {"name": source.name, "url": source.url, "ok": not missing_terms, "missing_terms": missing_terms}


def parse_sp_country_caps(source: IndexSource, document: str) -> dict[str, CountryCap]:
    text = strip_markup(document)
    pattern = re.compile(
        r"Country/Region\s+"
        r"(?P<country>[A-Za-z][A-Za-z .,&'-]+?)\s+"
        r"Number of Constituents\s+"
        r"(?P<constituents>[0-9,]+)\s+"
        r"Total Market Cap\s+"
        r"(?P<cap>[0-9,]+(?:\.[0-9]+)?)\s+"
        r"Index Weight\s+"
        r"(?P<weight>[0-9,]+(?:\.[0-9]+)?)%",
        re.IGNORECASE,
    )

    caps: dict[str, CountryCap] = {}
    for match in pattern.finditer(text):
        country = canonical_country(match.group("country"))
        caps[country] = CountryCap(
            country=country,
            constituents=int(match.group("constituents").replace(",", "")),
            market_cap_usd_millions=parse_number(match.group("cap")),
            index_weight_pct=parse_number(match.group("weight")),
            source=source.name,
        )

    if not caps:
        raise RuntimeError(
            f"Could not parse country market caps from {source.name}. "
            "The S&P DJI page may have changed layout or hidden the table."
        )
    return caps


def parse_sp_factsheet_country_caps(source: IndexSource, text: str) -> dict[str, CountryCap]:
    rows: dict[str, CountryCap] = {}
    names = "|".join(re.escape(name) for name in COUNTRY_ROW_NAMES)
    pattern = re.compile(
        rf"(?P<country>{names})\s+"
        r"(?P<constituents>[0-9,]+)\s+"
        r"(?P<cap>[0-9,]+(?:\.[0-9]+)?)\s+"
        r"(?P<weight>[0-9]+(?:\.[0-9]+)?)",
        re.IGNORECASE,
    )
    for match in pattern.finditer(text):
        country = canonical_country(match.group("country"))
        rows[country] = CountryCap(
            country=country,
            constituents=int(match.group("constituents").replace(",", "")),
            market_cap_usd_millions=parse_number(match.group("cap")),
            index_weight_pct=parse_number(match.group("weight")),
            source=f"{source.name} factsheet",
        )

    if not rows:
        raise RuntimeError(f"Could not parse country rows from {source.name} factsheet.")
    return rows


def parse_sp_factsheet_as_of(text: str) -> str:
    match = re.search(r"AS OF\s+([A-Z]+\s+[0-9]{1,2},\s+[0-9]{4})", text)
    if not match:
        return "latest available S&P factsheet"
    return match.group(1).title()


def parse_msc_factsheet_url(document: str) -> str | None:
    match = re.search(r"https://www\.msci\.com/documents/10199/[A-Za-z0-9-]+", html.unescape(document))
    if match:
        return match.group(0)
    return None


def parse_msc_as_of(text: str) -> str:
    matches = re.findall(r"\b([A-Z]{3}\s+[0-9]{1,2},\s+[0-9]{4})\b", text)
    dated_matches = []
    for value in matches:
        try:
            dated_matches.append((dt.datetime.strptime(value.title(), "%b %d, %Y").date(), value))
        except ValueError:
            continue
    if not dated_matches:
        raise RuntimeError("Could not parse MSCI factsheet as-of date.")
    _, latest = max(dated_matches)
    month_abbr, day, year = re.match(r"([A-Z]{3})\s+([0-9]{1,2}),\s+([0-9]{4})", latest).groups()
    month = dt.datetime.strptime(month_abbr.title(), "%b").strftime("%B")
    return f"{month} {int(day)}, {year}"


def parse_msc_acwi_imi_market_cap(text: str) -> float:
    cap_match = re.search(r"Mkt Cap\s*\(\s*USD Millions\)\s*Index\s+([0-9,]+(?:\.[0-9]+)?)", text)
    if not cap_match:
        raise RuntimeError("Could not parse MSCI ACWI IMI index market cap.")
    return parse_number(cap_match.group(1))


def parse_msc_acwi_imi_country_caps(text: str) -> dict[str, CountryCap]:
    denominator = parse_msc_acwi_imi_market_cap(text)
    country_section = re.search(r"COUNTRY WEIGHTS\s+(.*?)MAR\s+[0-9]{1,2},\s+[0-9]{4}", text, re.DOTALL)
    if not country_section:
        raise RuntimeError("Could not parse MSCI ACWI IMI country weights.")
    names = "|".join(re.escape(name) for name in COUNTRY_ROW_NAMES)
    rows: dict[str, CountryCap] = {}
    for match in re.finditer(rf"(?P<country>{names})\s+(?P<weight>[0-9]+(?:\.[0-9]+)?)%", country_section.group(1)):
        country = canonical_country(match.group("country"))
        weight = parse_number(match.group("weight"))
        rows[country] = CountryCap(
            country=country,
            constituents=None,
            market_cap_usd_millions=denominator * (weight / 100),
            index_weight_pct=weight,
            source="MSCI ACWI IMI factsheet",
        )
    if not rows:
        raise RuntimeError("Could not parse MSCI ACWI IMI country rows.")
    return rows


def fetch_factsheet_text(source: IndexSource) -> str:
    factsheet_path = os.environ.get(source.factsheet_file_env)
    if factsheet_path:
        with open(factsheet_path, "rb") as file:
            pdf = file.read()
    else:
        pdf = fetch_bytes(resolve_sp_factsheet_url(source), headers={"Referer": source.url})
    return extract_pdf_text(pdf)


def fetch_factsheet_country_caps(source: IndexSource) -> dict[str, CountryCap]:
    return parse_sp_factsheet_country_caps(source, fetch_factsheet_text(source))


def fetch_sp_global_bmi_page_text() -> str:
    return strip_markup(fetch_text(GLOBAL_INDEX_SOURCE.url))


def fetch_msci_acwi_imi_dataset() -> MarketCapDataset:
    document = fetch_text(MSCI_ACWI_IMI_PAGE_URL)
    factsheet_url = parse_msc_factsheet_url(document)
    if not factsheet_url:
        raise RuntimeError("Could not find MSCI ACWI IMI factsheet link.")
    pdf = fetch_bytes(factsheet_url, headers={"Referer": MSCI_ACWI_IMI_PAGE_URL})
    text = extract_pdf_text(pdf, require_country_breakdown=False)
    as_of = parse_msc_as_of(text)
    require_fresh_source(as_of, "MSCI ACWI IMI factsheet")
    country_caps = parse_msc_acwi_imi_country_caps(text)
    return MarketCapDataset(
        country_caps=country_caps,
        denominator_usd_millions=parse_msc_acwi_imi_market_cap(text),
        source_label="MSCI ACWI IMI country weights",
        freshness_label=f"MSCI ACWI IMI factsheet as of {as_of}; full country coverage required before use.",
        source_url=factsheet_url,
        share_basis="index_weight",
    )


def fetch_country_caps(sources: Iterable[IndexSource] = INDEX_SOURCES) -> dict[str, CountryCap]:
    country_caps: dict[str, CountryCap] = {}
    for source in sources:
        try:
            document = fetch_text(source.url)
        except (urllib.error.URLError, TimeoutError) as exc:
            document_error = exc
        else:
            document_error = None
            try:
                parsed = parse_sp_country_caps(source, document)
                country_caps.update(parsed)
                continue
            except RuntimeError as exc:
                document_error = exc

        try:
            parsed = fetch_factsheet_country_caps(source)
        except (RuntimeError, urllib.error.URLError, TimeoutError) as exc:
            raise RuntimeError(
                f"Could not fetch {source.name} HTML or factsheet data. "
                f"HTML error: {document_error}. Factsheet error: {exc}"
            ) from exc
        country_caps.update(parsed)
    return country_caps


def load_sp_global_bmi_snapshot(path: Path = SP_GLOBAL_BMI_SNAPSHOT_PATH) -> tuple[dict[str, CountryCap], str]:
    rows: dict[str, CountryCap] = {}
    as_of = "March 31, 2026"
    with path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames:
            for field in reader.fieldnames:
                if field.startswith("as_of="):
                    as_of = field.removeprefix("as_of=")
        for row in reader:
            country = canonical_country(row["country"])
            rows[country] = CountryCap(
                country=country,
                constituents=int(row["constituents"].replace(",", "")),
                market_cap_usd_millions=parse_number(row["total_market_cap_usd_millions"]),
                index_weight_pct=parse_number(row["index_weight_pct"]),
                source="S&P Global BMI snapshot",
            )
    if not rows:
        raise RuntimeError("S&P Global BMI snapshot contains no country rows.")
    return rows, as_of


def build_sp_global_bmi_dataset(
    country_caps: dict[str, CountryCap],
    freshness_label: str,
    source_label: str = "S&P Global BMI country index weights",
) -> MarketCapDataset:
    return MarketCapDataset(
        country_caps=country_caps,
        denominator_usd_millions=sum(cap.market_cap_usd_millions for cap in country_caps.values()),
        source_label=source_label,
        freshness_label=freshness_label,
        source_url=GLOBAL_INDEX_SOURCE.url,
        share_basis="index_weight",
    )


def fetch_sp_global_bmi_dataset() -> MarketCapDataset:
    errors = []
    try:
        text = fetch_sp_global_bmi_page_text()
        country_caps = parse_sp_factsheet_country_caps(GLOBAL_INDEX_SOURCE, text)
        as_of = parse_sp_factsheet_as_of(text)
        require_fresh_source(as_of, "S&P Global BMI index page")
        return build_sp_global_bmi_dataset(
            country_caps,
            f"S&P Global BMI index page as of {as_of}; shares use country Index Weight [%].",
        )
    except (RuntimeError, urllib.error.URLError, TimeoutError) as exc:
        errors.append(f"index page: {exc}")

    try:
        text = fetch_factsheet_text(GLOBAL_INDEX_SOURCE)
        country_caps = parse_sp_factsheet_country_caps(GLOBAL_INDEX_SOURCE, text)
        as_of = parse_sp_factsheet_as_of(text)
        require_fresh_source(as_of, "S&P Global BMI factsheet")
        return build_sp_global_bmi_dataset(
            country_caps,
            f"S&P Global BMI factsheet as of {as_of}; shares use country Index Weight [%].",
        )
    except (RuntimeError, urllib.error.URLError, TimeoutError) as exc:
        errors.append(f"factsheet PDF: {exc}")

    if os.environ.get(SP_ALLOW_STALE_SNAPSHOT_ENV) == "1":
        country_caps, as_of = load_sp_global_bmi_snapshot()
        return build_sp_global_bmi_dataset(
            country_caps,
            (
                f"Snapshot from S&P Global BMI factsheet as of {as_of}; "
                "live S&P PDF fetch was blocked for this run."
            ),
            "S&P Global BMI country index weights (snapshot fallback)",
        )
    raise RuntimeError(
        "Could not fetch live S&P Global BMI country data from the index page or factsheet PDF. "
        f"Set {SP_ALLOW_STALE_SNAPSHOT_ENV}=1 only if you explicitly want the stale checked-in snapshot. "
        + " | ".join(errors)
    )


def parse_cmc_global_market_cap(document: str) -> float:
    text = strip_markup(document)
    match = re.search(r"total market cap:\s*\$?\s*([0-9,.]+\s*[TMB])", text, re.IGNORECASE)
    if not match:
        raise RuntimeError("Could not parse CompaniesMarketCap global denominator.")
    return parse_market_cap_label(match.group(1))


def parse_cmc_all_countries(document: str) -> dict[str, CountryCap]:
    row_pattern = re.compile(
        r'<tr><td class="rank-td td-right" data-sort="[0-9]+">[0-9]+</td>'
        r'<td data-sort="(?P<country>[^"]+)">.*?</td>'
        r'<td class="td-right" data-sort="(?P<cap>[0-9]+)">.*?</td>'
        r'<td class="td-right" data-sort="(?P<constituents>[0-9]+)">',
        re.DOTALL,
    )
    caps = {}
    for match in row_pattern.finditer(document):
        country = canonical_country(match.group("country"))
        caps[country] = CountryCap(
            country=country,
            constituents=int(match.group("constituents")),
            market_cap_usd_millions=float(match.group("cap")) / 1_000_000,
            index_weight_pct=None,
            source="CompaniesMarketCap",
        )
    if not caps:
        raise RuntimeError("Could not parse CompaniesMarketCap all-countries table.")
    return caps


def fetch_cmc_dataset() -> MarketCapDataset:
    url = f"{CMC_BASE}/all-countries/"
    document = fetch_text(url)
    denominator = parse_cmc_global_market_cap(document)
    needed_countries = sorted({country for config in COUNTRY_GROUPS.values() for country in config["countries"]})
    all_country_caps = parse_cmc_all_countries(document)
    country_caps = {country: all_country_caps[country] for country in needed_countries if country in all_country_caps}
    return MarketCapDataset(
        country_caps=country_caps,
        denominator_usd_millions=denominator,
        source_label="CompaniesMarketCap all-countries table",
        freshness_label="Live page values; stock prices may be delayed by minutes to hours.",
        source_url=url,
    )


def parse_wfe_period(document: str, url: str) -> str:
    title_match = re.search(r"Market Statistics\s*-\s*([A-Za-z]+\s+[0-9]{4})", document)
    if title_match:
        return title_match.group(1)
    slug_match = re.search(r"/issue/([a-z]+)-([0-9]{4})/market-statistics", url)
    if slug_match:
        return f"{slug_match.group(1).title()} {slug_match.group(2)}"
    return "latest available WFE issue"


def parse_wfe_market_caps(document: str) -> dict[str, CountryCap]:
    start = document.find("Equity - Domestic market capitalisation")
    end = document.find("Equity - Number of listed companies", start)
    if start != -1 and end != -1:
        document = document[start:end]
    region_markers = ("Americas", "APAC", "EMEA")
    stop_markers = ("Total for Americas", "Total for APAC", "Total for EMEA")
    row_pattern = re.compile(
        r'<tr[^>]*>\s*<td[^>]*>(?P<exchange>.*?)</td>'
        r'<td[^>]*>(?P<previous>.*?)</td>'
        r'<td[^>]*>(?P<current>.*?)</td>',
        re.DOTALL,
    )
    caps: dict[str, CountryCap] = {}
    for match in row_pattern.finditer(document):
        exchange = strip_markup(match.group("exchange"))
        if not exchange or exchange in region_markers or exchange.startswith(stop_markers):
            continue
        countries = WFE_EXCHANGE_COUNTRIES.get(exchange)
        if not countries:
            continue
        try:
            value = parse_number(strip_markup(match.group("current")))
        except ValueError:
            continue
        for country in countries:
            existing = caps.get(country)
            caps[country] = CountryCap(
                country=country,
                constituents=None,
                market_cap_usd_millions=value + (existing.market_cap_usd_millions if existing else 0),
                index_weight_pct=None,
                source="WFE monthly domestic market capitalisation",
            )
    if not caps:
        raise RuntimeError("Could not parse WFE domestic market-cap table.")
    return caps


def fetch_wfe_country_caps() -> tuple[dict[str, CountryCap], str, str]:
    errors = []
    for url in candidate_wfe_urls():
        try:
            document = fetch_text(url)
            return parse_wfe_market_caps(document), parse_wfe_period(document, url), url
        except (RuntimeError, urllib.error.URLError, TimeoutError) as exc:
            errors.append(f"{url}: {exc}")
    raise RuntimeError("Could not fetch a current WFE market-statistics page. " + " | ".join(errors))


def fetch_wfe_hybrid_dataset() -> MarketCapDataset:
    wfe_caps, wfe_period, wfe_url = fetch_wfe_country_caps()
    cmc_dataset = fetch_cmc_dataset()
    needed_countries = sorted({country for config in COUNTRY_GROUPS.values() for country in config["countries"]})
    country_caps = {}
    for country in needed_countries:
        if country in cmc_dataset.country_caps:
            country_caps[country] = cmc_dataset.country_caps[country]
        elif country in wfe_caps:
            country_caps[country] = wfe_caps[country]
    return MarketCapDataset(
        country_caps=country_caps,
        denominator_usd_millions=cmc_dataset.denominator_usd_millions,
        source_label="CompaniesMarketCap all-countries table with WFE missing-country gap-fill",
        freshness_label=(
            f"Live CompaniesMarketCap values; WFE missing-country gap-fill rows from {wfe_period}."
        ),
        source_url=cmc_dataset.source_url or wfe_url,
    )


def require_dataset_reconciliation(dataset: MarketCapDataset) -> None:
    metrics = reconciliation_metrics(
        compile_groups(dataset.country_caps, dataset.denominator_usd_millions, dataset.share_basis),
        dataset.country_caps,
        dataset.denominator_usd_millions,
        dataset.share_basis,
    )
    if not reconciliation_passes(metrics):
        raise RuntimeError(
            f"{dataset.source_label} failed reconciliation: groups sum to {metrics['group_share_sum_pct']:.2f}%; "
            f"de-duped coverage is {metrics['unique_country_share_pct']:.2f}%."
        )


def fetch_market_cap_dataset() -> MarketCapDataset:
    try:
        dataset = fetch_sp_global_bmi_dataset()
        require_dataset_reconciliation(dataset)
        return dataset
    except RuntimeError as exc:
        print(f"warning: S&P Global BMI source unavailable; trying MSCI ACWI IMI. {exc}", file=sys.stderr)
    try:
        dataset = fetch_msci_acwi_imi_dataset()
        require_dataset_reconciliation(dataset)
        return dataset
    except RuntimeError as exc:
        print(f"warning: MSCI ACWI IMI source unavailable; trying legacy S&P BMI sources. {exc}", file=sys.stderr)
    try:
        country_caps = fetch_country_caps()
        return MarketCapDataset(
            country_caps=country_caps,
            denominator_usd_millions=sum(cap.market_cap_usd_millions for cap in country_caps.values()),
            source_label="S&P Developed BMI + S&P Emerging BMI country Total Market Cap rows",
            freshness_label="Latest S&P BMI page or factsheet available at run time.",
            source_url="https://www.spglobal.com/spdji/en/indices/equity/sp-global-bmi/",
        )
    except RuntimeError as exc:
        print(f"warning: S&P BMI source unavailable; trying WFE hybrid fallback. {exc}", file=sys.stderr)
    try:
        wfe_dataset = fetch_wfe_hybrid_dataset()
        metrics = reconciliation_metrics(
            compile_groups(wfe_dataset.country_caps, wfe_dataset.denominator_usd_millions, wfe_dataset.share_basis),
            wfe_dataset.country_caps,
            wfe_dataset.denominator_usd_millions,
            wfe_dataset.share_basis,
        )
        if not reconciliation_passes(metrics):
            print(
                "warning: WFE hybrid fallback failed reconciliation "
                f"(groups sum to {metrics['group_share_sum_pct']:.2f}%; "
                f"de-duped coverage is {metrics['unique_country_share_pct']:.2f}%). "
                "Falling back to CompaniesMarketCap for source consistency.",
                file=sys.stderr,
            )
            return fetch_cmc_dataset()
        return wfe_dataset
    except RuntimeError as exc:
        print(f"warning: WFE fallback unavailable; falling back to CompaniesMarketCap. {exc}", file=sys.stderr)
        return fetch_cmc_dataset()


def compile_groups(
    country_caps: dict[str, CountryCap],
    global_market_cap: float,
    share_basis: str = "market_cap",
) -> list[GroupResult]:
    if global_market_cap <= 0:
        raise RuntimeError("Global market cap denominator is zero.")

    results: list[GroupResult] = []
    for label, config in COUNTRY_GROUPS.items():
        countries = tuple(config["countries"])
        present = [country_caps[country] for country in countries if country in country_caps]
        missing = tuple(
            country
            for country in countries
            if country not in country_caps
            or (share_basis == "index_weight" and country_caps[country].index_weight_pct is None)
        )
        present_caps = [cap.market_cap_usd_millions for cap in present]
        group_cap = sum(present_caps)
        if share_basis == "index_weight":
            share = sum(cap.index_weight_pct or 0 for cap in present)
        else:
            share = (group_cap / global_market_cap) * 100
        results.append(
            GroupResult(
                label=label,
                ticker=str(config["ticker"]),
                countries=countries,
                market_cap_usd_millions=group_cap,
                share_of_global_pct=share,
                missing_countries=missing,
            )
        )
    return results


def country_group_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    for config in COUNTRY_GROUPS.values():
        for country in config["countries"]:
            counts[country] = counts.get(country, 0) + 1
    return counts


def reconciliation_metrics(
    results: list[GroupResult],
    country_caps: dict[str, CountryCap],
    global_market_cap: float,
    share_basis: str = "market_cap",
) -> dict[str, float | str]:
    unique_countries = sorted({country for result in results for country in result.countries if country in country_caps})
    group_share_sum = sum(result.share_of_global_pct for result in results)
    if share_basis == "index_weight":
        unique_share = sum(country_caps[country].index_weight_pct or 0 for country in unique_countries)
    else:
        unique_cap = sum(country_caps[country].market_cap_usd_millions for country in unique_countries)
        unique_share = (unique_cap / global_market_cap) * 100 if global_market_cap else 0
    duplicated_share = group_share_sum - unique_share
    if reconciliation_passes(
        {
            "group_share_sum_pct": group_share_sum,
            "unique_country_share_pct": unique_share,
            "duplicated_overlap_pct": duplicated_share,
        }
    ):
        note = (
            f"Shares sum to {group_share_sum:.2f}%; after de-duplicating overlapping ETF countries, "
            f"coverage is {unique_share:.2f}%."
        )
    else:
        note = (
            f"Warning: shares sum to {group_share_sum:.2f}%; after de-duplicating overlapping ETF countries, "
            f"coverage is {unique_share:.2f}%. This suggests the source/denominator mix is not comparable."
        )
    return {
        "group_share_sum_pct": group_share_sum,
        "unique_country_share_pct": unique_share,
        "duplicated_overlap_pct": duplicated_share,
        "note": note,
    }


def reconciliation_passes(metrics: dict[str, float | str]) -> bool:
    return (
        float(metrics["group_share_sum_pct"]) <= 106
        and 90 <= float(metrics["unique_country_share_pct"]) <= 103
    )


def usd_millions_to_trillions(value: float) -> float:
    return value / 1_000_000


def render_table(results: list[GroupResult], global_market_cap: float, source_label: str) -> str:
    rows = [
        (
            "Geography",
            "ETF",
            "Market cap (USD tn)",
            "Share of global",
            "Countries used",
            "Missing countries",
        )
    ]
    for result in results:
        rows.append(
            (
                result.label,
                result.ticker,
                f"{usd_millions_to_trillions(result.market_cap_usd_millions):,.2f}",
                f"{result.share_of_global_pct:,.2f}%",
                str(len(result.countries) - len(result.missing_countries)),
                ", ".join(result.missing_countries) or "-",
            )
        )

    total_market_cap = sum(result.market_cap_usd_millions for result in results)
    total_share = sum(result.share_of_global_pct for result in results)
    rows.append(
        (
            "Totals",
            "-",
            f"{usd_millions_to_trillions(total_market_cap):,.2f}",
            f"{total_share:,.2f}%",
            "-",
            "-",
        )
    )

    widths = [max(len(row[index]) for row in rows) for index in range(len(rows[0]))]
    rendered = []
    for row_index, row in enumerate(rows):
        rendered.append("  ".join(cell.ljust(widths[index]) for index, cell in enumerate(row)))
        if row_index == 0:
            rendered.append("  ".join("-" * width for width in widths))

    note = (
        f"\nGlobal denominator: USD {usd_millions_to_trillions(global_market_cap):,.2f} tn "
        f"({source_label})."
    )
    return "\n".join(rendered) + note


def confidence_line(results: list[GroupResult], source_label: str) -> str:
    missing = sorted({country for result in results for country in result.missing_countries})
    if source_label.startswith("S&P Global BMI"):
        if missing:
            return f"Data quality: index-grade country index weights with missing countries: {', '.join(missing)}."
        return "Data quality: index-grade country index weights; no missing countries."
    if source_label.startswith("S&P"):
        if missing:
            return f"Data quality: index-grade source with missing countries: {', '.join(missing)}."
        return "Data quality: index-grade source; no missing countries."
    if source_label.startswith("WFE"):
        if missing:
            return (
                "Data quality: WFE monthly exchange-reported fallback with CompaniesMarketCap gap-fill. "
                f"Missing countries: {', '.join(missing)}."
            )
        return (
            "Data quality: WFE monthly exchange-reported fallback with CompaniesMarketCap gap-fill; "
            "no missing countries."
        )
    if missing:
        return (
            "Data quality: fallback estimate, not index-grade. "
            f"Missing countries: {', '.join(missing)}."
        )
    return "Data quality: fallback estimate, not index-grade; no missing countries."


def build_payload(
    source_checks: list[dict[str, Any]],
    results: list[GroupResult],
    dataset: MarketCapDataset,
) -> dict[str, Any]:
    overlaps = country_group_counts()
    cap_lookup = dataset.country_caps
    reconciliation = reconciliation_metrics(results, cap_lookup, dataset.denominator_usd_millions, dataset.share_basis)
    return {
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "methodology": {
            "market_cap_source": dataset.source_label,
            "denominator_usd_millions": dataset.denominator_usd_millions,
            "overlap_allowed": True,
            "data_quality": confidence_line(results, dataset.source_label),
            "freshness": dataset.freshness_label,
            "source_url": dataset.source_url,
            "share_basis": dataset.share_basis,
            "reconciliation": reconciliation,
        },
        "source_checks": source_checks,
        "groups": [
            {
                "label": result.label,
                "ticker": result.ticker,
                "countries": result.countries,
                "market_cap_usd_millions": result.market_cap_usd_millions,
                "share_of_global_pct": result.share_of_global_pct,
                "missing_countries": result.missing_countries,
                "country_details": [
                    {
                        "country": country,
                        "market_cap_usd_millions": cap_lookup[country].market_cap_usd_millions
                        if country in cap_lookup
                        else None,
                        "source": cap_lookup[country].source if country in cap_lookup else None,
                        "included": country in cap_lookup,
                        "appears_in_multiple_groups": overlaps.get(country, 0) > 1,
                    }
                    for country in result.countries
                ],
            }
            for result in results
        ],
    }


def run(json_output: bool = False) -> int:
    checks = [check_source(source) for source in (*ETF_SOURCE_CHECKS, *PROVIDER_SOURCE_CHECKS)]
    failed_checks = [check for check in checks if not check["ok"]]
    if failed_checks:
        details = "\n".join(
            f"- {check['name']}: missing {', '.join(check['missing_terms'])} "
            f"{'[' + check['error'] + '] ' if check.get('error') else ''}({check['url']})"
            for check in failed_checks
        )
        raise RuntimeError(f"One or more methodology source checks failed:\n{details}")

    dataset = fetch_market_cap_dataset()
    results = compile_groups(dataset.country_caps, dataset.denominator_usd_millions, dataset.share_basis)
    payload = build_payload(checks, results, dataset)

    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(render_table(results, dataset.denominator_usd_millions, dataset.source_label))
        print(f"\nMarket-cap source: {dataset.source_label}")
        print(confidence_line(results, dataset.source_label))
        print("\nSource checks:")
        for check in checks:
            print(f"- OK: {check['name']} ({check['url']})")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Return ETF-shaped shares of global public-equity market cap.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """\
            Examples:
              python3 -m src.market_caps
              python3 -m src.market_caps --json
            """
        ),
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        return run(json_output=args.json)
    except (RuntimeError, urllib.error.URLError, TimeoutError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
