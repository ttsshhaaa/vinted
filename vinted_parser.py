import argparse
import csv
import json
import os
import re
import time
import unicodedata
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

API_HEADERS = {
    **DEFAULT_HEADERS,
    "Accept": "application/json, text/plain, */*",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

SEARCH_MODE = os.environ.get("SEARCH_MODE", "lite").strip().lower() or "lite"
DETAIL_CACHE_TTL_SECONDS = int(os.environ.get("DETAIL_CACHE_TTL_SECONDS", "21600"))
GEO_COOLDOWN_SECONDS = int(os.environ.get("GEO_COOLDOWN_SECONDS", "1800"))
DETAIL_CACHE: dict[str, tuple[float, tuple[str, str, str, int | None]]] = {}
GEO_COOLDOWNS: dict[str, float] = {}

GEO_DOMAINS = {
    "us": "https://www.vinted.com",
    "uk": "https://www.vinted.co.uk",
    "fr": "https://www.vinted.fr",
    "de": "https://www.vinted.de",
    "it": "https://www.vinted.it",
    "es": "https://www.vinted.es",
    "nl": "https://www.vinted.nl",
    "be": "https://www.vinted.be",
    "pt": "https://www.vinted.pt",
    "pl": "https://www.vinted.pl",
    "cz": "https://www.vinted.cz",
    "sk": "https://www.vinted.sk",
    "at": "https://www.vinted.at",
    "hu": "https://www.vinted.hu",
    "ro": "https://www.vinted.ro",
    "hr": "https://www.vinted.hr",
    "lt": "https://www.vinted.lt",
    "ee": "https://www.vinted.ee",
    "lu": "https://www.vinted.lu",
    "lv": "https://www.vinted.lv",
    "se": "https://www.vinted.se",
    "si": "https://www.vinted.si",
    "dk": "https://www.vinted.dk",
    "fi": "https://www.vinted.fi",
    "gr": "https://www.vinted.gr",
    "ie": "https://www.vinted.ie",
}

GEO_ALLOWED_COUNTRIES = {
    "us": {"united states", "usa", "etats-unis", "états-unis"},
    "uk": {"united kingdom", "great britain", "royaume-uni"},
    "fr": {"france"},
    "de": {"deutschland", "germany"},
    "it": {"italia", "italy"},
    "es": {"españa", "espana", "spain"},
    "nl": {"nederland", "netherlands"},
    "be": {"belgië / belgique", "belgie / belgique", "belgique", "belgië", "belgie"},
    "pt": {"portugal"},
    "pl": {"polska", "poland"},
    "cz": {"česko", "cesko", "czechia", "czech republic"},
    "sk": {"slovensko", "slovakia"},
    "at": {"österreich", "osterreich", "austria"},
    "hu": {"magyarország", "magyarorszag", "hungary"},
    "ro": {"românia", "romania"},
    "hr": {"hrvatska", "croatia"},
    "lt": {"lietuva", "lithuania"},
    "ee": {"eesti", "estonia"},
    "lu": {"luxembourg", "luxemburg"},
    "lv": {"latvija", "latvia"},
    "se": {"sverige", "sweden"},
    "si": {"slovenija", "slovenia"},
    "dk": {"danmark", "denmark"},
    "fi": {"suomi", "finland"},
    "gr": {"ellada", "greece"},
    "ie": {"ireland", "eir"},
}

GEO_ALLOWED_COUNTRIES = {
    geo: {
        "".join(char for char in unicodedata.normalize("NFKD", alias) if not unicodedata.combining(char))
        .replace("\\/", "/")
        .casefold()
        for alias in aliases
    }
    for geo, aliases in GEO_ALLOWED_COUNTRIES.items()
}

GEO_RELATED_MARKETS = {
    "uk": {"ie", "fr"},
    "fr": {"be", "nl", "lu", "es", "pt", "it", "de", "ie"},
    "de": {"at", "nl", "be", "lu", "fr", "dk", "pl", "cz"},
    "it": {"fr", "es", "pt", "si", "hr", "at"},
    "es": {"pt", "fr", "it"},
    "nl": {"be", "lu", "fr", "de", "dk"},
    "be": {"nl", "fr", "lu", "de"},
    "pt": {"es", "fr", "it"},
    "pl": {"cz", "sk", "de", "lt", "lv", "dk"},
    "cz": {"sk", "pl", "de", "at", "hu"},
    "sk": {"cz", "pl", "hu", "at"},
    "at": {"de", "cz", "sk", "hu", "it", "si"},
    "hu": {"sk", "at", "ro", "hr", "si", "cz"},
    "ro": {"hu", "bg", "gr"},
    "hr": {"si", "hu", "it"},
    "lt": {"lv", "ee", "pl"},
    "ee": {"lv", "lt", "fi"},
    "lu": {"fr", "be", "de", "nl"},
    "lv": {"lt", "ee", "pl"},
    "se": {"dk", "fi", "de"},
    "si": {"hr", "at", "it", "hu"},
    "dk": {"se", "fi", "de", "nl", "pl"},
    "fi": {"se", "ee", "dk"},
    "gr": {"ro", "it"},
    "ie": {"uk", "fr"},
}


@dataclass
class Item:
    geo: str
    item_id: str
    title: str
    subtitle: str
    brand: str
    size: str
    condition: str
    price: str
    total_price: str
    currency: str
    image_url: str
    item_url: str
    search_url: str
    seller_country: str = ""
    seller_city: str = ""
    seller_last_online: str = ""
    listing_age_minutes: int | None = None
    listing_age_label: str = ""


AGE_PATTERNS = [
    (re.compile(r"(\d+)\s*(?:minute|minutes|min)\b", re.IGNORECASE), 1),
    (re.compile(r"(\d+)\s*(?:heure|heures|hour|hours)\b", re.IGNORECASE), 60),
    (re.compile(r"(\d+)\s*(?:jour|jours|day|days)\b", re.IGNORECASE), 1440),
    (re.compile(r"(\d+)\s*(?:semaine|semaines|week|weeks)\b", re.IGNORECASE), 10080),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Multi-geo Vinted parser that extracts catalog items from public search pages."
    )
    parser.add_argument("--query", required=True, help="Search text, for example: nike tech fleece")
    parser.add_argument(
        "--geo",
        default="all",
        help="Comma-separated geo codes like fr,de,it or 'all'.",
    )
    parser.add_argument("--pages", type=int, default=1, help="Pages per geo.")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests per geo.")
    parser.add_argument(
        "--order",
        default="newest_first",
        help="Catalog ordering, for example newest_first, relevance, price_low_to_high.",
    )
    parser.add_argument("--price-from", dest="price_from", type=int, help="Minimum price.")
    parser.add_argument("--price-to", dest="price_to", type=int, help="Maximum price.")
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        help="Extra raw query param in key=value format. Can be repeated.",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory where JSON and CSV exports will be written.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Request timeout in seconds.",
    )
    return parser.parse_args()


def parse_extra_params(raw_params: Iterable[str]) -> dict[str, str]:
    params: dict[str, str] = {}
    for raw in raw_params:
        if "=" not in raw:
            raise ValueError(f"Invalid --param value '{raw}'. Expected key=value.")
        key, value = raw.split("=", 1)
        params[key] = value
    return params


def expand_geos(raw_geo: str | Iterable[str]) -> list[str]:
    if isinstance(raw_geo, str):
        if raw_geo.strip().lower() == "all":
            return list(GEO_DOMAINS.keys())
        geos = [part.strip().lower() for part in raw_geo.split(",") if part.strip()]
    else:
        geos = [str(part).strip().lower() for part in raw_geo if str(part).strip()]

    unknown = [geo for geo in geos if geo not in GEO_DOMAINS]
    if unknown:
        raise ValueError(
            f"Unknown geo(s): {', '.join(unknown)}. Supported: {', '.join(sorted(GEO_DOMAINS))}"
        )
    return geos


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def normalize_search_text(value: str) -> str:
    normalized = clean_text(value)
    normalized = unicodedata.normalize("NFKD", normalized)
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = re.sub(r"[^a-zA-Z0-9]+", " ", normalized)
    return clean_text(normalized).casefold()


def normalize_country_name(value: str) -> str:
    normalized = clean_text(value).replace("\\/", "/")
    normalized = unicodedata.normalize("NFKD", normalized)
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    return normalized.casefold()


def format_age_label(minutes: int | None) -> str:
    if minutes is None:
        return ""
    if minutes < 60:
        return f"{minutes} min ago"
    if minutes < 1440:
        return f"{minutes // 60} h ago"
    if minutes < 10080:
        return f"{minutes // 1440} d ago"
    return f"{minutes // 10080} w ago"


def format_last_online(value: str) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value
    return parsed.strftime("%Y-%m-%d %H:%M %z")


def extract_currency(*values: str) -> str:
    joined = " ".join(filter(None, values))
    match = re.search(r"([$€£]|PLN|CZK|HUF|RON|USD|EUR|GBP)", joined, flags=re.IGNORECASE)
    return match.group(1).upper() if match else ""


def split_subtitle(subtitle: str) -> tuple[str, str]:
    parts = [part.strip() for part in re.split(r"\s*[·•]\s*", subtitle) if part.strip()]
    size = parts[0] if len(parts) >= 1 else ""
    condition = parts[-1] if len(parts) >= 2 else ""
    return size, condition


def token_matches_query_token(query_token: str, candidate_tokens: list[str]) -> bool:
    for candidate in candidate_tokens:
        if query_token == candidate:
            return True
        if len(query_token) >= 4 and (query_token in candidate or candidate in query_token):
            return True
        if len(query_token) >= 4 and SequenceMatcher(None, query_token, candidate).ratio() >= 0.86:
            return True
    return False


def has_query_token_hit(query: str, haystack: str) -> bool:
    query_tokens = [token for token in normalize_search_text(query).split() if len(token) >= 2]
    haystack_tokens = [token for token in normalize_search_text(haystack).split() if len(token) >= 2]
    if not query_tokens or not haystack_tokens:
        return False
    return any(token_matches_query_token(query_token, haystack_tokens) for query_token in query_tokens)


def query_match_score(item: "Item", query: str) -> float:
    normalized_query = normalize_search_text(query)
    if not normalized_query:
        return 1.0

    title_text = normalize_search_text(item.title)
    brand_text = normalize_search_text(item.brand)
    subtitle_text = normalize_search_text(item.subtitle)
    haystack = " ".join(part for part in (title_text, brand_text, subtitle_text) if part).strip()
    if not haystack:
        return 0.0
    if normalized_query in haystack:
        return 1.0

    query_tokens = [token for token in normalized_query.split() if len(token) >= 2]
    haystack_tokens = [token for token in haystack.split() if len(token) >= 2]
    if not query_tokens or not haystack_tokens:
        return 0.0

    matched_tokens = sum(
        1 for query_token in query_tokens if token_matches_query_token(query_token, haystack_tokens)
    )
    token_ratio = matched_tokens / len(query_tokens)
    similarity = SequenceMatcher(None, normalized_query, haystack).ratio()
    title_similarity = SequenceMatcher(None, normalized_query, title_text).ratio() if title_text else 0.0

    if len(query_tokens) == 1:
        return max(token_ratio, similarity, title_similarity)
    return max(token_ratio, similarity * 0.75, title_similarity * 0.9)


def item_matches_query_text(item: "Item", query: str) -> bool:
    normalized_query = normalize_search_text(query)
    if not normalized_query:
        return True

    query_tokens = [token for token in normalized_query.split() if len(token) >= 2]
    score = query_match_score(item, query)
    haystack = " ".join(part for part in (item.title, item.brand, item.subtitle) if part).strip()
    if len(query_tokens) <= 1:
        return score >= 0.5 or has_query_token_hit(query, haystack)
    if len(query_tokens) == 2:
        return score >= 0.36 or has_query_token_hit(query, haystack)
    return score >= 0.3 or has_query_token_hit(query, haystack)


def build_search_url(
    base_url: str,
    query: str,
    page: int,
    order: str,
    price_from: int | None,
    price_to: int | None,
    extra_params: dict[str, str],
) -> str:
    params = {
        "search_text": query,
        "page": str(page),
        "order": order,
    }
    if price_from is not None:
        params["price_from"] = str(price_from)
    if price_to is not None:
        params["price_to"] = str(price_to)
    params.update(extra_params)
    return f"{base_url}/catalog?{urlencode(params, doseq=True)}"


def build_catalog_api_url(base_url: str) -> str:
    return f"{base_url}/api/v2/catalog/items"


def bootstrap_session(session: requests.Session, base_url: str, timeout: int) -> None:
    response = session.get(base_url, timeout=timeout, headers=DEFAULT_HEADERS)
    response.raise_for_status()


def request_catalog_api(
    session: requests.Session,
    base_url: str,
    api_url: str,
    search_url: str,
    params: dict[str, str],
    timeout: int,
) -> dict:
    last_error: requests.RequestException | None = None
    for attempt in range(2):
        try:
            if attempt > 0:
                time.sleep(1.0)
                bootstrap_session(session, base_url, timeout)
            response = session.get(
                api_url,
                params=params,
                timeout=timeout,
                headers={
                    **API_HEADERS,
                    "Referer": search_url,
                    "Origin": base_url,
                },
            )
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as exc:
            last_error = exc
            if exc.response is not None and exc.response.status_code != 403:
                raise
        except requests.RequestException as exc:
            last_error = exc
            if attempt == 1:
                raise
    if last_error:
        raise last_error
    raise requests.RequestException("Unknown Vinted catalog API failure")


def is_geo_in_cooldown(geo: str) -> bool:
    until = GEO_COOLDOWNS.get(geo)
    return bool(until and until > time.time())


def mark_geo_cooldown(geo: str) -> None:
    GEO_COOLDOWNS[geo] = time.time() + GEO_COOLDOWN_SECONDS


def format_money(raw_value: dict | None) -> str:
    if not raw_value:
        return ""
    amount = str(raw_value.get("amount", "")).strip()
    currency = str(raw_value.get("currency_code", "")).strip().upper()
    if not amount:
        return currency
    symbols = {
        "GBP": "£",
        "EUR": "€",
        "USD": "$",
    }
    symbol = symbols.get(currency, currency)
    try:
        numeric = float(amount)
        return f"{symbol}{numeric:.2f}" if symbol in {"£", "€", "$"} else f"{numeric:.2f} {symbol}"
    except ValueError:
        return f"{symbol}{amount}" if symbol in {"£", "€", "$"} else f"{amount} {symbol}".strip()


def parse_api_items(payload: dict, geo: str, search_url: str, base_url: str) -> list[Item]:
    items: list[Item] = []
    for raw_item in payload.get("items", []):
        item_url = str(raw_item.get("url") or "").strip()
        if not item_url:
            path = str(raw_item.get("path") or "").strip()
            item_url = f"{base_url}{path}" if path.startswith("/") else path
        if not item_url:
            continue

        photo = raw_item.get("photo") or {}
        price = format_money(raw_item.get("price"))
        total_price = format_money(raw_item.get("total_item_price"))
        currency = str((raw_item.get("price") or {}).get("currency_code", "")).upper()
        size = clean_text(str(raw_item.get("size_title") or ""))
        condition = clean_text(str(raw_item.get("status") or ""))
        brand = clean_text(str(raw_item.get("brand_title") or ""))
        title = clean_text(str(raw_item.get("title") or brand or ""))

        items.append(
            Item(
                geo=geo,
                item_id=str(raw_item.get("id") or ""),
                title=title,
                subtitle=" · ".join(part for part in (size, condition) if part),
                brand=brand,
                size=size,
                condition=condition,
                price=price,
                total_price=total_price,
                currency=currency,
                image_url=str(photo.get("full_size_url") or photo.get("url") or "").strip(),
                item_url=item_url,
                search_url=search_url,
            )
        )

    return items


def parse_items(html: str, geo: str, search_url: str) -> list[Item]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[Item] = []

    for container in soup.select("div.new-item-box__container[data-testid^='product-item-id-']"):
        item_id = container.get("data-testid", "").replace("product-item-id-", "")
        image = container.select_one("img")
        overlay = container.select_one("a.new-item-box__overlay")
        title_node = container.select_one("[data-testid$='--description-title']")
        subtitle_node = container.select_one("[data-testid$='--description-subtitle']")
        price_node = container.select_one("[data-testid$='--price-text']")
        total_node = container.select_one("[data-testid='total-combined-price']")

        if not overlay:
            continue

        alt_text = clean_text(image.get("alt", "")) if image else ""
        brand = clean_text(title_node.get_text(" ", strip=True)) if title_node else ""
        subtitle = clean_text(subtitle_node.get_text(" ", strip=True)) if subtitle_node else ""
        size, condition = split_subtitle(subtitle)
        item_url = overlay.get("href", "").strip()

        if item_url.startswith("/"):
            item_url = f"{GEO_DOMAINS[geo]}{item_url}"

        title = alt_text.split(", brand:", 1)[0] if ", brand:" in alt_text else alt_text or brand
        price = clean_text(price_node.get_text(" ", strip=True)) if price_node else ""
        total_price = clean_text(total_node.get_text(" ", strip=True)) if total_node else ""

        items.append(
            Item(
                geo=geo,
                item_id=item_id,
                title=title,
                subtitle=subtitle,
                brand=brand,
                size=size,
                condition=condition,
                price=price,
                total_price=total_price,
                currency=extract_currency(price, total_price, alt_text),
                image_url=image.get("src", "").strip() if image else "",
                item_url=item_url,
                search_url=search_url,
            )
        )

    return items


def fetch_html(session: requests.Session, url: str, timeout: int) -> str:
    response = session.get(url, timeout=timeout, headers=DEFAULT_HEADERS)
    response.raise_for_status()
    return response.content.decode("utf-8", errors="replace")


def extract_item_age_minutes_from_html(html: str) -> int | None:
    soup = BeautifulSoup(html, "html.parser")
    for text in soup.stripped_strings:
        normalized = clean_text(text)
        lower = normalized.lower()
        if not any(token in lower for token in ("ajout", "added", "il y a", "ago")):
            continue
        for pattern, multiplier in AGE_PATTERNS:
            match = pattern.search(lower)
            if match:
                return int(match.group(1)) * multiplier
    return None


def get_item_age_minutes(
    session: requests.Session,
    item_url: str,
    timeout: int = 30,
    html: str | None = None,
) -> int | None:
    if html is None:
        try:
            html = fetch_html(session, item_url, timeout=timeout)
        except requests.RequestException:
            return None
    return extract_item_age_minutes_from_html(html)


def get_cached_item_details(item_url: str) -> tuple[str, str, str, int | None] | None:
    cached = DETAIL_CACHE.get(item_url)
    if not cached:
        return None
    cached_at, payload = cached
    if time.time() - cached_at > DETAIL_CACHE_TTL_SECONDS:
        DETAIL_CACHE.pop(item_url, None)
        return None
    return payload


def set_cached_item_details(
    item_url: str,
    seller_country: str,
    seller_city: str,
    seller_last_online: str,
    listing_age_minutes: int | None,
) -> None:
    DETAIL_CACHE[item_url] = (
        time.time(),
        (seller_country, seller_city, seller_last_online, listing_age_minutes),
    )


def extract_seller_details_from_html(html: str) -> tuple[str, str, str]:
    country_match = re.search(r'country_title_local\\":\\"([^"]+)\\"', html)
    if not country_match:
        return "", "", ""

    snippet = html[country_match.start():country_match.start() + 500]
    country = clean_text(country_match.group(1).replace("\\/", "/"))

    last_online_match = re.search(r'last_logged_on_ts\\":\\"([^"]+)\\"', snippet)
    city_match = re.search(r'city\\":\\"([^"]*)\\"', snippet)
    return (
        country,
        clean_text(city_match.group(1).replace("\\/", "/")) if city_match else "",
        format_last_online(last_online_match.group(1)) if last_online_match else "",
    )


def item_matches_requested_geo(item_geo: str, seller_country: str) -> bool:
    if not seller_country:
        return True
    allowed_countries = set(GEO_ALLOWED_COUNTRIES.get(item_geo, set()))
    for related_geo in GEO_RELATED_MARKETS.get(item_geo, set()):
        allowed_countries.update(GEO_ALLOWED_COUNTRIES.get(related_geo, set()))
    if not allowed_countries:
        return True
    return normalize_country_name(seller_country) in allowed_countries


def enrich_item_details(session: requests.Session, item: Item, timeout: int) -> Item | None:
    cached = get_cached_item_details(item.item_url)
    if cached:
        seller_country, seller_city, seller_last_online, listing_age_minutes = cached
        if not item_matches_requested_geo(item.geo, seller_country):
            return None
        item.seller_country = seller_country
        item.seller_city = seller_city
        item.seller_last_online = seller_last_online
        item.listing_age_minutes = listing_age_minutes
        item.listing_age_label = format_age_label(listing_age_minutes)
        return item

    html = fetch_html(session, item.item_url, timeout=timeout)
    seller_country, seller_city, seller_last_online = extract_seller_details_from_html(html)
    if not item_matches_requested_geo(item.geo, seller_country):
        return None

    listing_age_minutes = extract_item_age_minutes_from_html(html)
    set_cached_item_details(
        item.item_url,
        seller_country,
        seller_city,
        seller_last_online,
        listing_age_minutes,
    )
    item.seller_country = seller_country
    item.seller_city = seller_city
    item.seller_last_online = seller_last_online
    item.listing_age_minutes = listing_age_minutes
    item.listing_age_label = format_age_label(listing_age_minutes)
    return item


def safe_enrich_item_details(session: requests.Session, item: Item, timeout: int) -> Item | None:
    try:
        return enrich_item_details(session, item, timeout=timeout)
    except requests.RequestException:
        if SEARCH_MODE == "lite":
            return item
        return item


def scrape_geo(
    session: requests.Session,
    geo: str,
    query: str,
    pages: int,
    delay: float,
    order: str,
    price_from: int | None,
    price_to: int | None,
    extra_params: dict[str, str],
    timeout: int,
) -> list[Item]:
    base_url = GEO_DOMAINS[geo]
    api_url = build_catalog_api_url(base_url)
    all_items: list[Item] = []
    if is_geo_in_cooldown(geo):
        raise requests.RequestException(
            f"[{geo}] cooldown active after recent Vinted block; retry later"
        )
    bootstrap_session(session, base_url, timeout)

    for page in range(1, pages + 1):
        search_url = build_search_url(
            base_url=base_url,
            query=query,
            page=page,
            order=order,
            price_from=price_from,
            price_to=price_to,
            extra_params=extra_params,
        )
        params = {
            "search_text": query,
            "page": str(page),
            "order": order,
        }
        if price_from is not None:
            params["price_from"] = str(price_from)
        if price_to is not None:
            params["price_to"] = str(price_to)
        params.update(extra_params)

        try:
            payload = request_catalog_api(
                session=session,
                base_url=base_url,
                api_url=api_url,
                search_url=search_url,
                params=params,
                timeout=timeout,
            )
            page_items = parse_api_items(payload, geo=geo, search_url=search_url, base_url=base_url)
        except requests.RequestException:
            html = fetch_html(session, search_url, timeout=timeout)
            page_items = parse_items(html, geo=geo, search_url=search_url)

        page_items = [item for item in page_items if item_matches_query_text(item, query)]
        filtered_items: list[Item] = []
        if SEARCH_MODE == "lite":
            filtered_items = page_items
        else:
            max_workers = min(4, max(1, len(page_items)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                enriched_items = executor.map(
                    lambda current_item: safe_enrich_item_details(session, current_item, timeout),
                    page_items,
                )
                for enriched_item in enriched_items:
                    if enriched_item is None:
                        continue
                    filtered_items.append(enriched_item)

        print(
            f"[{geo}] page={page} raw_items={len(page_items)} filtered_items={len(filtered_items)} url={search_url}"
        )
        all_items.extend(filtered_items)

        if page < pages and delay > 0:
            time.sleep(delay)

    return all_items


def dedupe_items(items: list[Item]) -> list[Item]:
    unique: dict[str, Item] = {}
    for item in items:
        unique[item.item_url] = item
    return list(unique.values())


def sort_items_by_query_relevance(items: list[Item], query: str) -> list[Item]:
    return sorted(
        items,
        key=lambda item: (
            query_match_score(item, query),
            1 if normalize_search_text(query) in normalize_search_text(item.title) else 0,
            item.listing_age_minutes is not None,
            -(item.listing_age_minutes or 10**9),
        ),
        reverse=True,
    )


def write_outputs(items: list[Item], output_dir: Path, query: str) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    slug = re.sub(r"[^a-z0-9]+", "_", query.lower()).strip("_") or "query"
    json_path = output_dir / f"vinted_{slug}_{stamp}.json"
    csv_path = output_dir / f"vinted_{slug}_{stamp}.csv"

    payload = [asdict(item) for item in items]
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    fieldnames = list(payload[0].keys()) if payload else list(Item.__dataclass_fields__.keys())
    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(payload)

    return json_path, csv_path


def run_search(
    query: str,
    geos: list[str],
    pages: int = 1,
    delay: float = 1.0,
    order: str = "newest_first",
    price_from: int | None = None,
    price_to: int | None = None,
    extra_params: dict[str, str] | None = None,
    timeout: int = 30,
    output_dir: str | Path = "output",
) -> dict:
    extra_params = extra_params or {}
    session = requests.Session()
    all_items: list[Item] = []
    failures: list[str] = []

    for geo in geos:
        try:
            all_items.extend(
                scrape_geo(
                    session=session,
                    geo=geo,
                    query=query,
                    pages=pages,
                    delay=delay,
                    order=order,
                    price_from=price_from,
                    price_to=price_to,
                    extra_params=extra_params,
                    timeout=timeout,
                )
            )
        except requests.RequestException as exc:
            if "403" in str(exc):
                mark_geo_cooldown(geo)
            failures.append(f"[{geo}] {exc}")

    unique_items = sort_items_by_query_relevance(dedupe_items(all_items), query)
    json_path, csv_path = write_outputs(unique_items, output_dir=Path(output_dir), query=query)
    return {
        "items": unique_items,
        "raw_count": len(all_items),
        "unique_count": len(unique_items),
        "json_path": json_path,
        "csv_path": csv_path,
        "failures": failures,
    }


def main() -> None:
    args = parse_args()
    geos = expand_geos(args.geo)
    extra_params = parse_extra_params(args.param)
    result = run_search(
        query=args.query,
        geos=geos,
        pages=args.pages,
        delay=args.delay,
        order=args.order,
        price_from=args.price_from,
        price_to=args.price_to,
        extra_params=extra_params,
        timeout=args.timeout,
        output_dir=args.output_dir,
    )

    print(f"\nDone. Total raw items: {result['raw_count']}")
    print(f"Done. Unique items: {result['unique_count']}")
    print(f"JSON: {result['json_path'].resolve()}")
    print(f"CSV:  {result['csv_path'].resolve()}")
    if result["failures"]:
        print("Warnings:")
        for failure in result["failures"]:
            print(f" - {failure}")


if __name__ == "__main__":
    main()
