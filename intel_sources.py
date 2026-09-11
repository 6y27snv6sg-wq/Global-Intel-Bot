"""
Global-Intel-Bot
Verified International Intelligence Sources
============================================

مزود Telegram + X للمصادر الرسمية الدولية المثبتة.

القواعد:
1) لا يضاف handle بالتخمين.
2) المصدر الرسمي لا يكفي وحده؛ يجب أن تكون للمادة قيمة دولية.
3) الحسابات اللغوية/المساندة لا تنشئ قصة ثانية عند تطابق الحدث.
4) محتوى الحدث هو الذي يحدد القسم النهائي، وليس اسم الناشر.
5) التصريحات والروايات الرسمية تُنسب إلى جهتها.
6) فشل منصة أو مصدر لا يوقف بقية المزودات.
7) لا توجد عملية polling تلقائية هنا؛ bot.py يستدعي collector لاحقًا من مسار خلفي.
8) Telegram يُقرأ من صفحة القناة العامة فقط.
9) X API الرسمي هو المسار الأول عند وجود X_BEARER_TOKEN؛ وعند غيابه يُستخدم اكتشاف بحث عام محدود للحسابات الموثقة فقط، بلا scraping مباشر لـX.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qs, unquote, urlparse

import aiohttp


log = logging.getLogger(__name__)

GRADE_A_PLUS = "A+"
GRADE_A = "A"
VALID_GRADES = {GRADE_A_PLUS, GRADE_A}

PLATFORM_X = "x"
PLATFORM_TELEGRAM = "telegram"
VALID_PLATFORMS = {PLATFORM_X, PLATFORM_TELEGRAM}

ROLE_PRIMARY = "primary"
ROLE_SUPPORT = "support"
ROLE_SPECIALIZED = "specialized"
VALID_ROLES = {ROLE_PRIMARY, ROLE_SUPPORT, ROLE_SPECIALIZED}

ROUTE_OFFICIAL = "official_statement"
ROUTE_DEFENSE = "defense_security"
ROUTE_ECONOMY = "economy_markets"
ROUTE_BREAKING = "breaking"
ROUTE_DYNAMIC = "dynamic"
VALID_ROUTING_HINTS = {
    ROUTE_OFFICIAL, ROUTE_DEFENSE, ROUTE_ECONOMY,
    ROUTE_BREAKING, ROUTE_DYNAMIC,
}

INTERNATIONAL_TOPICS: Set[str] = {
    "foreign_policy", "diplomacy", "international_relations", "geopolitics",
    "war", "conflict", "ceasefire", "mediation", "sanctions",
    "financial_sanctions", "defense", "military", "military_operations",
    "military_alliances", "international_security", "nuclear",
    "nuclear_security", "nuclear_safety", "safeguards", "peacekeeping",
    "strategic_affairs", "international_trade", "global_economy",
    "energy_security", "international_energy", "summits",
    "multilateral_relations", "international_decisions", "terrorism",
    "arms_control",
}

EXCLUDED_CONTENT_TYPES: Set[str] = {
    "local_services", "municipal_news", "routine_domestic_news",
    "domestic_ceremonies", "routine_protocol", "tourism", "local_events",
    "recruitment", "jobs", "internal_hr", "passport_services",
    "visa_services", "routine_consular_services", "customer_service",
    "traffic", "weather", "sports", "entertainment", "culture_only",
    "awareness_campaign", "commemoration_only", "human_interest_only",
}

MAX_RESPONSE_BYTES = 900_000
MAX_POSTS_PER_SOURCE = 8
HTTP_TIMEOUT_SECONDS = 6
CIRCUIT_FAILURES = 3
CIRCUIT_COOLDOWN_SECONDS = 300
X_USER_ID_CACHE_TTL = 6 * 3600
X_API_BASE = "https://api.x.com/2"

# Free/no-token fallback. It does not scrape X itself: it asks a public search
# endpoint for already-indexed x.com status URLs, then accepts only exact handles
# that already exist in VERIFIED_SOCIAL_SOURCES.
X_SEARCH_RSS_URL = "https://www.bing.com/search"
X_SEARCH_REFRESH_SECONDS = 300
X_SEARCH_MAX_AGE_HOURS = 48
X_SEARCH_BATCH_SIZE = 4
X_SEARCH_MAX_RESULTS_PER_BATCH = 30
X_SEARCH_TIMEOUT_SECONDS = 5
X_SEARCH_CACHE_TTL_SECONDS = 3 * 3600
X_SEARCH_CIRCUIT_ID = "__x_search_engine__"

USER_AGENT = "Al-Arrab-News/1.0 (public-source social-news monitor)"
DEFAULT_INCLUDE_SUPPORT = False

_CIRCUIT_STATE: Dict[str, Dict[str, float]] = {}
_X_USER_ID_CACHE: Dict[str, Tuple[str, float]] = {}
_SOURCE_HEALTH: Dict[str, Dict[str, Any]] = {}
_X_SEARCH_CACHE: Dict[str, Dict[str, Any]] = {}
_X_SEARCH_LAST_RUN = 0.0


def _src(
    source_id: str,
    entity: str,
    entity_ar: str,
    organization: str,
    organization_ar: str,
    platform: str,
    handle: str,
    grade: str,
    role: str,
    language: str,
    routing_hint: str,
    topics: Iterable[str],
    dedup_group: str,
    priority: int,
    official_proof: str,
    notes: str = "",
) -> Dict:
    url = f"https://t.me/{handle}" if platform == PLATFORM_TELEGRAM else f"https://x.com/{handle}"
    return {
        "id": source_id, "entity": entity, "entity_ar": entity_ar,
        "organization": organization, "organization_ar": organization_ar,
        "platform": platform, "handle": handle, "url": url,
        "grade": grade, "role": role, "active": True, "language": language,
        "routing_hint": routing_hint, "topics": set(topics),
        "dedup_group": dedup_group, "priority": priority,
        "official_proof": official_proof, "notes": notes,
    }


VERIFIED_SOCIAL_SOURCES: List[Dict] = [
    _src("oman_mofa_x","Oman","سلطنة عُمان","Ministry of Foreign Affairs","وزارة الخارجية العُمانية",PLATFORM_X,"FMofOman",GRADE_A_PLUS,ROLE_PRIMARY,"multi",ROUTE_OFFICIAL,{"foreign_policy","diplomacy","international_relations","mediation","conflict"},"oman_mofa",100,"https://www.fm.gov.om/"),
    _src("russia_mofa_telegram","Russia","روسيا","Ministry of Foreign Affairs","وزارة الخارجية الروسية",PLATFORM_TELEGRAM,"MID_Russia",GRADE_A_PLUS,ROLE_PRIMARY,"ru",ROUTE_DYNAMIC,{"foreign_policy","diplomacy","international_relations","conflict","sanctions","international_security"},"russia_mofa",100,"https://mid.ru/"),
    _src("russia_mofa_telegram_en","Russia","روسيا","Ministry of Foreign Affairs","وزارة الخارجية الروسية",PLATFORM_TELEGRAM,"MFARussia",GRADE_A,ROLE_SUPPORT,"en",ROUTE_DYNAMIC,{"foreign_policy","diplomacy","international_relations","conflict","sanctions"},"russia_mofa",75,"https://mid.ru/"),
    _src("france_mofa_x","France","فرنسا","Ministry for Europe and Foreign Affairs","وزارة أوروبا والشؤون الخارجية الفرنسية",PLATFORM_X,"francediplo",GRADE_A_PLUS,ROLE_PRIMARY,"fr",ROUTE_DYNAMIC,{"foreign_policy","diplomacy","international_relations","sanctions","conflict"},"france_mofa",100,"https://www.diplomatie.gouv.fr/"),
    _src("france_mofa_x_en","France","فرنسا","Ministry for Europe and Foreign Affairs","وزارة أوروبا والشؤون الخارجية الفرنسية",PLATFORM_X,"francediplo_EN",GRADE_A,ROLE_SUPPORT,"en",ROUTE_DYNAMIC,{"foreign_policy","diplomacy","international_relations"},"france_mofa",75,"https://www.diplomatie.gouv.fr/"),
    _src("france_mofa_x_ar","France","فرنسا","Ministry for Europe and Foreign Affairs","وزارة أوروبا والشؤون الخارجية الفرنسية",PLATFORM_X,"francediplo_AR",GRADE_A,ROLE_SUPPORT,"ar",ROUTE_DYNAMIC,{"foreign_policy","diplomacy","international_relations"},"france_mofa",72,"https://www.diplomatie.gouv.fr/"),
    _src("germany_mofa_x","Germany","ألمانيا","Federal Foreign Office","وزارة الخارجية الألمانية",PLATFORM_X,"AuswaertigesAmt",GRADE_A_PLUS,ROLE_PRIMARY,"de",ROUTE_DYNAMIC,{"foreign_policy","diplomacy","international_relations","international_security","conflict"},"germany_mofa",100,"https://www.auswaertiges-amt.de/"),
    _src("germany_mofa_x_en","Germany","ألمانيا","Federal Foreign Office","وزارة الخارجية الألمانية",PLATFORM_X,"GermanyDiplo",GRADE_A,ROLE_SUPPORT,"en",ROUTE_DYNAMIC,{"foreign_policy","diplomacy","international_relations","international_security"},"germany_mofa",78,"https://www.auswaertiges-amt.de/"),
    _src("uk_fcdo_x","United Kingdom","المملكة المتحدة","Foreign, Commonwealth & Development Office","وزارة الخارجية والتنمية البريطانية",PLATFORM_X,"FCDOGovUK",GRADE_A_PLUS,ROLE_PRIMARY,"en",ROUTE_DYNAMIC,{"foreign_policy","diplomacy","international_relations","sanctions","conflict","international_security"},"uk_fcdo",100,"https://www.gov.uk/government/organisations/foreign-commonwealth-development-office"),
    _src("uk_fcdo_x_ar","United Kingdom","المملكة المتحدة","Foreign, Commonwealth & Development Office","وزارة الخارجية والتنمية البريطانية",PLATFORM_X,"FCDOArabic",GRADE_A,ROLE_SUPPORT,"ar",ROUTE_DYNAMIC,{"foreign_policy","diplomacy","international_relations"},"uk_fcdo",72,"https://www.gov.uk/government/organisations/foreign-commonwealth-development-office"),
    _src("uk_mod_press_x","United Kingdom","المملكة المتحدة","Ministry of Defence","وزارة الدفاع البريطانية",PLATFORM_X,"DefenceHQPress",GRADE_A_PLUS,ROLE_PRIMARY,"en",ROUTE_DEFENSE,{"defense","military","military_operations","international_security","military_alliances","conflict"},"uk_mod",100,"https://www.gov.uk/government/organisations/ministry-of-defence"),
    _src("uk_mod_x","United Kingdom","المملكة المتحدة","Ministry of Defence","وزارة الدفاع البريطانية",PLATFORM_X,"DefenceHQ",GRADE_A,ROLE_SUPPORT,"en",ROUTE_DEFENSE,{"defense","military","international_security"},"uk_mod",80,"https://www.gov.uk/government/organisations/ministry-of-defence"),
    _src("japan_mofa_x_en","Japan","اليابان","Ministry of Foreign Affairs","وزارة الخارجية اليابانية",PLATFORM_X,"MofaJapan_en",GRADE_A_PLUS,ROLE_PRIMARY,"en",ROUTE_DYNAMIC,{"foreign_policy","diplomacy","international_relations","international_security","sanctions","strategic_affairs"},"japan_mofa",100,"https://www.mofa.go.jp/"),
    _src("australia_dfat_x","Australia","أستراليا","Department of Foreign Affairs and Trade","وزارة الخارجية والتجارة الأسترالية",PLATFORM_X,"DFAT",GRADE_A_PLUS,ROLE_PRIMARY,"en",ROUTE_DYNAMIC,{"foreign_policy","diplomacy","international_relations","sanctions","international_trade","international_security"},"australia_dfat",100,"https://www.dfat.gov.au/"),
    _src("india_mea_x","India","الهند","Ministry of External Affairs","وزارة الشؤون الخارجية الهندية",PLATFORM_X,"MEAIndia",GRADE_A_PLUS,ROLE_PRIMARY,"en",ROUTE_DYNAMIC,{"foreign_policy","diplomacy","international_relations","strategic_affairs","international_security","conflict"},"india_mea",100,"https://www.mea.gov.in/"),
    _src("eu_eeas_x","European Union","الاتحاد الأوروبي","European External Action Service","دائرة العمل الخارجي الأوروبية",PLATFORM_X,"eu_eeas",GRADE_A_PLUS,ROLE_PRIMARY,"en",ROUTE_DYNAMIC,{"foreign_policy","diplomacy","international_relations","sanctions","conflict","international_security"},"eu_eeas",100,"https://www.eeas.europa.eu/"),
    _src("eu_security_defence_x","European Union","الاتحاد الأوروبي","EU Security and Defence","الأمن والدفاع في الاتحاد الأوروبي",PLATFORM_X,"EUSec_Defence",GRADE_A,ROLE_SPECIALIZED,"en",ROUTE_DEFENSE,{"defense","military","international_security","military_operations"},"eu_security_defence",88,"https://www.eeas.europa.eu/"),
    _src("eu_council_press_x","European Union","الاتحاد الأوروبي","Council of the European Union","مجلس الاتحاد الأوروبي",PLATFORM_X,"EUCouncilPress",GRADE_A_PLUS,ROLE_PRIMARY,"en",ROUTE_DYNAMIC,{"sanctions","foreign_policy","international_decisions","summits","international_security","defense"},"eu_council",100,"https://www.consilium.europa.eu/"),
    _src("nato_press_x","NATO","حلف شمال الأطلسي","NATO Press","المكتب الصحفي لحلف شمال الأطلسي",PLATFORM_X,"NATOPress",GRADE_A_PLUS,ROLE_PRIMARY,"en",ROUTE_DEFENSE,{"defense","military","military_operations","military_alliances","international_security","conflict"},"nato",100,"https://www.nato.int/"),
    _src("nato_general_x","NATO","حلف شمال الأطلسي","NATO","حلف شمال الأطلسي",PLATFORM_X,"NATO",GRADE_A,ROLE_SUPPORT,"en",ROUTE_DEFENSE,{"defense","military_alliances","international_security"},"nato",82,"https://www.nato.int/"),
    _src("nato_cmc_x","NATO","حلف شمال الأطلسي","NATO Military Committee","اللجنة العسكرية لحلف شمال الأطلسي",PLATFORM_X,"CMC_NATO",GRADE_A,ROLE_SPECIALIZED,"en",ROUTE_DEFENSE,{"defense","military","international_security","strategic_affairs"},"nato_military",85,"https://www.nato.int/"),
    _src("nato_pascad_x","NATO","حلف شمال الأطلسي","NATO International Military Staff","الهيئة العسكرية الدولية في الناتو",PLATFORM_X,"NATO_PASCAD",GRADE_A,ROLE_SPECIALIZED,"en",ROUTE_DEFENSE,{"defense","military","international_security"},"nato_military",82,"https://www.nato.int/"),
    _src("iaea_x","IAEA","الوكالة الدولية للطاقة الذرية","International Atomic Energy Agency","الوكالة الدولية للطاقة الذرية",PLATFORM_X,"IAEAorg",GRADE_A_PLUS,ROLE_PRIMARY,"en",ROUTE_DYNAMIC,{"nuclear","nuclear_security","nuclear_safety","safeguards","international_security","conflict"},"iaea",100,"https://www.iaea.org/"),
    _src("un_peacekeeping_x","United Nations","الأمم المتحدة","United Nations Peacekeeping","عمليات حفظ السلام التابعة للأمم المتحدة",PLATFORM_X,"UNPeacekeeping",GRADE_A,ROLE_SPECIALIZED,"en",ROUTE_DEFENSE,{"peacekeeping","conflict","ceasefire","international_security"},"un_peacekeeping",86,"https://peacekeeping.un.org/"),
]

VERIFIED_SOURCE_BY_ID = {s["id"]: s for s in VERIFIED_SOCIAL_SOURCES}
VERIFIED_SOURCE_BY_PLATFORM_HANDLE = {
    f'{s["platform"]}:{s["handle"].lower()}': s for s in VERIFIED_SOCIAL_SOURCES
}


def normalize_handle(handle: str) -> str:
    return (handle or "").strip().lstrip("@").lower()


def get_source_by_id(source_id: str) -> Optional[Dict]:
    source = VERIFIED_SOURCE_BY_ID.get(source_id or "")
    return source if source and source.get("active") else None


def get_source(platform: str, handle: str) -> Optional[Dict]:
    key = f"{(platform or '').strip().lower()}:{normalize_handle(handle)}"
    source = VERIFIED_SOURCE_BY_PLATFORM_HANDLE.get(key)
    return source if source and source.get("active") else None


def is_verified_source(platform: str, handle: str) -> bool:
    return get_source(platform, handle) is not None


def get_active_sources(platform=None, grade=None, primary_only=False, include_support=True) -> List[Dict]:
    result = []
    for source in VERIFIED_SOCIAL_SOURCES:
        if not source.get("active"):
            continue
        if platform and source.get("platform") != platform:
            continue
        if grade and source.get("grade") != grade:
            continue
        if primary_only and source.get("role") != ROLE_PRIMARY:
            continue
        if not include_support and source.get("role") == ROLE_SUPPORT:
            continue
        result.append(source)
    return sorted(
        result,
        key=lambda s: (s.get("role") == ROLE_PRIMARY, s.get("grade") == GRADE_A_PLUS, s.get("priority", 0)),
        reverse=True,
    )


def get_primary_source_for_group(dedup_group: str) -> Optional[Dict]:
    candidates = [s for s in VERIFIED_SOCIAL_SOURCES if s.get("active") and s.get("dedup_group") == dedup_group]
    if not candidates:
        return None
    candidates.sort(
        key=lambda s: (s.get("role") == ROLE_PRIMARY, s.get("grade") == GRADE_A_PLUS, s.get("priority", 0)),
        reverse=True,
    )
    return candidates[0]


def get_dedup_group(platform: str, handle: str) -> Optional[str]:
    source = get_source(platform, handle)
    return source.get("dedup_group") if source else None


def should_prefer_source(candidate: Dict, existing: Dict) -> bool:
    if not candidate:
        return False
    if not existing:
        return True
    if candidate.get("role") == ROLE_PRIMARY and existing.get("role") != ROLE_PRIMARY:
        return True
    if existing.get("role") == ROLE_PRIMARY and candidate.get("role") != ROLE_PRIMARY:
        return False
    if candidate.get("grade") == GRADE_A_PLUS and existing.get("grade") != GRADE_A_PLUS:
        return True
    if existing.get("grade") == GRADE_A_PLUS and candidate.get("grade") != GRADE_A_PLUS:
        return False
    return candidate.get("priority", 0) > existing.get("priority", 0)


def should_suppress_duplicate_source(candidate: Dict, selected_source: Dict) -> bool:
    if not candidate or not selected_source:
        return False
    a = candidate.get("dedup_group")
    b = selected_source.get("dedup_group")
    return bool(a and b and a == b and not should_prefer_source(candidate, selected_source))


def source_has_international_value(source: Dict) -> bool:
    return bool(source and set(source.get("topics") or set()) & INTERNATIONAL_TOPICS)


def should_accept_content_type(content_type: Optional[str]) -> bool:
    return not content_type or content_type not in EXCLUDED_CONTENT_TYPES


def attribution_prefix(source: Dict) -> str:
    if not source:
        return "بحسب المصدر الرسمي"
    if source.get("organization_ar"):
        return f'بحسب {source["organization_ar"]}'
    if source.get("entity_ar"):
        return f'بحسب الجهة الرسمية في {source["entity_ar"]}'
    return "بحسب المصدر الرسمي"


_WS = re.compile(r"\s+")
_TAG = re.compile(r"<[^>]+>")


def _clean_text(value: object) -> str:
    text = html.unescape(str(value or ""))
    text = _TAG.sub(" ", text)
    return _WS.sub(" ", text).strip()


def _fingerprint(text: str) -> str:
    text = _clean_text(text).lower()
    text = re.sub(r"[^\w\u0600-\u06ff]+", " ", text)
    return hashlib.sha1(_WS.sub(" ", text).strip().encode("utf-8", "ignore")).hexdigest()


def _canonical_external_url(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    if parsed.netloc.lower() in {"t.me","telegram.me","x.com","twitter.com","www.x.com","www.twitter.com"}:
        return ""
    return parsed._replace(fragment="").geturl()


def _safe_iso_datetime(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def build_social_event(*, source_id: str, text: str, url: str, published=None, external_id="", content_url="", metadata=None) -> Dict:
    source = get_source_by_id(source_id)
    if not source:
        raise ValueError(f"Unknown/inactive social source: {source_id}")
    clean = _clean_text(text)
    return {
        "provider": "social_intel",
        "source_id": source_id,
        "platform": source["platform"],
        "handle": source["handle"],
        "entity": source["entity"],
        "entity_ar": source["entity_ar"],
        "organization": source["organization"],
        "organization_ar": source["organization_ar"],
        "source_grade": source["grade"],
        "source_role": source["role"],
        "language": source["language"],
        "dedup_group": source["dedup_group"],
        "priority": source["priority"],
        "routing_hint": source["routing_hint"],
        "attribution_ar": attribution_prefix(source),
        "title": clean[:280].rstrip(),
        "original_title": clean,
        "summary": "",
        "url": (url or "").strip(),
        "published": published,
        "external_id": (external_id or "").strip(),
        "content_url": _canonical_external_url(content_url),
        "metadata": dict(metadata or {}),
    }


class _TelegramPublicParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.posts = []
        self._current = None
        self._div_depth = 0
        self._message_depth = None
        self._text_depth = None

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        cls = set((a.get("class") or "").split())

        if tag == "div":
            self._div_depth += 1
            if self._current is None and "tgme_widget_message" in cls and a.get("data-post"):
                self._current = {
                    "data_post": a["data-post"], "text_parts": [], "published": "",
                    "permalink": "", "links": [], "forwarded": False,
                }
                self._message_depth = self._div_depth

            if self._current is not None:
                if "tgme_widget_message_text" in cls:
                    self._text_depth = self._div_depth
                if "tgme_widget_message_forwarded_from" in cls:
                    self._current["forwarded"] = True

        if self._current is None:
            return

        if tag == "time" and a.get("datetime"):
            self._current["published"] = a["datetime"]

        if tag == "a":
            href = (a.get("href") or "").strip()
            if href:
                if "tgme_widget_message_date" in cls:
                    self._current["permalink"] = href
                else:
                    self._current["links"].append(href)

    def handle_data(self, data):
        if self._current is not None and self._text_depth is not None:
            self._current["text_parts"].append(data)

    def handle_endtag(self, tag):
        if tag != "div":
            return

        if self._current is not None:
            if self._text_depth == self._div_depth:
                self._text_depth = None
            if self._message_depth == self._div_depth:
                self._current["text"] = _clean_text(" ".join(self._current["text_parts"]))
                self.posts.append(self._current)
                self._current = None
                self._message_depth = None
                self._text_depth = None

        self._div_depth = max(0, self._div_depth - 1)


def parse_telegram_public_html(document: str, *, source_id: str, include_forwarded=False) -> List[Dict]:
    source = get_source_by_id(source_id)
    if not source or source["platform"] != PLATFORM_TELEGRAM:
        raise ValueError(f"Not a Telegram source: {source_id}")
    if not document:
        return []

    parser = _TelegramPublicParser()
    try:
        parser.feed(document)
    except Exception:
        return []

    result, seen = [], set()
    expected_handle = normalize_handle(source["handle"])

    for post in parser.posts:
        text = _clean_text(post.get("text", ""))
        if not text or (post.get("forwarded") and not include_forwarded):
            continue

        data_post = (post.get("data_post") or "").strip()
        if "/" not in data_post:
            continue

        handle, post_id = data_post.split("/", 1)
        if normalize_handle(handle) != expected_handle:
            continue

        if post_id in seen:
            continue
        seen.add(post_id)

        content_url = ""
        for href in post.get("links") or []:
            candidate = _canonical_external_url(href)
            if candidate:
                content_url = candidate
                break

        result.append(
            build_social_event(
                source_id=source_id,
                text=text,
                url=(post.get("permalink") or f"https://t.me/{handle}/{post_id}"),
                published=_safe_iso_datetime(post.get("published", "")),
                external_id=post_id,
                content_url=content_url,
                metadata={"forwarded": bool(post.get("forwarded")), "ingest": "telegram_public_page"},
            )
        )
        if len(result) >= MAX_POSTS_PER_SOURCE:
            break

    return result


def parse_x_posts_payload(payload: Dict[str, Any], *, source_id: str) -> List[Dict]:
    source = get_source_by_id(source_id)
    if not source or source["platform"] != PLATFORM_X:
        raise ValueError(f"Not an X source: {source_id}")

    data = payload.get("data")
    if not isinstance(data, list):
        return []

    result, seen = [], set()
    for item in data:
        if not isinstance(item, dict):
            continue
        post_id = str(item.get("id") or "").strip()
        text = _clean_text(item.get("text") or "")
        if not post_id or not text or post_id in seen:
            continue
        seen.add(post_id)

        content_url = ""
        entities = item.get("entities") or {}
        urls = entities.get("urls") if isinstance(entities, dict) else None
        if isinstance(urls, list):
            for u in urls:
                if not isinstance(u, dict):
                    continue
                candidate = _canonical_external_url(u.get("expanded_url") or u.get("unwound_url") or "")
                if candidate:
                    content_url = candidate
                    break

        result.append(
            build_social_event(
                source_id=source_id,
                text=text,
                url=f'https://x.com/{source["handle"]}/status/{post_id}',
                published=_safe_iso_datetime(str(item.get("created_at") or "")),
                external_id=post_id,
                content_url=content_url,
                metadata={"ingest": "x_api_v2"},
            )
        )
        if len(result) >= MAX_POSTS_PER_SOURCE:
            break

    return result


def _circuit_state(source_id):
    return _CIRCUIT_STATE.setdefault(source_id, {"failures": 0.0, "open_until": 0.0})


def _circuit_is_open(source_id):
    return _circuit_state(source_id)["open_until"] > time.monotonic()


def _circuit_success(source_id):
    state = _circuit_state(source_id)
    state["failures"] = 0.0
    state["open_until"] = 0.0


def _circuit_failure(source_id):
    state = _circuit_state(source_id)
    state["failures"] += 1.0
    if state["failures"] >= CIRCUIT_FAILURES:
        state["open_until"] = time.monotonic() + CIRCUIT_COOLDOWN_SECONDS


def reset_circuit_breakers():
    _CIRCUIT_STATE.clear()


def _record_health(
    source_id: str,
    status: str,
    *,
    detail: str = "",
    items: Optional[int] = None,
    transport: str = "",
    x_api_status: str = "",
) -> None:
    state = _SOURCE_HEALTH.setdefault(source_id, {})
    state["status"] = status
    state["updated_monotonic"] = time.monotonic()
    if detail:
        state["detail"] = _clean_text(detail)[:240]
    else:
        state.pop("detail", None)
    if items is not None:
        state["items"] = max(0, int(items))
    if transport:
        state["transport"] = transport
    if x_api_status:
        state["x_api_status"] = x_api_status


def source_health() -> Dict[str, Dict[str, Any]]:
    """Return a read-only snapshot of current process-local source health."""
    return {source_id: dict(state) for source_id, state in _SOURCE_HEALTH.items()}


async def _read_bounded(response: aiohttp.ClientResponse) -> str:
    chunks, total = [], 0
    async for chunk in response.content.iter_chunked(64 * 1024):
        total += len(chunk)
        if total > MAX_RESPONSE_BYTES:
            raise ValueError("response exceeds MAX_RESPONSE_BYTES")
        chunks.append(chunk)
    raw = b"".join(chunks)
    charset = response.charset or "utf-8"
    try:
        return raw.decode(charset, errors="replace")
    except LookupError:
        return raw.decode("utf-8", errors="replace")


async def _get_text(session, url, *, source_id, headers=None, params=None, mark_success=True) -> str:
    if _circuit_is_open(source_id):
        _record_health(source_id, "circuit_open")
        return ""
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS, connect=2)
    try:
        async with session.get(url, headers=headers, params=params, timeout=timeout, allow_redirects=True) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}")
            text = await _read_bounded(response)
        if mark_success:
            _circuit_success(source_id)
            _record_health(source_id, "ok")
        return text
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _circuit_failure(source_id)
        _record_health(source_id, "fetch_failed", detail=str(exc))
        log.warning("social source failed: %s: %s", source_id, exc)
        return ""


async def _get_json(session, url, *, source_id, headers, params=None) -> Dict[str, Any]:
    text = await _get_text(
        session, url, source_id=source_id, headers=headers, params=params, mark_success=False
    )
    if not text:
        return {}
    try:
        payload = json.loads(text)
        if not isinstance(payload, dict):
            raise ValueError("JSON payload is not an object")
        _circuit_success(source_id)
        _record_health(source_id, "ok")
        return payload
    except Exception as exc:
        _circuit_failure(source_id)
        _record_health(source_id, "parse_failed", detail=str(exc))
        log.warning("social JSON parser failed: %s: %s", source_id, exc)
        return {}


async def _collect_telegram_source(session, source, *, include_forwarded):
    source_id = source["id"]
    document = await _get_text(
        session, f'https://t.me/s/{source["handle"]}', source_id=source_id, mark_success=False
    )
    if not document:
        return []
    try:
        events = parse_telegram_public_html(
            document, source_id=source_id, include_forwarded=include_forwarded
        )
        _circuit_success(source_id)
        _record_health(source_id, "ok" if events else "empty", items=len(events), transport="telegram_public_page")
        return events
    except Exception as exc:
        _circuit_failure(source_id)
        _record_health(source_id, "parse_failed", detail=str(exc))
        log.warning("telegram parser failed: %s: %s", source_id, exc)
        return []


def _x_bearer_token() -> str:
    return (os.getenv("X_BEARER_TOKEN") or "").strip()


async def _resolve_x_user_id(session, source, bearer):
    handle = source["handle"]
    cache_key = normalize_handle(handle)
    cached = _X_USER_ID_CACHE.get(cache_key)
    now = time.monotonic()
    if cached and cached[1] > now:
        return cached[0]

    headers = {"Authorization": f"Bearer {bearer}"}
    payload = await _get_json(
        session,
        f"{X_API_BASE}/users/by/username/{handle}",
        source_id=source["id"],
        headers=headers,
    )
    data = payload.get("data")
    if not isinstance(data, dict):
        return ""
    user_id = str(data.get("id") or "").strip()
    username = normalize_handle(str(data.get("username") or ""))
    if not user_id or (username and username != cache_key):
        return ""

    _X_USER_ID_CACHE[cache_key] = (user_id, now + X_USER_ID_CACHE_TTL)
    return user_id


async def _collect_x_source(session, source, *, bearer):
    source_id = source["id"]
    if not bearer:
        _record_health(source_id, "disabled", detail="X_BEARER_TOKEN missing", transport="x_api", x_api_status="disabled")
        return []
    user_id = await _resolve_x_user_id(session, source, bearer)
    if not user_id:
        if not _circuit_is_open(source_id):
            _record_health(source_id, "unavailable", detail="X user lookup returned no usable account")
        return []

    payload = await _get_json(
        session,
        f"{X_API_BASE}/users/{user_id}/tweets",
        source_id=source_id,
        headers={"Authorization": f"Bearer {bearer}"},
        params={
            "max_results": str(MAX_POSTS_PER_SOURCE),
            "exclude": "retweets,replies",
            "tweet.fields": "created_at,entities",
        },
    )
    if not payload:
        return []
    try:
        events = parse_x_posts_payload(payload, source_id=source_id)
        _circuit_success(source_id)
        _record_health(source_id, "ok" if events else "empty", items=len(events), transport="x_api_v2", x_api_status="enabled")
        return events
    except Exception as exc:
        _circuit_failure(source_id)
        _record_health(source_id, "parse_failed", detail=str(exc))
        log.warning("x parser failed: %s: %s", source_id, exc)
        return []



_X_STATUS_URL_RE = re.compile(
    r"https?://(?:www\.)?(?:x\.com|twitter\.com)/([A-Za-z0-9_]{1,15})/status/(\d+)",
    re.IGNORECASE,
)


def _rss_text(value: str) -> str:
    value = html.unescape(str(value or ""))
    value = re.sub(r"<[^>]+>", " ", value)
    return _clean_text(value)


def _extract_exact_x_status_url(raw: str, allowed_handles: Dict[str, Dict]) -> Tuple[str, str, str]:
    """Return (source_id, canonical_url, post_id) only for a verified exact handle."""
    raw = html.unescape(str(raw or "")).strip()
    candidates = [raw]

    try:
        parsed = urlparse(raw)
        if parsed.netloc.lower().endswith("bing.com"):
            query = parse_qs(parsed.query)
            for key in ("url", "u", "r"):
                for value in query.get(key, []):
                    if value:
                        candidates.append(unquote(value))
    except Exception:
        pass

    candidates.extend(match.group(0) for match in _X_STATUS_URL_RE.finditer(raw))

    for candidate in candidates:
        match = _X_STATUS_URL_RE.search(candidate)
        if not match:
            continue
        handle, post_id = match.group(1), match.group(2)
        source = allowed_handles.get(normalize_handle(handle))
        if not source:
            continue
        return (
            source["id"],
            f'https://x.com/{source["handle"]}/status/{post_id}',
            post_id,
        )
    return "", "", ""


def _safe_rss_datetime(value: str) -> Optional[datetime]:
    value = str(value or "").strip()
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return _safe_iso_datetime(value)


def parse_x_search_rss(document: str, *, sources: Iterable[Dict]) -> Dict[str, List[Dict]]:
    """Parse search RSS and accept only direct status URLs from verified X handles."""
    allowed = {
        normalize_handle(source["handle"]): source
        for source in sources
        if source.get("platform") == PLATFORM_X
    }
    output: Dict[str, List[Dict]] = {source["id"]: [] for source in allowed.values()}
    if not document or not allowed:
        return output

    try:
        root = ET.fromstring(document)
    except ET.ParseError:
        raise ValueError("search RSS is not valid XML")

    now = datetime.now(timezone.utc)
    oldest = now - timedelta(hours=X_SEARCH_MAX_AGE_HOURS)
    seen: Set[Tuple[str, str]] = set()

    for item in root.findall(".//item"):
        title = _rss_text(item.findtext("title") or "")
        description = _rss_text(item.findtext("description") or "")
        link = _clean_text(item.findtext("link") or "")
        guid = _clean_text(item.findtext("guid") or "")
        raw_blob = " ".join([link, guid, item.findtext("description") or "", item.findtext("title") or ""])

        source_id, canonical_url, post_id = _extract_exact_x_status_url(raw_blob, allowed)
        if not source_id or not canonical_url or not post_id:
            continue

        published = _safe_rss_datetime(item.findtext("pubDate") or "")
        if published is None or published < oldest or published > now + timedelta(minutes=15):
            continue

        key = (source_id, post_id)
        if key in seen:
            continue
        seen.add(key)

        text_value = description if len(description) > len(title) else title
        if not text_value:
            continue

        output[source_id].append(
            build_social_event(
                source_id=source_id,
                text=text_value,
                url=canonical_url,
                published=published,
                external_id=post_id,
                metadata={
                    "ingest": "x_search_discovery",
                    "discovery_engine": "bing_rss",
                    "x_api": "disabled",
                },
            )
        )

    for source_id in output:
        output[source_id] = sorted(
            output[source_id],
            key=lambda event: event.get("published") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )[:MAX_POSTS_PER_SOURCE]
    return output


def _x_search_cache_events(source_id: str, *, now: Optional[float] = None) -> List[Dict]:
    entry = _X_SEARCH_CACHE.get(source_id)
    if not entry:
        return []
    now = time.monotonic() if now is None else now
    updated = float(entry.get("updated_monotonic") or 0.0)
    if updated <= 0 or (now - updated) > X_SEARCH_CACHE_TTL_SECONDS:
        _X_SEARCH_CACHE.pop(source_id, None)
        return []
    return [dict(event) for event in (entry.get("events") or [])]


def _x_search_query(batch: List[Dict]) -> str:
    clauses = [f'site:x.com/{source["handle"]}/status' for source in batch]
    return " OR ".join(clauses)


async def _fetch_x_search_rss(session: aiohttp.ClientSession, query: str) -> str:
    if _circuit_is_open(X_SEARCH_CIRCUIT_ID):
        return ""

    timeout = aiohttp.ClientTimeout(total=X_SEARCH_TIMEOUT_SECONDS, connect=2)
    params = {
        "q": query,
        "format": "rss",
        "count": str(X_SEARCH_MAX_RESULTS_PER_BATCH),
        "setlang": "en-us",
    }
    try:
        async with session.get(
            X_SEARCH_RSS_URL,
            params=params,
            timeout=timeout,
            allow_redirects=True,
            headers={"Accept": "application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.5"},
        ) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}")
            document = await _read_bounded(response)
        _circuit_success(X_SEARCH_CIRCUIT_ID)
        return document
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _circuit_failure(X_SEARCH_CIRCUIT_ID)
        log.warning("X search discovery failed: %s", exc)
        return ""


async def _collect_x_via_search(session: aiohttp.ClientSession, sources: List[Dict]) -> List[Dict]:
    """Free best-effort X discovery; cached and batched to avoid hammering search."""
    global _X_SEARCH_LAST_RUN

    if not sources:
        return []

    now = time.monotonic()
    due = not _X_SEARCH_LAST_RUN or (now - _X_SEARCH_LAST_RUN) >= X_SEARCH_REFRESH_SECONDS
    if not due:
        merged = []
        for source in sources:
            cached = _x_search_cache_events(source["id"], now=now)
            merged.extend(cached)
            _record_health(
                source["id"],
                "ok" if cached else "empty",
                detail="cached search discovery",
                items=len(cached),
                transport="x_search_discovery",
                x_api_status="disabled",
            )
        return merged

    _X_SEARCH_LAST_RUN = now
    batches = [
        sources[index:index + X_SEARCH_BATCH_SIZE]
        for index in range(0, len(sources), X_SEARCH_BATCH_SIZE)
    ]

    tasks = [
        asyncio.create_task(_fetch_x_search_rss(session, _x_search_query(batch)))
        for batch in batches
    ]
    documents = await asyncio.gather(*tasks, return_exceptions=True)

    refreshed: Set[str] = set()
    merged: List[Dict] = []

    for batch, document in zip(batches, documents):
        if isinstance(document, BaseException):
            if isinstance(document, asyncio.CancelledError):
                raise document
            document = ""

        if document:
            try:
                parsed = parse_x_search_rss(document, sources=batch)
            except Exception as exc:
                log.warning("X search RSS parser failed: %s", exc)
                parsed = {}
                document = ""

        if not document:
            for source in batch:
                cached = _x_search_cache_events(source["id"], now=now)
                merged.extend(cached)
                _record_health(
                    source["id"],
                    "fetch_failed",
                    detail="search discovery unavailable; using cache" if cached else "search discovery unavailable",
                    items=len(cached),
                    transport="x_search_discovery",
                    x_api_status="disabled",
                )
            continue

        for source in batch:
            source_id = source["id"]
            events = list((parsed or {}).get(source_id) or [])
            _X_SEARCH_CACHE[source_id] = {
                "updated_monotonic": now,
                "events": [dict(event) for event in events],
            }
            refreshed.add(source_id)
            merged.extend(events)
            _record_health(
                source_id,
                "ok" if events else "empty",
                detail="X API disabled; search discovery active",
                items=len(events),
                transport="x_search_discovery",
                x_api_status="disabled",
            )

    return merged


def _event_identity(event: Dict) -> Tuple[str, str]:
    group = _clean_text(event.get("dedup_group", ""))
    content_url = _canonical_external_url(event.get("content_url", ""))
    if content_url:
        return group, content_url.lower()
    return group, _fingerprint(event.get("original_title") or event.get("title") or "")


def deduplicate_social_events(events: Iterable[Dict]) -> List[Dict]:
    chosen, by_key = [], {}
    for event in events:
        key = _event_identity(event)
        idx = by_key.get(key)
        if idx is None:
            by_key[key] = len(chosen)
            chosen.append(event)
            continue

        current = chosen[idx]
        candidate_source = get_source_by_id(event.get("source_id", ""))
        current_source = get_source_by_id(current.get("source_id", ""))
        if candidate_source and current_source and should_prefer_source(candidate_source, current_source):
            chosen[idx] = event

    return sorted(
        chosen,
        key=lambda e: (
            int(e.get("priority", 0)),
            e.get("published") or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )


async def collect_social_intel(
    *,
    source_ids: Optional[Iterable[str]] = None,
    include_support: bool = DEFAULT_INCLUDE_SUPPORT,
    include_forwarded_telegram: bool = False,
    session: Optional[aiohttp.ClientSession] = None,
) -> List[Dict]:
    requested = set(source_ids or [])
    sources = [
        source for source in get_active_sources(include_support=include_support)
        if not requested or source["id"] in requested
    ]
    if not sources:
        return []

    bearer = _x_bearer_token()

    # Without an X token, retain the verified X registry and discover only exact
    # x.com status URLs through the bounded search fallback. Telegram support
    # mirrors are added as an independent free lane, then all events are deduped.
    if not bearer and not include_support:
        primary_x = [
            source for source in sources
            if source["platform"] == PLATFORM_X
        ]
        telegram_sources = [
            source for source in get_active_sources(include_support=True)
            if (not requested or source["id"] in requested)
            and source["platform"] == PLATFORM_TELEGRAM
        ]
        sources = [*primary_x, *telegram_sources]

    async def run(active_session):
        tasks, task_sources = [], []
        x_search_sources: List[Dict] = []

        for source in sources:
            if source["platform"] == PLATFORM_TELEGRAM:
                tasks.append(
                    _collect_telegram_source(
                        active_session,
                        source,
                        include_forwarded=include_forwarded_telegram,
                    )
                )
                task_sources.append(source)
            elif source["platform"] == PLATFORM_X:
                if bearer:
                    tasks.append(_collect_x_source(active_session, source, bearer=bearer))
                    task_sources.append(source)
                else:
                    x_search_sources.append(source)

        if x_search_sources:
            tasks.append(_collect_x_via_search(active_session, x_search_sources))
            task_sources.append({"id": "x_search_discovery"})

        if not tasks:
            return []

        results = await asyncio.gather(*tasks, return_exceptions=True)
        merged = []
        for source, result in zip(task_sources, results):
            if isinstance(result, BaseException):
                if isinstance(result, asyncio.CancelledError):
                    raise result
                log.warning("isolated social failure: %s: %s", source["id"], result)
                continue
            merged.extend(result or [])
        return deduplicate_social_events(merged)

    if session is not None:
        return await run(session)

    async with aiohttp.ClientSession(
        headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/json,application/rss+xml"},
        connector=aiohttp.TCPConnector(limit=12),
    ) as owned:
        return await run(owned)


def validate_registry() -> List[str]:
    errors = []
    required = {
        "id","entity","entity_ar","organization","organization_ar","platform","handle",
        "url","grade","role","active","language","routing_hint","topics","dedup_group",
        "priority","official_proof",
    }
    seen_ids, seen_handles = set(), set()

    for source in VERIFIED_SOCIAL_SOURCES:
        source_id = source.get("id", "UNKNOWN")
        missing = required - set(source.keys())
        if missing:
            errors.append(f"{source_id}: missing fields: {sorted(missing)}")
            continue
        if source_id in seen_ids:
            errors.append(f"duplicate source id: {source_id}")
        seen_ids.add(source_id)

        key = f'{source["platform"]}:{normalize_handle(source["handle"])}'
        if key in seen_handles:
            errors.append(f"duplicate source handle: {key}")
        seen_handles.add(key)

        if source["grade"] not in VALID_GRADES:
            errors.append(f"{source_id}: invalid grade")
        if source["platform"] not in VALID_PLATFORMS:
            errors.append(f"{source_id}: invalid platform")
        if source["role"] not in VALID_ROLES:
            errors.append(f"{source_id}: invalid role")
        if source["routing_hint"] not in VALID_ROUTING_HINTS:
            errors.append(f"{source_id}: invalid routing_hint")
        if not source_has_international_value(source):
            errors.append(f"{source_id}: no international-value topic")
        if not str(source["url"]).startswith("https://"):
            errors.append(f"{source_id}: invalid source url")
        if not str(source["official_proof"]).startswith("https://"):
            errors.append(f"{source_id}: invalid official proof")
        if not isinstance(source.get("priority"), int) or not 0 <= source["priority"] <= 100:
            errors.append(f"{source_id}: invalid priority")
    return errors


def registry_stats() -> Dict[str, int]:
    active = [s for s in VERIFIED_SOCIAL_SOURCES if s.get("active")]
    return {
        "total": len(active),
        "a_plus": sum(1 for s in active if s["grade"] == GRADE_A_PLUS),
        "a": sum(1 for s in active if s["grade"] == GRADE_A),
        "x": sum(1 for s in active if s["platform"] == PLATFORM_X),
        "telegram": sum(1 for s in active if s["platform"] == PLATFORM_TELEGRAM),
        "primary": sum(1 for s in active if s["role"] == ROLE_PRIMARY),
        "support": sum(1 for s in active if s["role"] == ROLE_SUPPORT),
        "specialized": sum(1 for s in active if s["role"] == ROLE_SPECIALIZED),
    }


REGISTRY_ERRORS = validate_registry()
if REGISTRY_ERRORS:
    raise RuntimeError(
        "intel_sources registry validation failed:\n- "
        + "\n- ".join(REGISTRY_ERRORS)
    )
