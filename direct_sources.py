"""
Global-Intel-Bot
Direct Intelligence Radar Sources
=================================

مصادر الإنذار المباشر عالية القيمة. هذا المزود مستقل عن:
- intel_sources.py  -> Telegram + X
- news_engine.py    -> منظومة الأخبار الحالية
- bot.py            -> المنسق والعرض

القواعد:
1) درجة المصدر منفصلة عن درجة تأكيد الحدث.
2) لا يُحوّل البلاغ الأولي أو بلاغ الطرف الثالث إلى حقيقة مؤكدة.
3) فشل مصدر لا يوقف بقية المصادر.
4) تكرار الحدث داخل العائلة المؤسسية يُدمج، ويُفضّل المصدر الأعلى أولوية.
5) routing_hint إشارة فقط؛ التصنيف النهائي يتم لاحقًا حسب مضمون الحدث.
6) لا توجد أي عملية polling تلقائية هنا. يستدعي bot.py هذا المزود لاحقًا من مسار خلفي مستقل.
7) OFAC يُقرأ من صفحة Recent Actions الجامعة فقط لتجنب طلبات مكررة لنفس العائلة المؤسسية.
8) حساسات الزلازل والطيران تمر عبر بوابات شدة/حداثة قبل التسليم إلى bot.py لتجنب الضجيج.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import aiohttp


log = logging.getLogger(__name__)

# =========================================================
# SOURCE GRADES / ROLES / TYPES
# =========================================================

GRADE_A_PLUS = "A+"
GRADE_A = "A"
VALID_GRADES = {GRADE_A_PLUS, GRADE_A}

ROLE_PRIMARY_SENSOR = "primary_sensor"
ROLE_SPECIALIZED_SENSOR = "specialized_sensor"
ROLE_SUPPORT_SENSOR = "support_sensor"
VALID_ROLES = {
    ROLE_PRIMARY_SENSOR,
    ROLE_SPECIALIZED_SENSOR,
    ROLE_SUPPORT_SENSOR,
}

TYPE_MARITIME_SECURITY = "maritime_security"
TYPE_SANCTIONS = "sanctions"
TYPE_AVIATION_SECURITY = "aviation_security"
TYPE_SEISMIC_ALERT = "seismic_alert"
VALID_SOURCE_TYPES = {
    TYPE_MARITIME_SECURITY,
    TYPE_SANCTIONS,
    TYPE_AVIATION_SECURITY,
    TYPE_SEISMIC_ALERT,
}

# =========================================================
# EVENT STATUS
# =========================================================

STATUS_PRELIMINARY = "preliminary"
STATUS_THIRD_PARTY_REPORT = "third_party_report"
STATUS_OFFICIAL_REPORT = "official_report"
STATUS_UNDER_INVESTIGATION = "under_investigation"
STATUS_VERIFIED_REPORT = "verified_report"
STATUS_AUTHORITY_CONFIRMED = "authority_confirmed"
STATUS_ADVISORY = "advisory"

VALID_EVENT_STATUSES = {
    STATUS_PRELIMINARY,
    STATUS_THIRD_PARTY_REPORT,
    STATUS_OFFICIAL_REPORT,
    STATUS_UNDER_INVESTIGATION,
    STATUS_VERIFIED_REPORT,
    STATUS_AUTHORITY_CONFIRMED,
    STATUS_ADVISORY,
}

_EVENT_STATUS_STRENGTH = {
    STATUS_PRELIMINARY: 10,
    STATUS_THIRD_PARTY_REPORT: 20,
    STATUS_UNDER_INVESTIGATION: 30,
    STATUS_OFFICIAL_REPORT: 40,
    STATUS_ADVISORY: 50,
    STATUS_VERIFIED_REPORT: 80,
    STATUS_AUTHORITY_CONFIRMED: 100,
}

# =========================================================
# TOPICS / ROUTING
# =========================================================

RADAR_TOPICS: Set[str] = {
    "maritime_security",
    "shipping",
    "ship_attack",
    "boarding",
    "hijacking",
    "piracy",
    "navigation_risk",
    "missile",
    "drone",
    "conflict",
    "international_security",
    "sanctions",
    "financial_sanctions",
    "asset_freeze",
    "designation",
    "export_controls",
    "aviation_security",
    "conflict_zone",
    "airspace_risk",
    "earthquake",
    "tsunami",
    "natural_disaster",
}

ROUTE_DEFENSE = "defense_security"
ROUTE_ECONOMY = "economy_markets"
ROUTE_OFFICIAL = "official_statement"
ROUTE_BREAKING = "breaking"
ROUTE_DYNAMIC = "dynamic"
VALID_ROUTING_HINTS = {
    ROUTE_DEFENSE,
    ROUTE_ECONOMY,
    ROUTE_OFFICIAL,
    ROUTE_BREAKING,
    ROUTE_DYNAMIC,
}

# =========================================================
# NETWORK SAFETY
# =========================================================

MAX_RESPONSE_BYTES = 900_000
MAX_ITEMS_PER_SOURCE = 20
CIRCUIT_FAILURES = 3
CIRCUIT_COOLDOWN_SECONDS = 300
USER_AGENT = (
    "Global-Intel-Bot/1.0 "
    "(public-source monitor; contact via project operator)"
)

# process-local breaker; intentionally does not persist across deploys
_CIRCUIT_STATE: Dict[str, Dict[str, float]] = {}
_LAST_ATTEMPT: Dict[str, float] = {}
_SOURCE_HEALTH: Dict[str, Dict[str, object]] = {}
_LAST_SUCCESS: Dict[str, float] = {}

HEALTH_OK = "ok"
HEALTH_EMPTY = "empty"
HEALTH_FETCH_FAILED = "fetch_failed"
HEALTH_PARSE_FAILED = "parse_failed"
HEALTH_CIRCUIT_OPEN = "circuit_open"
HEALTH_UNKNOWN = "unknown"

def _set_source_health(source_id: str, status: str, *, detail: str = "", items: int = 0) -> None:
    now = time.monotonic()
    previous = _SOURCE_HEALTH.get(source_id, {})
    if status in {HEALTH_OK, HEALTH_EMPTY}:
        _LAST_SUCCESS[source_id] = now
    _SOURCE_HEALTH[source_id] = {
        "status": status,
        "detail": _clean_text(detail)[:240] if detail else "",
        "items": max(0, int(items or 0)),
        "updated_monotonic": now,
        "last_attempt_monotonic": _LAST_ATTEMPT.get(source_id),
        "last_success_monotonic": _LAST_SUCCESS.get(source_id),
        "consecutive_failures": int(_circuit_state(source_id)["failures"]),
        "circuit_open_until": float(_circuit_state(source_id)["open_until"]),
        "previous_status": previous.get("status", ""),
    }

def _ensure_health_entry(source_id: str) -> None:
    """Create an explicit unknown entry without overwriting a real prior state."""
    if source_id not in _SOURCE_HEALTH:
        _SOURCE_HEALTH[source_id] = {
            "status": HEALTH_UNKNOWN,
            "detail": "",
            "items": 0,
            "updated_monotonic": 0.0,
            "last_attempt_monotonic": _LAST_ATTEMPT.get(source_id),
            "last_success_monotonic": _LAST_SUCCESS.get(source_id),
            "consecutive_failures": int(_circuit_state(source_id)["failures"]),
            "circuit_open_until": float(_circuit_state(source_id)["open_until"]),
            "previous_status": "",
        }

def get_direct_source_health() -> Dict[str, Dict[str, object]]:
    """Return truthful process-local sensor health without false not-due recovery."""
    for source in get_active_direct_sources():
        _ensure_health_entry(source["id"])
    snapshot = {}
    for source_id, value in _SOURCE_HEALTH.items():
        item = dict(value)
        source = get_direct_source(source_id)
        if source:
            item["due"] = _source_due(source)
        item["consecutive_failures"] = int(_circuit_state(source_id)["failures"])
        item["circuit_open_until"] = float(_circuit_state(source_id)["open_until"])
        snapshot[source_id] = item
    return snapshot

def get_direct_network_health() -> Dict[str, object]:
    """Aggregate direct-radar health for diagnostics; no network I/O."""
    snapshot = get_direct_source_health()
    statuses = [str(v.get("status") or HEALTH_UNKNOWN) for v in snapshot.values()]
    healthy = sum(s in {HEALTH_OK, HEALTH_EMPTY} for s in statuses)
    failed = sum(s in {HEALTH_FETCH_FAILED, HEALTH_PARSE_FAILED, HEALTH_CIRCUIT_OPEN} for s in statuses)
    unknown = sum(s == HEALTH_UNKNOWN for s in statuses)
    if failed and healthy:
        state = "degraded"
    elif failed and not healthy:
        state = "unavailable"
    elif healthy:
        state = "ok"
    else:
        state = "unknown"
    return {
        "state": state,
        "active_sensors": len(snapshot),
        "healthy_sensors": healthy,
        "failed_sensors": failed,
        "unknown_sensors": unknown,
        "sensors": snapshot,
    }

def _source_due(source: Dict, *, now: Optional[float] = None) -> bool:
    now = time.monotonic() if now is None else now
    last = _LAST_ATTEMPT.get(source["id"])
    if last is None:
        return True
    interval = max(15, int(source.get("poll_interval_seconds", 60)))
    return (now - last) >= interval

# =========================================================
# DIRECT SOURCES
# =========================================================

DIRECT_SOURCES: List[Dict] = [
    {
        "id": "ukmto_warnings",
        "entity": "United Kingdom",
        "entity_ar": "المملكة المتحدة",
        "organization": "UK Maritime Trade Operations",
        "organization_ar": "عمليات التجارة البحرية البريطانية",
        "source_type": TYPE_MARITIME_SECURITY,
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY_SENSOR,
        "active": True,
        "url": "https://www.ukmto.org/ukmto-products/warnings",
        "parser": "ukmto_html",
        "official_proof": "https://www.ukmto.org/",
        "regions": {
            "Middle East", "Arabian Gulf", "Gulf of Oman", "Arabian Sea",
            "Red Sea", "Gulf of Aden", "Indian Ocean",
        },
        "topics": {
            "maritime_security", "shipping", "ship_attack", "boarding",
            "hijacking", "piracy", "navigation_risk", "missile", "drone",
            "conflict", "international_security",
        },
        "product_types": {"warning"},
        "routing_hint": ROUTE_DEFENSE,
        "dedup_group": "ukmto",
        "priority": 100,
        "poll_interval_seconds": 30,
        "timeout_seconds": 5,
        "default_event_status": STATUS_OFFICIAL_REPORT,
        "attribution_ar": "بحسب عمليات التجارة البحرية البريطانية (UKMTO)",
        "notes": (
            "مصدر أولي للأمن البحري. يجب إبقاء مصدر البلاغ وحالة التحقق "
            "منفصلين عن موثوقية UKMTO نفسها."
        ),
    },
    {
        "id": "ofac_recent_actions",
        "entity": "United States",
        "entity_ar": "الولايات المتحدة",
        "organization": "Office of Foreign Assets Control",
        "organization_ar": "مكتب مراقبة الأصول الأجنبية - وزارة الخزانة الأمريكية",
        "source_type": TYPE_SANCTIONS,
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY_SENSOR,
        "active": True,
        "url": "https://ofac.treasury.gov/recent-actions",
        "fallback_urls": (
            "https://ofac.treasury.gov/recent-actions/sanctions-list-updates",
            "https://ofac.treasury.gov/recent-actions/regulations-and-guidance",
        ),
        "parser": "ofac_html",
        "official_proof": "https://ofac.treasury.gov/",
        "regions": {"Global"},
        "topics": {
            "sanctions", "financial_sanctions", "asset_freeze",
            "designation", "international_security",
        },
        "product_types": {
            "sanctions_list_update", "general_license", "regulation",
            "guidance", "enforcement_action",
        },
        "routing_hint": ROUTE_ECONOMY,
        "dedup_group": "ofac",
        "priority": 100,
        "poll_interval_seconds": 60,
        "timeout_seconds": 7,
        "per_url_timeout_seconds": 2.2,
        "default_event_status": STATUS_AUTHORITY_CONFIRMED,
        "attribution_ar": (
            "بحسب مكتب مراقبة الأصول الأجنبية بوزارة الخزانة الأمريكية"
        ),
        "notes": (
            "المصدر الأولي للعقوبات والتعيينات والتحديثات المرتبطة بـOFAC."
        ),
    },
    {
        "id": "usgs_earthquakes",
        "entity": "United States",
        "entity_ar": "الولايات المتحدة",
        "organization": "U.S. Geological Survey",
        "organization_ar": "هيئة المسح الجيولوجي الأمريكية",
        "source_type": TYPE_SEISMIC_ALERT,
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY_SENSOR,
        "active": True,
        "url": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_hour.geojson",
        "parser": "usgs_geojson",
        "official_proof": "https://earthquake.usgs.gov/earthquakes/feed/",
        "regions": {"Global"},
        "topics": {"earthquake", "tsunami", "natural_disaster"},
        "product_types": {"earthquake_event"},
        "routing_hint": ROUTE_BREAKING,
        "dedup_group": "global_earthquake",
        "priority": 100,
        "poll_interval_seconds": 60,
        "timeout_seconds": 4,
        "default_event_status": STATUS_VERIFIED_REPORT,
        "attribution_ar": "بحسب هيئة المسح الجيولوجي الأمريكية (USGS)",
        "notes": (
            "رادار زلازل عالمي لحظي. لا يُنشر كل حدث؛ تمر فقط الأحداث "
            "ذات القيمة الإخبارية أو مؤشرات التسونامي/الخطورة."
        ),
    },
    {
        "id": "gdacs_earthquakes",
        "entity": "International",
        "entity_ar": "دولي",
        "organization": "Global Disaster Alert and Coordination System",
        "organization_ar": "النظام العالمي للإنذار والتنسيق في حالات الكوارث (GDACS)",
        "source_type": TYPE_SEISMIC_ALERT,
        "grade": GRADE_A,
        "role": ROLE_SPECIALIZED_SENSOR,
        "active": True,
        "url": "https://www.gdacs.org/contentdata/xml/rss_eq_24h.xml",
        "parser": "gdacs_rss",
        "official_proof": "https://www.gdacs.org/feed_reference.aspx",
        "regions": {"Global"},
        "topics": {"earthquake", "tsunami", "natural_disaster"},
        "product_types": {"disaster_alert"},
        "routing_hint": ROUTE_BREAKING,
        "dedup_group": "global_earthquake",
        "priority": 92,
        "poll_interval_seconds": 360,
        "timeout_seconds": 5,
        "default_event_status": STATUS_VERIFIED_REPORT,
        "attribution_ar": "بحسب النظام العالمي للإنذار والتنسيق في حالات الكوارث (GDACS)",
        "notes": (
            "حساس تأكيد دولي عالي الإشارة. تُقبل التنبيهات البرتقالية والحمراء "
            "للزلازل فقط لتجنب الضجيج."
        ),
    },
    {
        "id": "easa_conflict_zones",
        "entity": "European Union",
        "entity_ar": "الاتحاد الأوروبي",
        "organization": "European Union Aviation Safety Agency",
        "organization_ar": "وكالة سلامة الطيران التابعة للاتحاد الأوروبي (EASA)",
        "source_type": TYPE_AVIATION_SECURITY,
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY_SENSOR,
        "active": True,
        "url": "https://www.easa.europa.eu/en/domains/air-operations/czibs/export-json?_format=json&page=",
        "parser": "easa_json",
        "official_proof": "https://www.easa.europa.eu/en/domains/air-operations/czibs",
        "regions": {"Global"},
        "topics": {"aviation_security", "conflict_zone", "airspace_risk", "international_security"},
        "product_types": {"conflict_zone_advisory"},
        "routing_hint": ROUTE_DEFENSE,
        "dedup_group": "easa_conflict_zones",
        "priority": 98,
        "poll_interval_seconds": 300,
        "timeout_seconds": 5,
        "default_event_status": STATUS_ADVISORY,
        "max_event_age_seconds": 7 * 24 * 3600,
        "attribution_ar": "بحسب وكالة سلامة الطيران التابعة للاتحاد الأوروبي (EASA)",
        "notes": (
            "يرصد الإصدارات والتحديثات الحديثة فقط لنشرات مناطق النزاع الجوي، "
            "ولا يعيد نشر القائمة التاريخية النشطة كاملة."
        ),
    },
]

DIRECT_SOURCE_BY_ID: Dict[str, Dict] = {
    source["id"]: source for source in DIRECT_SOURCES
}

# =========================================================
# REGISTRY HELPERS
# =========================================================

def get_direct_source(source_id: str) -> Optional[Dict]:
    return DIRECT_SOURCE_BY_ID.get(source_id)


def get_active_direct_sources() -> List[Dict]:
    return sorted(
        (s for s in DIRECT_SOURCES if s.get("active", False)),
        key=lambda s: (s.get("priority", 0), s.get("grade") == GRADE_A_PLUS),
        reverse=True,
    )


def get_sources_by_type(source_type: str) -> List[Dict]:
    return [
        s for s in get_active_direct_sources()
        if s.get("source_type") == source_type
    ]


def get_sources_for_topic(topic: str) -> List[Dict]:
    return [
        s for s in get_active_direct_sources()
        if topic in s.get("topics", set())
    ]


def get_dedup_group(source_id: str) -> Optional[str]:
    source = get_direct_source(source_id)
    return source.get("dedup_group") if source else None


def same_source_family(source_id_a: str, source_id_b: str) -> bool:
    a = get_dedup_group(source_id_a)
    b = get_dedup_group(source_id_b)
    return bool(a and b and a == b)


def should_prefer_direct_source(
    candidate_source_id: str,
    current_source_id: str,
) -> bool:
    candidate = get_direct_source(candidate_source_id)
    current = get_direct_source(current_source_id)
    if not candidate:
        return False
    if not current:
        return True

    cp = candidate.get("priority", 0)
    xp = current.get("priority", 0)
    if cp != xp:
        return cp > xp

    cg = candidate.get("grade")
    xg = current.get("grade")
    if cg != xg:
        return cg == GRADE_A_PLUS

    return False


def event_status_strength(status: str) -> int:
    return _EVENT_STATUS_STRENGTH.get(status, 0)


def stronger_event_status(candidate_status: str, current_status: str) -> bool:
    return event_status_strength(candidate_status) > event_status_strength(current_status)


# =========================================================
# NORMALIZED EVENT
# =========================================================

def build_radar_event(
    *,
    source_id: str,
    title: str,
    url: str,
    published=None,
    summary: str = "",
    event_status: Optional[str] = None,
    original_title: str = "",
    external_id: str = "",
    region: str = "",
    metadata: Optional[Dict] = None,
) -> Dict:
    source = get_direct_source(source_id)
    if not source:
        raise ValueError(f"Unknown direct source: {source_id}")

    status = (
        event_status
        or source.get("default_event_status")
        or STATUS_PRELIMINARY
    )
    if status not in VALID_EVENT_STATUSES:
        raise ValueError(f"Invalid event status: {status}")

    return {
        "provider": "direct_radar",
        "source_id": source_id,
        "source_name": source.get("organization"),
        "source_name_ar": source.get("organization_ar"),
        "source_grade": source.get("grade"),
        "source_type": source.get("source_type"),
        "source_role": source.get("role"),
        "dedup_group": source.get("dedup_group"),
        "priority": source.get("priority", 0),
        "routing_hint": source.get("routing_hint", ROUTE_DYNAMIC),
        "attribution_ar": source.get("attribution_ar", ""),
        "title": _clean_text(title),
        "original_title": _clean_text(original_title or title),
        "summary": _clean_text(summary),
        "url": _canonical_url(url),
        "published": published,
        "region": _clean_text(region),
        "external_id": _clean_text(external_id),
        "event_status": status,
        "event_status_strength": event_status_strength(status),
        "metadata": dict(metadata or {}),
    }


def attribution_prefix(source_id: str) -> str:
    source = get_direct_source(source_id)
    return _clean_text(source.get("attribution_ar", "")) if source else ""


def event_status_ar(status: str) -> str:
    return {
        STATUS_PRELIMINARY: "معلومات أولية",
        STATUS_THIRD_PARTY_REPORT: "بلاغ من طرف ثالث",
        STATUS_OFFICIAL_REPORT: "بلاغ رسمي",
        STATUS_UNDER_INVESTIGATION: "قيد التحقيق",
        STATUS_VERIFIED_REPORT: "بلاغ متحقق منه",
        STATUS_AUTHORITY_CONFIRMED: "مؤكد من الجهة المختصة",
        STATUS_ADVISORY: "تحذير/إرشاد رسمي",
    }.get(status, "حالة غير محددة")


# =========================================================
# HTML / TEXT NORMALIZATION
# =========================================================

_WS = re.compile(r"\s+")
_TAG = re.compile(r"<[^>]+>")
UKMTO_ID_RE = re.compile(
    r"\b(?:UKMTO\s*)?#?\s*(\d{1,4})(?:[-/]\d{2,4})?\b",
    re.IGNORECASE,
)


def _clean_text(value: object) -> str:
    text = html.unescape(str(value or ""))
    text = _TAG.sub(" ", text)
    return _WS.sub(" ", text).strip()


def _canonical_url(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    p = urlparse(value)
    if not p.scheme or not p.netloc:
        return value
    # Remove fragment only; query may be semantically meaningful on source sites.
    return urlunparse((p.scheme.lower(), p.netloc.lower(), p.path, p.params, p.query, ""))


def _fingerprint_text(text: str) -> str:
    t = _clean_text(text).lower()
    t = re.sub(r"[^\w\u0600-\u06ff]+", " ", t)
    t = _WS.sub(" ", t).strip()
    return hashlib.sha1(t.encode("utf-8", "ignore")).hexdigest()


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.anchors: List[Tuple[str, str]] = []
        self._href: Optional[str] = None
        self._parts: List[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if href is not None:
            self._href = href
            self._parts = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._href is not None:
            self.anchors.append((self._href, _clean_text(" ".join(self._parts))))
            self._href = None
            self._parts = []


def _anchors(document: str) -> List[Tuple[str, str]]:
    parser = _AnchorParser()
    try:
        parser.feed(document or "")
    except Exception:
        return []
    return parser.anchors



def _parse_datetime(value: object) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = _clean_text(value)
        if not raw:
            return None
        time_match = re.search(r'datetime=["\']([^"\']+)["\']', raw, flags=re.I)
        if time_match:
            raw = time_match.group(1)
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = parsedate_to_datetime(raw)
            except (TypeError, ValueError, OverflowError):
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _event_is_recent(value: object, max_age_seconds: int) -> bool:
    published = _parse_datetime(value)
    if published is None:
        return False
    age = (datetime.now(timezone.utc) - published).total_seconds()
    return -300 <= age <= max_age_seconds


def _xml_local_name(tag: str) -> str:
    return str(tag or "").rsplit("}", 1)[-1].lower()


def _xml_child_text(node: ET.Element, *names: str) -> str:
    wanted = {str(name).lower() for name in names}
    for child in list(node):
        if _xml_local_name(child.tag) in wanted:
            return _clean_text("".join(child.itertext()))
    return ""


# =========================================================
# UKMTO PARSING
# =========================================================

def infer_ukmto_status(text: str) -> Tuple[str, Dict]:
    low = _clean_text(text).lower()

    under_investigation = any(
        phrase in low
        for phrase in (
            "authorities are investigating",
            "authorities investigating",
            "investigation is ongoing",
            "investigations are ongoing",
            "under investigation",
        )
    )

    metadata = {"under_investigation": under_investigation}

    # Preserve provenance first. "Third party" must not be upgraded merely
    # because an authority is investigating the report.
    if "third party" in low or "third-party" in low:
        return STATUS_THIRD_PARTY_REPORT, metadata

    if any(
        phrase in low
        for phrase in (
            "verified report",
            "report has been verified",
            "ukmto has verified",
        )
    ):
        return STATUS_VERIFIED_REPORT, metadata

    if any(
        phrase in low
        for phrase in (
            "advisory",
            "unable to confirm",
            "exercise caution",
            "vessels are advised",
        )
    ):
        return STATUS_ADVISORY, metadata

    return STATUS_OFFICIAL_REPORT, metadata


def parse_ukmto_html(
    document: str,
    *,
    source_id: str = "ukmto_warnings",
    base_url: str = "https://www.ukmto.org/",
) -> List[Dict]:
    """
    Conservative public-HTML parser.
    It emits only links/text that visibly resemble a UKMTO warning/incident.
    If the public page renders no products in HTML, it safely returns [].
    """
    if not document:
        return []

    events: List[Dict] = []
    seen = set()

    for href, label in _anchors(document):
        absolute = urljoin(base_url, href)
        low_url = absolute.lower()
        text = _clean_text(label)

        relevant_url = any(
            token in low_url
            for token in (
                "/recent-incidents",
                "/ukmto-products/warnings",
                "/warning",
                "/incident",
            )
        )

        low_text = text.lower()
        relevant_text = any(
            token in low_text
            for token in (
                "ukmto",
                "attack",
                "warning",
                "advisory",
                "hijack",
                "boarding",
                "incident",
            )
        )

        if not relevant_url or not relevant_text or len(text) < 8:
            continue

        id_match = UKMTO_ID_RE.search(text)
        external_id = id_match.group(1) if id_match else ""

        status, metadata = infer_ukmto_status(text)
        key = external_id or _fingerprint_text(text)
        if key in seen:
            continue
        seen.add(key)

        events.append(
            build_radar_event(
                source_id=source_id,
                title=text,
                original_title=text,
                summary=text,
                url=absolute,
                external_id=external_id,
                event_status=status,
                metadata=metadata,
            )
        )

        if len(events) >= MAX_ITEMS_PER_SOURCE:
            break

    return events


# =========================================================
# OFAC PARSING
# =========================================================

_OFAC_ACTION_PATH = re.compile(
    r"/recent-actions/(?!sanctions-list-updates/?$)[^?#]+",
    re.IGNORECASE,
)

def parse_ofac_html(document: str, *, source_id: str) -> List[Dict]:
    if source_id != "ofac_recent_actions":
        raise ValueError(f"Unsupported OFAC source: {source_id}")
    if not document:
        return []

    source = get_direct_source(source_id)
    base_url = source["url"] if source else "https://ofac.treasury.gov/"

    events: List[Dict] = []
    seen = set()

    for href, label in _anchors(document):
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.netloc.lower() != "ofac.treasury.gov":
            continue

        text = _clean_text(label)
        if len(text) < 8:
            continue

        path = parsed.path.rstrip("/")
        low_path = path.lower()

        is_action = bool(_OFAC_ACTION_PATH.search(path))
        if not is_action:
            continue

        canonical = _canonical_url(absolute)
        key = canonical or _fingerprint_text(text)
        if key in seen:
            continue
        seen.add(key)

        external_id = parsed.path.rstrip("/").split("/")[-1]

        events.append(
            build_radar_event(
                source_id=source_id,
                title=text,
                original_title=text,
                summary="",
                url=canonical,
                external_id=external_id,
                event_status=STATUS_AUTHORITY_CONFIRMED,
                metadata={},
            )
        )

        if len(events) >= MAX_ITEMS_PER_SOURCE:
            break

    return events



# =========================================================
# EARLY-WARNING PARSERS
# =========================================================

def parse_usgs_geojson(document: str, *, source_id: str) -> List[Dict]:
    if source_id != "usgs_earthquakes":
        raise ValueError(f"Unsupported USGS source: {source_id}")
    payload = json.loads(document)
    if payload.get("type") != "FeatureCollection" or not isinstance(payload.get("features"), list):
        raise ValueError("invalid USGS GeoJSON")

    events: List[Dict] = []
    for feature in payload["features"]:
        if not isinstance(feature, dict):
            continue
        props = feature.get("properties") or {}
        if not isinstance(props, dict):
            continue

        try:
            magnitude = float(props.get("mag"))
        except (TypeError, ValueError):
            continue
        alert = _clean_text(props.get("alert")).lower()
        tsunami = int(props.get("tsunami") or 0)
        significance = int(props.get("sig") or 0)

        # High-signal gate: no routine M4.5 noise.
        if not (
            magnitude >= 5.5
            or tsunami == 1
            or alert in {"orange", "red"}
            or significance >= 600
        ):
            continue

        epoch_ms = props.get("time")
        published = None
        if isinstance(epoch_ms, (int, float)):
            published = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
        if published is None or not _event_is_recent(published, 2 * 3600):
            continue

        place = _clean_text(props.get("place")) or "location not specified"
        url = _canonical_url(props.get("url"))
        event_id = _clean_text(feature.get("id") or props.get("code"))
        geometry = feature.get("geometry") or {}
        coordinates = geometry.get("coordinates") if isinstance(geometry, dict) else []
        depth_km = None
        if isinstance(coordinates, list) and len(coordinates) >= 3:
            try:
                depth_km = round(float(coordinates[2]), 1)
            except (TypeError, ValueError):
                depth_km = None

        title = f"Earthquake M{magnitude:.1f} - {place}"
        summary_parts = [f"USGS magnitude {magnitude:.1f}"]
        if depth_km is not None:
            summary_parts.append(f"depth {depth_km:g} km")
        if tsunami == 1:
            summary_parts.append("tsunami flag: yes")
        if alert:
            summary_parts.append(f"alert level: {alert}")
        summary_parts.append(f"significance: {significance}")

        events.append(
            build_radar_event(
                source_id=source_id,
                title=title,
                original_title=title,
                summary="; ".join(summary_parts),
                url=url or get_direct_source(source_id)["official_proof"],
                published=published,
                external_id=event_id,
                region=place,
                event_status=STATUS_VERIFIED_REPORT,
                metadata={
                    "magnitude": magnitude,
                    "alert": alert,
                    "tsunami": tsunami,
                    "significance": significance,
                    "depth_km": depth_km,
                },
            )
        )
        if len(events) >= MAX_ITEMS_PER_SOURCE:
            break
    return events


def parse_gdacs_rss(document: str, *, source_id: str) -> List[Dict]:
    if source_id != "gdacs_earthquakes":
        raise ValueError(f"Unsupported GDACS source: {source_id}")
    root = ET.fromstring(document)
    events: List[Dict] = []

    for item in root.iter():
        if _xml_local_name(item.tag) != "item":
            continue

        event_type = _xml_child_text(item, "eventtype").upper()
        alert_level = _xml_child_text(item, "alertlevel").lower()
        if event_type and event_type != "EQ":
            continue
        if alert_level not in {"orange", "red"}:
            continue

        title = _xml_child_text(item, "title")
        link = _canonical_url(_xml_child_text(item, "link"))
        description = _xml_child_text(item, "description")
        published_raw = _xml_child_text(item, "pubdate", "fromdate", "todate")
        published = _parse_datetime(published_raw)
        if published is None or not _event_is_recent(published, 30 * 3600):
            continue

        event_id = _xml_child_text(item, "eventid")
        episode_id = _xml_child_text(item, "episodeid")
        country = _xml_child_text(item, "country")
        if not title:
            title = f"Earthquake - {country or 'GDACS alert'}"
        elif "earthquake" not in title.lower():
            title = f"Earthquake - {title}"

        external_id = ":".join(part for part in (event_id, episode_id) if part)
        events.append(
            build_radar_event(
                source_id=source_id,
                title=title,
                original_title=title,
                summary=description,
                url=link or get_direct_source(source_id)["official_proof"],
                published=published,
                external_id=external_id,
                region=country,
                event_status=STATUS_VERIFIED_REPORT,
                metadata={
                    "alert_level": alert_level,
                    "event_type": event_type or "EQ",
                },
            )
        )
        if len(events) >= MAX_ITEMS_PER_SOURCE:
            break
    return events


def parse_easa_json(document: str, *, source_id: str) -> List[Dict]:
    if source_id != "easa_conflict_zones":
        raise ValueError(f"Unsupported EASA source: {source_id}")
    payload = json.loads(document)
    zones = payload.get("conflict_zones")
    if not isinstance(zones, list):
        raise ValueError("invalid EASA conflict-zones JSON")

    source = get_direct_source(source_id) or {}
    max_age = int(source.get("max_event_age_seconds", 7 * 24 * 3600))
    events: List[Dict] = []

    for zone in zones:
        if not isinstance(zone, dict):
            continue
        status = _clean_text(zone.get("status"))
        if status.lower() != "active":
            continue

        published = _parse_datetime(zone.get("updated")) or _parse_datetime(zone.get("issued_date"))
        if published is None or not _event_is_recent(published, max_age):
            continue

        name = _clean_text(zone.get("name"))
        country = _clean_text(zone.get("country"))
        nid = _clean_text(zone.get("Nid") or zone.get("nid"))
        if not name:
            continue

        title = f"EASA conflict-zone airspace advisory: {name}"
        summary = f"Status: {status}"
        valid_until = _clean_text(zone.get("valid_until_date"))
        if valid_until:
            summary += f"; valid until {valid_until}"

        events.append(
            build_radar_event(
                source_id=source_id,
                title=title,
                original_title=title,
                summary=summary,
                url=source.get("official_proof", ""),
                published=published,
                external_id=nid,
                region=country or name,
                event_status=STATUS_ADVISORY,
                metadata={
                    "status": status,
                    "valid_until": valid_until,
                    "coordinates": _clean_text(zone.get("coordinates")),
                },
            )
        )
        if len(events) >= MAX_ITEMS_PER_SOURCE:
            break
    return events


def _parse_source_document(source: Dict, document: str) -> List[Dict]:
    parser_name = source.get("parser")
    source_id = source["id"]
    if parser_name == "ukmto_html":
        return parse_ukmto_html(document, source_id=source_id, base_url=source["url"])
    if parser_name == "ofac_html":
        return parse_ofac_html(document, source_id=source_id)
    if parser_name == "usgs_geojson":
        return parse_usgs_geojson(document, source_id=source_id)
    if parser_name == "gdacs_rss":
        return parse_gdacs_rss(document, source_id=source_id)
    if parser_name == "easa_json":
        return parse_easa_json(document, source_id=source_id)
    raise ValueError(f"Unsupported direct parser: {parser_name}")


# =========================================================
# CIRCUIT BREAKER
# =========================================================

def _circuit_state(source_id: str) -> Dict[str, float]:
    return _CIRCUIT_STATE.setdefault(
        source_id,
        {"failures": 0.0, "open_until": 0.0},
    )


def _circuit_is_open(source_id: str, now: Optional[float] = None) -> bool:
    now = time.monotonic() if now is None else now
    state = _circuit_state(source_id)
    return state["open_until"] > now


def _circuit_success(source_id: str) -> None:
    state = _circuit_state(source_id)
    state["failures"] = 0.0
    state["open_until"] = 0.0


def _circuit_failure(source_id: str) -> None:
    state = _circuit_state(source_id)
    state["failures"] += 1.0
    if state["failures"] >= CIRCUIT_FAILURES:
        state["open_until"] = time.monotonic() + CIRCUIT_COOLDOWN_SECONDS


def reset_circuit_breakers() -> None:
    _CIRCUIT_STATE.clear()
    _LAST_ATTEMPT.clear()
    _LAST_SUCCESS.clear()
    _SOURCE_HEALTH.clear()


# =========================================================
# HTTP FETCH
# =========================================================

async def _read_bounded(response: aiohttp.ClientResponse) -> str:
    chunks: List[bytes] = []
    size = 0

    async for chunk in response.content.iter_chunked(64 * 1024):
        size += len(chunk)
        if size > MAX_RESPONSE_BYTES:
            raise ValueError("response exceeds MAX_RESPONSE_BYTES")
        chunks.append(chunk)

    raw = b"".join(chunks)
    charset = response.charset or "utf-8"
    try:
        return raw.decode(charset, errors="replace")
    except LookupError:
        return raw.decode("utf-8", errors="replace")


async def _fetch_text(
    session: aiohttp.ClientSession,
    source: Dict,
) -> str:
    source_id = source["id"]
    if _circuit_is_open(source_id):
        return ""

    total_timeout = float(source.get("timeout_seconds", 5))
    per_url_timeout = float(
        source.get("per_url_timeout_seconds", min(2.5, total_timeout))
    )
    urls = [source["url"], *list(source.get("fallback_urls") or ())]
    started = time.monotonic()
    last_exc: Optional[BaseException] = None

    for url in urls:
        remaining = total_timeout - (time.monotonic() - started)
        if remaining <= 0.2:
            break

        attempt_timeout = max(0.2, min(per_url_timeout, remaining))
        timeout = aiohttp.ClientTimeout(
            total=attempt_timeout,
            connect=min(1.25, attempt_timeout),
            sock_connect=min(1.25, attempt_timeout),
            sock_read=max(0.2, min(1.8, attempt_timeout)),
        )

        try:
            async with session.get(
                url,
                timeout=timeout,
                allow_redirects=True,
            ) as response:
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}")
                document = await _read_bounded(response)
            if document:
                return document
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_exc = exc
            continue

    _circuit_failure(source_id)
    detail = type(last_exc).__name__ if last_exc else "all_routes_failed"
    if last_exc and str(last_exc):
        detail = f"{detail}: {last_exc}"
    _set_source_health(source_id, HEALTH_FETCH_FAILED, detail=detail)
    log.warning("direct source failed after all official routes: %s: %s", source_id, detail)
    return ""


def _document_matches_source(source: Dict, document: str) -> bool:
    """Reject HTTP-200 challenge/error pages before treating a sensor as healthy."""
    low = (document or "").lower()
    if not low:
        return False

    parser_name = source.get("parser")
    if parser_name == "ukmto_html":
        return "ukmto" in low and any(token in low for token in ("warning", "incident", "maritime"))
    if parser_name == "ofac_html":
        return "ofac" in low and any(token in low for token in ("recent actions", "sanctions", "treasury"))
    if parser_name == "usgs_geojson":
        return '"featurecollection"' in low and '"features"' in low
    if parser_name == "gdacs_rss":
        return "<rss" in low and ("gdacs" in low or "<channel" in low)
    if parser_name == "easa_json":
        return '"conflict_zones"' in low
    return False


async def _collect_one(
    session: aiohttp.ClientSession,
    source: Dict,
) -> List[Dict]:
    source_id = source["id"]
    if _circuit_is_open(source_id):
        _set_source_health(source_id, HEALTH_CIRCUIT_OPEN, detail="cooldown")
        return []

    _LAST_ATTEMPT[source_id] = time.monotonic()
    timeout_seconds = float(source.get("timeout_seconds", 5))
    try:
        document = await asyncio.wait_for(
            _fetch_text(session, source),
            timeout=timeout_seconds + 0.5,
        )
    except asyncio.TimeoutError:
        _circuit_failure(source_id)
        _set_source_health(source_id, HEALTH_FETCH_FAILED, detail="hard_timeout")
        log.warning("direct source hard timeout: %s", source_id)
        return []
    if not document:
        return []

    if not _document_matches_source(source, document):
        _circuit_failure(source_id)
        _set_source_health(source_id, HEALTH_PARSE_FAILED, detail="unexpected_document_shape")
        log.warning("direct source returned unexpected document shape: %s", source_id)
        return []

    try:
        events = _parse_source_document(source, document)

        _circuit_success(source_id)
        _set_source_health(
            source_id,
            HEALTH_OK if events else HEALTH_EMPTY,
            items=len(events),
        )
        return events
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _circuit_failure(source_id)
        _set_source_health(source_id, HEALTH_PARSE_FAILED, detail=type(exc).__name__)
        # Parsing failure is isolated from the other sensors.
        log.warning("direct parser failed: %s: %s", source_id, exc)
        return []


# =========================================================
# DEDUP
# =========================================================

def _event_identity(event: Dict) -> Tuple[str, str]:
    group = _clean_text(event.get("dedup_group", ""))
    external_id = _clean_text(event.get("external_id", "")).lower()
    url = _canonical_url(event.get("url", ""))

    if external_id:
        # For OFAC, the terminal detail slug is stable across listing paths.
        if group == "ofac":
            return group, external_id
        return group, external_id

    if url:
        return group, url.lower()

    return group, _fingerprint_text(event.get("original_title") or event.get("title", ""))


def _near_duplicate_title(a: Dict, b: Dict) -> bool:
    if a.get("dedup_group") != b.get("dedup_group"):
        return False
    ta = _clean_text(a.get("original_title") or a.get("title", "")).lower()
    tb = _clean_text(b.get("original_title") or b.get("title", "")).lower()
    return bool(ta and tb and _fingerprint_text(ta) == _fingerprint_text(tb))


def deduplicate_radar_events(events: Iterable[Dict]) -> List[Dict]:
    chosen: List[Dict] = []
    key_to_index: Dict[Tuple[str, str], int] = {}

    for event in events:
        key = _event_identity(event)
        idx = key_to_index.get(key)

        if idx is None:
            # second line of defence for same-family mirrors with differing URLs
            duplicate_idx = None
            for i, current in enumerate(chosen):
                if _near_duplicate_title(event, current):
                    duplicate_idx = i
                    break
            idx = duplicate_idx

        if idx is None:
            key_to_index[key] = len(chosen)
            chosen.append(event)
            continue

        current = chosen[idx]
        replace = should_prefer_direct_source(
            event.get("source_id", ""),
            current.get("source_id", ""),
        )

        if not replace and stronger_event_status(
            event.get("event_status", ""),
            current.get("event_status", ""),
        ):
            replace = True

        if replace:
            merged = dict(event)
            merged_meta = dict(current.get("metadata") or {})
            merged_meta.update(event.get("metadata") or {})
            merged["metadata"] = merged_meta
            chosen[idx] = merged

    return sorted(
        chosen,
        key=lambda e: (
            int(e.get("priority", 0)),
            int(e.get("event_status_strength", 0)),
        ),
        reverse=True,
    )


def _log_direct_health_summary() -> None:
    health = get_direct_network_health()
    sensors = health["sensors"]
    compact = " ".join(
        f"{source['id']}={sensors.get(source['id'], {}).get('status', HEALTH_UNKNOWN)}"
        for source in get_active_direct_sources()
    )
    log.info(
        "Direct radar sensors state=%s healthy=%d/%d %s",
        health["state"],
        health["healthy_sensors"],
        health["active_sensors"],
        compact,
    )


# =========================================================
# PUBLIC COLLECTOR
# =========================================================

async def collect_direct_radar(
    *,
    source_ids: Optional[Iterable[str]] = None,
    session: Optional[aiohttp.ClientSession] = None,
    force: bool = False,
) -> List[Dict]:
    """
    Collect all selected direct sensors concurrently.

    No exception from one source escapes and blocks the others.
    If session is supplied, the caller owns it.
    """
    selected_ids = set(source_ids or [])
    candidates = [
        s for s in get_active_direct_sources()
        if not selected_ids or s["id"] in selected_ids
    ]
    if force:
        sources = candidates
    else:
        sources = [s for s in candidates if _source_due(s)]
        for source in candidates:
            _ensure_health_entry(source["id"])

    if not sources:
        _log_direct_health_summary()
        return []

    async def run(active_session: aiohttp.ClientSession) -> List[Dict]:
        results = await asyncio.gather(
            *(_collect_one(active_session, source) for source in sources),
            return_exceptions=True,
        )

        merged: List[Dict] = []
        for source, result in zip(sources, results):
            if isinstance(result, BaseException):
                if isinstance(result, asyncio.CancelledError):
                    raise result
                log.warning("direct source isolated failure: %s: %s", source["id"], result)
                continue
            merged.extend(result or [])

        output = deduplicate_radar_events(merged)
        _log_direct_health_summary()
        return output

    if session is not None:
        return await run(session)

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,application/geo+json,application/xml,text/xml,text/html;q=0.8,*/*;q=0.5",
    }
    connector = aiohttp.TCPConnector(limit=max(4, len(sources) * 2))
    async with aiohttp.ClientSession(headers=headers, connector=connector) as owned:
        return await run(owned)


# =========================================================
# VALIDATION / STATS
# =========================================================

def validate_direct_sources() -> List[str]:
    errors: List[str] = []
    seen_ids = set()

    required_fields = {
        "id", "entity", "organization", "source_type", "grade", "role",
        "active", "url", "official_proof", "parser", "topics", "dedup_group",
        "priority", "default_event_status",
    }

    for source in DIRECT_SOURCES:
        source_id = source.get("id", "<missing-id>")
        missing = required_fields - set(source.keys())
        if missing:
            errors.append(f"{source_id}: missing fields: {sorted(missing)}")

        if source_id in seen_ids:
            errors.append(f"{source_id}: duplicate source id")
        seen_ids.add(source_id)

        if source.get("grade") not in VALID_GRADES:
            errors.append(f"{source_id}: invalid grade")
        if source.get("role") not in VALID_ROLES:
            errors.append(f"{source_id}: invalid role")
        if source.get("source_type") not in VALID_SOURCE_TYPES:
            errors.append(f"{source_id}: invalid source_type")
        for candidate_url in [source.get("url"), *list(source.get("fallback_urls") or ())]:
            parsed = urlparse(str(candidate_url or ""))
            if parsed.scheme != "https" or not parsed.netloc:
                errors.append(f"{source_id}: invalid HTTPS source URL: {candidate_url}")
        if not source.get("parser"):
            errors.append(f"{source_id}: missing parser")
        if source.get("parser") not in {"ukmto_html", "ofac_html", "usgs_geojson", "gdacs_rss", "easa_json"}:
            errors.append(f"{source_id}: invalid parser")
        if source.get("routing_hint") not in VALID_ROUTING_HINTS:
            errors.append(f"{source_id}: invalid routing_hint")
        if source.get("default_event_status") not in VALID_EVENT_STATUSES:
            errors.append(f"{source_id}: invalid default_event_status")

        unknown_topics = set(source.get("topics", set())) - RADAR_TOPICS
        if unknown_topics:
            errors.append(f"{source_id}: unknown topics: {sorted(unknown_topics)}")

        priority = source.get("priority")
        if not isinstance(priority, int) or not 0 <= priority <= 100:
            errors.append(f"{source_id}: priority must be 0..100")

        poll = source.get("poll_interval_seconds", 0)
        if not isinstance(poll, int) or poll < 15:
            errors.append(f"{source_id}: poll_interval_seconds too low/invalid")

        timeout = source.get("timeout_seconds", 0)
        if not isinstance(timeout, (int, float)) or timeout <= 0 or timeout > 15:
            errors.append(f"{source_id}: invalid timeout_seconds")

        for field in ("url", "official_proof"):
            value = str(source.get(field, ""))
            if not value.startswith("https://"):
                errors.append(f"{source_id}: {field} must use HTTPS")

    return errors


def registry_stats() -> Dict[str, int]:
    active = [s for s in DIRECT_SOURCES if s.get("active")]
    return {
        "total": len(DIRECT_SOURCES),
        "active": len(active),
        "a_plus": sum(1 for s in active if s.get("grade") == GRADE_A_PLUS),
        "a": sum(1 for s in active if s.get("grade") == GRADE_A),
        "maritime": sum(1 for s in active if s.get("source_type") == TYPE_MARITIME_SECURITY),
        "sanctions": sum(1 for s in active if s.get("source_type") == TYPE_SANCTIONS),
        "aviation": sum(1 for s in active if s.get("source_type") == TYPE_AVIATION_SECURITY),
        "seismic": sum(1 for s in active if s.get("source_type") == TYPE_SEISMIC_ALERT),
        "aviation": sum(1 for s in active if s.get("source_type") == TYPE_AVIATION_SECURITY),
    }


REGISTRY_ERRORS = validate_direct_sources()
if REGISTRY_ERRORS:
    raise RuntimeError(
        "direct_sources.py registry validation failed:\n- "
        + "\n- ".join(REGISTRY_ERRORS)
    )
