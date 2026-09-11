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
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import logging
import re
import time
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
TYPE_NUCLEAR_SECURITY = "nuclear_security"
TYPE_ENERGY_SECURITY = "energy_security"
TYPE_SECURITY_ADVISORY = "security_advisory"
VALID_SOURCE_TYPES = {
    TYPE_MARITIME_SECURITY,
    TYPE_SANCTIONS,
    TYPE_AVIATION_SECURITY,
    TYPE_NUCLEAR_SECURITY,
    TYPE_ENERGY_SECURITY,
    TYPE_SECURITY_ADVISORY,
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
    "nuclear_security",
    "nuclear_safety",
    "energy_security",
    "strategic_energy",
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

HEALTH_OK = "ok"
HEALTH_EMPTY = "empty"
HEALTH_FETCH_FAILED = "fetch_failed"
HEALTH_PARSE_FAILED = "parse_failed"
HEALTH_CIRCUIT_OPEN = "circuit_open"
HEALTH_NOT_DUE = "not_due"

def _set_source_health(source_id: str, status: str, *, detail: str = "", items: int = 0) -> None:
    _SOURCE_HEALTH[source_id] = {
        "status": status,
        "detail": _clean_text(detail)[:240] if detail else "",
        "items": max(0, int(items or 0)),
        "updated_monotonic": time.monotonic(),
    }

def get_direct_source_health() -> Dict[str, Dict[str, object]]:
    """Return a copy of process-local sensor health telemetry."""
    return {key: dict(value) for key, value in _SOURCE_HEALTH.items()}

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
        "default_event_status": STATUS_AUTHORITY_CONFIRMED,
        "attribution_ar": (
            "بحسب مكتب مراقبة الأصول الأجنبية بوزارة الخزانة الأمريكية"
        ),
        "notes": (
            "المصدر الأولي للعقوبات والتعيينات والتحديثات المرتبطة بـOFAC."
        ),
    },
    {
        "id": "ofac_sanctions_updates",
        "entity": "United States",
        "entity_ar": "الولايات المتحدة",
        "organization": "Office of Foreign Assets Control",
        "organization_ar": "مكتب مراقبة الأصول الأجنبية - وزارة الخزانة الأمريكية",
        "source_type": TYPE_SANCTIONS,
        "grade": GRADE_A_PLUS,
        "role": ROLE_SPECIALIZED_SENSOR,
        "active": True,
        "url": "https://ofac.treasury.gov/recent-actions/sanctions-list-updates",
        "official_proof": "https://ofac.treasury.gov/",
        "regions": {"Global"},
        "topics": {
            "sanctions", "financial_sanctions", "asset_freeze",
            "designation", "international_security",
        },
        "product_types": {"sanctions_list_update"},
        "routing_hint": ROUTE_ECONOMY,
        "dedup_group": "ofac",
        "priority": 98,
        "poll_interval_seconds": 45,
        "timeout_seconds": 7,
        "default_event_status": STATUS_AUTHORITY_CONFIRMED,
        "attribution_ar": (
            "بحسب مكتب مراقبة الأصول الأجنبية بوزارة الخزانة الأمريكية"
        ),
        "notes": (
            "مسار متخصص داخل OFAC. نفس dedup_group لمنع تكرار المادة."
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
    if source_id not in {"ofac_recent_actions", "ofac_sanctions_updates"}:
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
        # The specialized page may expose action/detail links outside the
        # exact recent-actions pattern; keep only OFAC detail-like links.
        if source_id == "ofac_sanctions_updates" and not is_action:
            is_action = (
                "/sanctions-list-updates/" in low_path
                or "/sanctions/" in low_path
            )

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

    timeout_seconds = float(source.get("timeout_seconds", 5))
    timeout = aiohttp.ClientTimeout(
        total=timeout_seconds,
        connect=min(2.0, timeout_seconds),
    )

    try:
        async with session.get(
            source["url"],
            timeout=timeout,
            allow_redirects=True,
        ) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}")
            text = await _read_bounded(response)
        return text
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _circuit_failure(source_id)
        _set_source_health(source_id, HEALTH_FETCH_FAILED, detail=type(exc).__name__)
        log.warning("direct source failed: %s: %s", source_id, exc)
        return ""


def _document_matches_source(source_id: str, document: str) -> bool:
    """Reject HTTP-200 challenge/error pages before treating a sensor as healthy."""
    low = (document or "").lower()
    if not low:
        return False
    if source_id == "ukmto_warnings":
        return "ukmto" in low and any(token in low for token in ("warning", "incident", "maritime"))
    if source_id in {"ofac_recent_actions", "ofac_sanctions_updates"}:
        return "ofac" in low and any(token in low for token in ("recent actions", "sanctions", "treasury"))
    return False


async def _collect_one(
    session: aiohttp.ClientSession,
    source: Dict,
) -> List[Dict]:
    source_id = source["id"]
    if _circuit_is_open(source_id):
        _set_source_health(source_id, HEALTH_CIRCUIT_OPEN)
        return []

    _LAST_ATTEMPT[source_id] = time.monotonic()
    document = await _fetch_text(session, source)
    if not document:
        return []

    if not _document_matches_source(source_id, document):
        _circuit_failure(source_id)
        _set_source_health(source_id, HEALTH_PARSE_FAILED, detail="unexpected_document_shape")
        log.warning("direct source returned unexpected document shape: %s", source_id)
        return []

    try:
        if source_id == "ukmto_warnings":
            events = parse_ukmto_html(
                document,
                source_id=source_id,
                base_url=source["url"],
            )
        elif source_id in {"ofac_recent_actions", "ofac_sanctions_updates"}:
            events = parse_ofac_html(document, source_id=source_id)
        else:
            raise ValueError(f"Unsupported direct source: {source_id}")

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
            if source not in sources:
                _set_source_health(source["id"], HEALTH_NOT_DUE)

    if not sources:
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

        return deduplicate_radar_events(merged)

    if session is not None:
        return await run(session)

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
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
        "active", "url", "official_proof", "topics", "dedup_group",
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
    }


REGISTRY_ERRORS = validate_direct_sources()
if REGISTRY_ERRORS:
    raise RuntimeError(
        "direct_sources.py registry validation failed:\n- "
        + "\n- ".join(REGISTRY_ERRORS)
    )
