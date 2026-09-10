"""
Global-Intel-Bot
Direct Intelligence Radar Sources
=================================

هذا الملف مخصص للمصادر المباشرة عالية القيمة التي قد تسبق وكالات الأنباء، مثل:
- التحذيرات البحرية والأمنية
- العقوبات والقرارات المالية الدولية
- مخاطر الطيران ومناطق النزاع
- التنبيهات التشغيلية والمؤسسية المباشرة

هذا الملف مستقل عن:
- intel_sources.py  -> Telegram + X
- news_engine.py    -> منظومة الأخبار الحالية
- bot.py            -> المنسق والعرض

قواعد الرادار:
1) لا يدخل مصدر إلا إذا كان رسميًا أو مؤسسيًا موثوقًا بصورة واضحة.
2) درجة المصدر منفصلة عن درجة تأكيد الحدث.
3) المصدر A+ قد ينقل بلاغًا أوليًا أو من طرف ثالث؛ لا نحوله إلى حقيقة مؤكدة.
4) الخبر الواحد لا ينشر أكثر من مرة.
5) وصول مصدر أقوى لاحقًا يحدّث الحدث بدل إنشاء قصة ثانية.
6) محتوى الحدث هو الذي يحدد القسم النهائي، وليس اسم المصدر.
7) الرادار لا يغيّر news_engine.py ولا يعتمد عليه.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set


# =========================================================
# SOURCE GRADES
# =========================================================

GRADE_A_PLUS = "A+"
GRADE_A = "A"

VALID_GRADES = {
    GRADE_A_PLUS,
    GRADE_A,
}


# =========================================================
# SOURCE ROLES
# =========================================================

ROLE_PRIMARY_SENSOR = "primary_sensor"
ROLE_SPECIALIZED_SENSOR = "specialized_sensor"
ROLE_SUPPORT_SENSOR = "support_sensor"

VALID_ROLES = {
    ROLE_PRIMARY_SENSOR,
    ROLE_SPECIALIZED_SENSOR,
    ROLE_SUPPORT_SENSOR,
}


# =========================================================
# SOURCE TYPES
# =========================================================

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
# EVENT CONFIDENCE / STATUS
#
# مهم:
# هذه الحالات تصف الحدث، لا موثوقية المؤسسة.
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


# =========================================================
# RADAR TOPICS
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


# =========================================================
# ROUTING HINTS
#
# مجرد إشارة أولية.
# التصنيف النهائي يتم لاحقًا حسب مضمون الحدث.
# =========================================================

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
# DIRECT RADAR SOURCES
# =========================================================

DIRECT_SOURCES: List[Dict] = [

    # =====================================================
    # UKMTO
    # United Kingdom Maritime Trade Operations
    # =====================================================

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
            "Middle East",
            "Arabian Gulf",
            "Gulf of Oman",
            "Arabian Sea",
            "Red Sea",
            "Gulf of Aden",
            "Indian Ocean",
        },

        "topics": {
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
        },

        "product_types": {
            "warning",
        },

        "routing_hint": ROUTE_DEFENSE,
        "dedup_group": "ukmto",
        "priority": 100,

        # الرادار سريع، لكن لا نضعه داخل request path للمستخدم.
        "poll_interval_seconds": 30,
        "timeout_seconds": 5,

        # UKMTO قد ينقل بلاغات من سفن أو أطراف ثالثة.
        "default_event_status": STATUS_OFFICIAL_REPORT,

        "attribution_ar": "بحسب عمليات التجارة البحرية البريطانية (UKMTO)",

        "notes": (
            "مصدر أولي عالي القيمة للأمن البحري. "
            "يجب قراءة حالة كل Warning ومصدر البلاغ بدقة؛ "
            "وجود التحذير على UKMTO لا يعني أن تفاصيل الواقعة مؤكدة بالكامل."
        ),
    },


    # =====================================================
    # OFAC
    # U.S. Treasury - Office of Foreign Assets Control
    # =====================================================

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

        "regions": {
            "Global",
        },

        "topics": {
            "sanctions",
            "financial_sanctions",
            "asset_freeze",
            "designation",
            "international_security",
        },

        "product_types": {
            "sanctions_list_update",
            "general_license",
            "regulation",
            "guidance",
            "enforcement_action",
        },

        "routing_hint": ROUTE_ECONOMY,
        "dedup_group": "ofac",
        "priority": 100,

        "poll_interval_seconds": 60,
        "timeout_seconds": 5,

        "default_event_status": STATUS_AUTHORITY_CONFIRMED,

        "attribution_ar": "بحسب مكتب مراقبة الأصول الأجنبية بوزارة الخزانة الأمريكية",

        "notes": (
            "المصدر الأولي للعقوبات والتعيينات والتحديثات المرتبطة بـOFAC. "
            "إذا نقلت وكالة أنباء نفس الإجراء لاحقًا فلا يُنشأ خبر ثانٍ."
        ),
    },


    # =====================================================
    # OFAC - SANCTIONS LIST UPDATES
    #
    # مسار متخصص داخل OFAC لرفع سرعة التقاط التعيينات.
    # نفس dedup_group لمنع تكرار الحدث مع Recent Actions.
    # =====================================================

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

        "regions": {
            "Global",
        },

        "topics": {
            "sanctions",
            "financial_sanctions",
            "asset_freeze",
            "designation",
            "international_security",
        },

        "product_types": {
            "sanctions_list_update",
        },

        "routing_hint": ROUTE_ECONOMY,
        "dedup_group": "ofac",
        "priority": 98,

        "poll_interval_seconds": 45,
        "timeout_seconds": 5,

        "default_event_status": STATUS_AUTHORITY_CONFIRMED,

        "attribution_ar": "بحسب مكتب مراقبة الأصول الأجنبية بوزارة الخزانة الأمريكية",

        "notes": (
            "مسار سريع متخصص في تحديثات قوائم العقوبات. "
            "لا يُكرر المادة إذا التقطها ofac_recent_actions."
        ),
    },
]


# =========================================================
# INDEXES
# =========================================================

DIRECT_SOURCE_BY_ID: Dict[str, Dict] = {
    source["id"]: source
    for source in DIRECT_SOURCES
}


# =========================================================
# HELPERS
# =========================================================

def get_direct_source(source_id: str) -> Optional[Dict]:
    """Return one radar source by ID."""
    return DIRECT_SOURCE_BY_ID.get(source_id)


def get_active_direct_sources() -> List[Dict]:
    """
    Return active sources ordered by priority.

    لا يعني الترتيب أن مصدرًا ينتظر الآخر؛
    التنفيذ لاحقًا يجب أن يكون متوازيًا.
    """
    return sorted(
        (
            source
            for source in DIRECT_SOURCES
            if source.get("active", False)
        ),
        key=lambda source: (
            source.get("priority", 0),
            source.get("grade") == GRADE_A_PLUS,
        ),
        reverse=True,
    )


def get_sources_by_type(source_type: str) -> List[Dict]:
    """Return active radar sources for one source type."""
    return [
        source
        for source in get_active_direct_sources()
        if source.get("source_type") == source_type
    ]


def get_sources_for_topic(topic: str) -> List[Dict]:
    """Return active radar sources that cover a topic."""
    return [
        source
        for source in get_active_direct_sources()
        if topic in source.get("topics", set())
    ]


def get_dedup_group(source_id: str) -> Optional[str]:
    source = get_direct_source(source_id)
    if not source:
        return None
    return source.get("dedup_group")


def same_source_family(source_id_a: str, source_id_b: str) -> bool:
    """
    True when two radar entries belong to the same institutional/event family.

    مثال:
    ofac_recent_actions + ofac_sanctions_updates
    """
    group_a = get_dedup_group(source_id_a)
    group_b = get_dedup_group(source_id_b)

    return bool(
        group_a
        and group_b
        and group_a == group_b
    )


def should_prefer_direct_source(
    candidate_source_id: str,
    current_source_id: str,
) -> bool:
    """
    Decide whether a new radar source should replace the current source
    when both describe the same event.

    priority first, then grade.
    """
    candidate = get_direct_source(candidate_source_id)
    current = get_direct_source(current_source_id)

    if not candidate:
        return False

    if not current:
        return True

    candidate_priority = candidate.get("priority", 0)
    current_priority = current.get("priority", 0)

    if candidate_priority != current_priority:
        return candidate_priority > current_priority

    candidate_grade = candidate.get("grade")
    current_grade = current.get("grade")

    if candidate_grade != current_grade:
        return candidate_grade == GRADE_A_PLUS

    return False


# =========================================================
# EVENT STATUS HELPERS
# =========================================================

_EVENT_STATUS_STRENGTH = {
    STATUS_PRELIMINARY: 10,
    STATUS_THIRD_PARTY_REPORT: 20,
    STATUS_UNDER_INVESTIGATION: 30,
    STATUS_OFFICIAL_REPORT: 40,
    STATUS_ADVISORY: 50,
    STATUS_VERIFIED_REPORT: 80,
    STATUS_AUTHORITY_CONFIRMED: 100,
}


def event_status_strength(status: str) -> int:
    """Numeric confidence strength used only for update decisions."""
    return _EVENT_STATUS_STRENGTH.get(status, 0)


def stronger_event_status(
    candidate_status: str,
    current_status: str,
) -> bool:
    """Return True if candidate carries stronger confirmation."""
    return (
        event_status_strength(candidate_status)
        > event_status_strength(current_status)
    )


# =========================================================
# NORMALIZED RADAR EVENT
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
    """
    Normalize a direct-source alert without importing news_engine.py.

    هذا هو الشكل الذي سيربط لاحقًا مع bot.py عبر adapter صغير.
    """

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
        "title": (title or "").strip(),
        "original_title": (original_title or title or "").strip(),
        "summary": (summary or "").strip(),
        "url": (url or "").strip(),
        "published": published,
        "region": (region or "").strip(),
        "external_id": (external_id or "").strip(),
        "event_status": status,
        "event_status_strength": event_status_strength(status),
        "metadata": metadata or {},
    }


# =========================================================
# ARABIC ATTRIBUTION
# =========================================================

def attribution_prefix(source_id: str) -> str:
    source = get_direct_source(source_id)

    if not source:
        return ""

    return source.get("attribution_ar", "").strip()


def event_status_ar(status: str) -> str:
    labels = {
        STATUS_PRELIMINARY: "معلومات أولية",
        STATUS_THIRD_PARTY_REPORT: "بلاغ من طرف ثالث",
        STATUS_OFFICIAL_REPORT: "بلاغ رسمي",
        STATUS_UNDER_INVESTIGATION: "قيد التحقيق",
        STATUS_VERIFIED_REPORT: "بلاغ متحقق منه",
        STATUS_AUTHORITY_CONFIRMED: "مؤكد من الجهة المختصة",
        STATUS_ADVISORY: "تحذير/إرشاد رسمي",
    }

    return labels.get(status, "حالة غير محددة")


# =========================================================
# VALIDATION
# =========================================================

def validate_direct_sources() -> List[str]:
    """
    Validate registry consistency.

    لا يوجد أي اتصال بالشبكة هنا.
    """
    errors: List[str] = []
    seen_ids = set()

    required_fields = {
        "id",
        "entity",
        "organization",
        "source_type",
        "grade",
        "role",
        "active",
        "url",
        "official_proof",
        "topics",
        "dedup_group",
        "priority",
        "default_event_status",
    }

    for source in DIRECT_SOURCES:
        source_id = source.get("id", "<missing-id>")

        missing = required_fields - set(source.keys())
        if missing:
            errors.append(
                f"{source_id}: missing fields: {sorted(missing)}"
            )

        if source_id in seen_ids:
            errors.append(
                f"{source_id}: duplicate source id"
            )
        seen_ids.add(source_id)

        if source.get("grade") not in VALID_GRADES:
            errors.append(
                f"{source_id}: invalid grade"
            )

        if source.get("role") not in VALID_ROLES:
            errors.append(
                f"{source_id}: invalid role"
            )

        if source.get("source_type") not in VALID_SOURCE_TYPES:
            errors.append(
                f"{source_id}: invalid source_type"
            )

        if source.get("routing_hint") not in VALID_ROUTING_HINTS:
            errors.append(
                f"{source_id}: invalid routing_hint"
            )

        if source.get("default_event_status") not in VALID_EVENT_STATUSES:
            errors.append(
                f"{source_id}: invalid default_event_status"
            )

        topics = source.get("topics", set())
        unknown_topics = set(topics) - RADAR_TOPICS
        if unknown_topics:
            errors.append(
                f"{source_id}: unknown topics: {sorted(unknown_topics)}"
            )

        priority = source.get("priority")
        if not isinstance(priority, int) or not (0 <= priority <= 100):
            errors.append(
                f"{source_id}: priority must be 0..100"
            )

        poll_interval = source.get("poll_interval_seconds", 0)
        if (
            not isinstance(poll_interval, int)
            or poll_interval < 15
        ):
            errors.append(
                f"{source_id}: poll_interval_seconds too low/invalid"
            )

        timeout_seconds = source.get("timeout_seconds", 0)
        if (
            not isinstance(timeout_seconds, (int, float))
            or timeout_seconds <= 0
            or timeout_seconds > 15
        ):
            errors.append(
                f"{source_id}: invalid timeout_seconds"
            )

        url = str(source.get("url", ""))
        proof = str(source.get("official_proof", ""))

        if not url.startswith("https://"):
            errors.append(
                f"{source_id}: source URL must use HTTPS"
            )

        if not proof.startswith("https://"):
            errors.append(
                f"{source_id}: official proof URL must use HTTPS"
            )

    return errors


def registry_stats() -> Dict[str, int]:
    active = [
        source
        for source in DIRECT_SOURCES
        if source.get("active")
    ]

    return {
        "total": len(DIRECT_SOURCES),
        "active": len(active),
        "a_plus": sum(
            1
            for source in active
            if source.get("grade") == GRADE_A_PLUS
        ),
        "a": sum(
            1
            for source in active
            if source.get("grade") == GRADE_A
        ),
        "maritime": sum(
            1
            for source in active
            if source.get("source_type") == TYPE_MARITIME_SECURITY
        ),
        "sanctions": sum(
            1
            for source in active
            if source.get("source_type") == TYPE_SANCTIONS
        ),
        "aviation": sum(
            1
            for source in active
            if source.get("source_type") == TYPE_AVIATION_SECURITY
        ),
    }


# =========================================================
# STARTUP SAFETY
# =========================================================

REGISTRY_ERRORS = validate_direct_sources()

if REGISTRY_ERRORS:
    raise RuntimeError(
        "direct_sources.py registry validation failed:\n- "
        + "\n- ".join(REGISTRY_ERRORS)
    )
