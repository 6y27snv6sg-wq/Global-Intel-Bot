
"""
Global-Intel-Bot
Verified International Intelligence Sources
============================================

هذا الملف مخصص للمصادر الرسمية الدولية عبر:
- Telegram
- X

قواعد القبول:
1) لا يدخل المصدر إلا بعد إثبات رسميته من موقع حكومي/مؤسسي أصلي.
2) لا يكفي أن يكون رسميًا؛ يجب أن يحمل قيمة واضحة في الشؤون الدولية.
3) A+ = مصدر أولي عالي القيمة والأولوية.
4) A  = مصدر رسمي مفيد، لكنه ثانوي أو متخصص أو معرض للتكرار.
5) الحسابات اللغوية/الترجمة لا تُعامل كخبر مستقل إذا سبقها المصدر الرئيسي.
6) لا يحدد المصدر القسم النهائي للخبر؛ محتوى الحدث هو الذي يحدد القسم.
7) الخبر الواحد لا يظهر في أكثر من قسم.
8) الروايات والتصريحات الرسمية تُنسب إلى جهتها ولا تقدم كحقيقة مستقلة.

مهم:
- لا تضف handle بالتخمين.
- المصادر غير مكتملة الإثبات تبقى خارج VERIFIED_SOCIAL_SOURCES.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set


# =========================================================
# GRADES
# =========================================================

GRADE_A_PLUS = "A+"
GRADE_A = "A"

VALID_GRADES = {
    GRADE_A_PLUS,
    GRADE_A,
}


# =========================================================
# PLATFORMS
# =========================================================

PLATFORM_X = "x"
PLATFORM_TELEGRAM = "telegram"

VALID_PLATFORMS = {
    PLATFORM_X,
    PLATFORM_TELEGRAM,
}


# =========================================================
# SOURCE ROLES
# =========================================================

ROLE_PRIMARY = "primary"
ROLE_SUPPORT = "support"
ROLE_SPECIALIZED = "specialized"

VALID_ROLES = {
    ROLE_PRIMARY,
    ROLE_SUPPORT,
    ROLE_SPECIALIZED,
}


# =========================================================
# ROUTING HINTS
#
# هذه ليست تصنيفًا نهائيًا.
# الخبر يُصنف لاحقًا حسب مضمون الحدث.
# =========================================================

ROUTE_OFFICIAL = "official_statement"
ROUTE_DEFENSE = "defense_security"
ROUTE_ECONOMY = "economy_markets"
ROUTE_BREAKING = "breaking"
ROUTE_DYNAMIC = "dynamic"

VALID_ROUTING_HINTS = {
    ROUTE_OFFICIAL,
    ROUTE_DEFENSE,
    ROUTE_ECONOMY,
    ROUTE_BREAKING,
    ROUTE_DYNAMIC,
}


# =========================================================
# INTERNATIONAL VALUE TOPICS
# =========================================================

INTERNATIONAL_TOPICS: Set[str] = {
    "foreign_policy",
    "diplomacy",
    "international_relations",
    "geopolitics",
    "war",
    "conflict",
    "ceasefire",
    "mediation",
    "sanctions",
    "financial_sanctions",
    "defense",
    "military",
    "military_operations",
    "military_alliances",
    "international_security",
    "nuclear",
    "nuclear_security",
    "nuclear_safety",
    "safeguards",
    "peacekeeping",
    "strategic_affairs",
    "international_trade",
    "global_economy",
    "energy_security",
    "international_energy",
    "summits",
    "multilateral_relations",
    "international_decisions",
    "terrorism",
    "arms_control",
}


# =========================================================
# CONTENT TO EXCLUDE
# =========================================================

EXCLUDED_CONTENT_TYPES: Set[str] = {
    "local_services",
    "municipal_news",
    "routine_domestic_news",
    "domestic_ceremonies",
    "routine_protocol",
    "tourism",
    "local_events",
    "recruitment",
    "jobs",
    "internal_hr",
    "passport_services",
    "visa_services",
    "routine_consular_services",
    "customer_service",
    "traffic",
    "weather",
    "sports",
    "entertainment",
    "culture_only",
    "awareness_campaign",
    "commemoration_only",
    "human_interest_only",
}


# =========================================================
# VERIFIED SOCIAL SOURCES
# =========================================================

VERIFIED_SOCIAL_SOURCES: List[Dict] = [

    # =====================================================
    # MIDDLE EAST
    # =====================================================

    {
        "id": "oman_mofa_x",
        "entity": "Oman",
        "entity_ar": "سلطنة عُمان",
        "organization": "Ministry of Foreign Affairs",
        "organization_ar": "وزارة الخارجية العُمانية",
        "platform": PLATFORM_X,
        "handle": "FMofOman",
        "url": "https://x.com/FMofOman",
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY,
        "active": True,
        "language": "multi",
        "routing_hint": ROUTE_OFFICIAL,
        "topics": {
            "foreign_policy",
            "diplomacy",
            "international_relations",
            "mediation",
            "conflict",
        },
        "dedup_group": "oman_mofa",
        "priority": 100,
        "official_proof": "https://www.fm.gov.om/",
        "notes": (
            "مصدر أساسي للدبلوماسية العُمانية والوساطات "
            "والملفات الإقليمية والدولية."
        ),
    },


    # =====================================================
    # RUSSIA
    # =====================================================

    {
        "id": "russia_mofa_telegram",
        "entity": "Russia",
        "entity_ar": "روسيا",
        "organization": "Ministry of Foreign Affairs",
        "organization_ar": "وزارة الخارجية الروسية",
        "platform": PLATFORM_TELEGRAM,
        "handle": "MID_Russia",
        "url": "https://t.me/MID_Russia",
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY,
        "active": True,
        "language": "ru",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "foreign_policy",
            "diplomacy",
            "international_relations",
            "conflict",
            "sanctions",
            "international_security",
        },
        "dedup_group": "russia_mofa",
        "priority": 100,
        "official_proof": "https://mid.ru/",
        "notes": (
            "القناة الروسية الأساسية لوزارة الخارجية. "
            "تُفضّل على النسخة الإنجليزية عند تطابق الحدث."
        ),
    },

    {
        "id": "russia_mofa_telegram_en",
        "entity": "Russia",
        "entity_ar": "روسيا",
        "organization": "Ministry of Foreign Affairs",
        "organization_ar": "وزارة الخارجية الروسية",
        "platform": PLATFORM_TELEGRAM,
        "handle": "MFARussia",
        "url": "https://t.me/MFARussia",
        "grade": GRADE_A,
        "role": ROLE_SUPPORT,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "foreign_policy",
            "diplomacy",
            "international_relations",
            "conflict",
            "sanctions",
        },
        "dedup_group": "russia_mofa",
        "priority": 75,
        "official_proof": "https://mid.ru/",
        "notes": (
            "قناة إنجليزية مساندة. "
            "لا تنشر نفس الحدث مرة ثانية إذا وصل من MID_Russia."
        ),
    },


    # =====================================================
    # FRANCE
    # =====================================================

    {
        "id": "france_mofa_x",
        "entity": "France",
        "entity_ar": "فرنسا",
        "organization": "Ministry for Europe and Foreign Affairs",
        "organization_ar": "وزارة أوروبا والشؤون الخارجية الفرنسية",
        "platform": PLATFORM_X,
        "handle": "francediplo",
        "url": "https://x.com/francediplo",
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY,
        "active": True,
        "language": "fr",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "foreign_policy",
            "diplomacy",
            "international_relations",
            "sanctions",
            "conflict",
        },
        "dedup_group": "france_mofa",
        "priority": 100,
        "official_proof": "https://www.diplomatie.gouv.fr/",
        "notes": "الحساب الفرنسي الأساسي للخارجية.",
    },

    {
        "id": "france_mofa_x_en",
        "entity": "France",
        "entity_ar": "فرنسا",
        "organization": "Ministry for Europe and Foreign Affairs",
        "organization_ar": "وزارة أوروبا والشؤون الخارجية الفرنسية",
        "platform": PLATFORM_X,
        "handle": "francediplo_EN",
        "url": "https://x.com/francediplo_EN",
        "grade": GRADE_A,
        "role": ROLE_SUPPORT,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "foreign_policy",
            "diplomacy",
            "international_relations",
        },
        "dedup_group": "france_mofa",
        "priority": 75,
        "official_proof": "https://www.diplomatie.gouv.fr/",
        "notes": "نسخة إنجليزية مساندة وليست قصة مستقلة.",
    },

    {
        "id": "france_mofa_x_ar",
        "entity": "France",
        "entity_ar": "فرنسا",
        "organization": "Ministry for Europe and Foreign Affairs",
        "organization_ar": "وزارة أوروبا والشؤون الخارجية الفرنسية",
        "platform": PLATFORM_X,
        "handle": "francediplo_AR",
        "url": "https://x.com/francediplo_AR",
        "grade": GRADE_A,
        "role": ROLE_SUPPORT,
        "active": True,
        "language": "ar",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "foreign_policy",
            "diplomacy",
            "international_relations",
        },
        "dedup_group": "france_mofa",
        "priority": 72,
        "official_proof": "https://www.diplomatie.gouv.fr/",
        "notes": (
            "مفيد للنص العربي الرسمي، "
            "لكن لا يُنشئ خبرًا جديدًا إذا وُجد الحدث من الحساب الأساسي."
        ),
    },


    # =====================================================
    # GERMANY
    # =====================================================

    {
        "id": "germany_mofa_x",
        "entity": "Germany",
        "entity_ar": "ألمانيا",
        "organization": "Federal Foreign Office",
        "organization_ar": "وزارة الخارجية الألمانية",
        "platform": PLATFORM_X,
        "handle": "AuswaertigesAmt",
        "url": "https://x.com/AuswaertigesAmt",
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY,
        "active": True,
        "language": "de",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "foreign_policy",
            "diplomacy",
            "international_relations",
            "international_security",
            "conflict",
        },
        "dedup_group": "germany_mofa",
        "priority": 100,
        "official_proof": "https://www.auswaertiges-amt.de/",
        "notes": (
            "الخارجية الألمانية أثبتت الحساب مجددًا في محتواها الرسمي لعام 2026."
        ),
    },

    {
        "id": "germany_mofa_x_en",
        "entity": "Germany",
        "entity_ar": "ألمانيا",
        "organization": "Federal Foreign Office",
        "organization_ar": "وزارة الخارجية الألمانية",
        "platform": PLATFORM_X,
        "handle": "GermanyDiplo",
        "url": "https://x.com/GermanyDiplo",
        "grade": GRADE_A,
        "role": ROLE_SUPPORT,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "foreign_policy",
            "diplomacy",
            "international_relations",
            "international_security",
        },
        "dedup_group": "germany_mofa",
        "priority": 78,
        "official_proof": "https://www.auswaertiges-amt.de/",
        "notes": (
            "الحساب الإنجليزي الرسمي؛ "
            "يستخدم كمساند للحساب الألماني الأساسي."
        ),
    },


    # =====================================================
    # UNITED KINGDOM - FOREIGN AFFAIRS
    # =====================================================

    {
        "id": "uk_fcdo_x",
        "entity": "United Kingdom",
        "entity_ar": "المملكة المتحدة",
        "organization": "Foreign, Commonwealth & Development Office",
        "organization_ar": "وزارة الخارجية والتنمية البريطانية",
        "platform": PLATFORM_X,
        "handle": "FCDOGovUK",
        "url": "https://x.com/FCDOGovUK",
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "foreign_policy",
            "diplomacy",
            "international_relations",
            "sanctions",
            "conflict",
            "international_security",
        },
        "dedup_group": "uk_fcdo",
        "priority": 100,
        "official_proof": "https://www.gov.uk/government/organisations/foreign-commonwealth-development-office",
        "notes": "الحساب المؤسسي الأساسي للخارجية البريطانية.",
    },

    {
        "id": "uk_fcdo_x_ar",
        "entity": "United Kingdom",
        "entity_ar": "المملكة المتحدة",
        "organization": "Foreign, Commonwealth & Development Office",
        "organization_ar": "وزارة الخارجية والتنمية البريطانية",
        "platform": PLATFORM_X,
        "handle": "FCDOArabic",
        "url": "https://x.com/FCDOArabic",
        "grade": GRADE_A,
        "role": ROLE_SUPPORT,
        "active": True,
        "language": "ar",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "foreign_policy",
            "diplomacy",
            "international_relations",
        },
        "dedup_group": "uk_fcdo",
        "priority": 72,
        "official_proof": "https://www.gov.uk/government/organisations/foreign-commonwealth-development-office",
        "notes": (
            "يستخدم لدعم الترجمة العربية فقط عند تطابق الحدث "
            "ولا يُعامل كمصدر قصة منفصل."
        ),
    },


    # =====================================================
    # UNITED KINGDOM - DEFENCE
    # =====================================================

    {
        "id": "uk_mod_press_x",
        "entity": "United Kingdom",
        "entity_ar": "المملكة المتحدة",
        "organization": "Ministry of Defence",
        "organization_ar": "وزارة الدفاع البريطانية",
        "platform": PLATFORM_X,
        "handle": "DefenceHQPress",
        "url": "https://x.com/DefenceHQPress",
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DEFENSE,
        "topics": {
            "defense",
            "military",
            "military_operations",
            "international_security",
            "military_alliances",
            "conflict",
        },
        "dedup_group": "uk_mod",
        "priority": 100,
        "official_proof": "https://www.gov.uk/government/organisations/ministry-of-defence",
        "notes": (
            "الأولوية للمواد الصحفية والعملياتية الدولية "
            "على الحساب العام لوزارة الدفاع."
        ),
    },

    {
        "id": "uk_mod_x",
        "entity": "United Kingdom",
        "entity_ar": "المملكة المتحدة",
        "organization": "Ministry of Defence",
        "organization_ar": "وزارة الدفاع البريطانية",
        "platform": PLATFORM_X,
        "handle": "DefenceHQ",
        "url": "https://x.com/DefenceHQ",
        "grade": GRADE_A,
        "role": ROLE_SUPPORT,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DEFENSE,
        "topics": {
            "defense",
            "military",
            "international_security",
        },
        "dedup_group": "uk_mod",
        "priority": 80,
        "official_proof": "https://www.gov.uk/government/organisations/ministry-of-defence",
        "notes": "مصدر مساند؛ يمنع تكراره مع DefenceHQPress.",
    },


    # =====================================================
    # JAPAN
    # =====================================================

    {
        "id": "japan_mofa_x_en",
        "entity": "Japan",
        "entity_ar": "اليابان",
        "organization": "Ministry of Foreign Affairs",
        "organization_ar": "وزارة الخارجية اليابانية",
        "platform": PLATFORM_X,
        "handle": "MofaJapan_en",
        "url": "https://x.com/MofaJapan_en",
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "foreign_policy",
            "diplomacy",
            "international_relations",
            "international_security",
            "sanctions",
            "strategic_affairs",
        },
        "dedup_group": "japan_mofa",
        "priority": 100,
        "official_proof": "https://www.mofa.go.jp/",
        "notes": "الحساب الإنجليزي الرسمي مناسب مباشرة للرصد الدولي.",
    },


    # =====================================================
    # AUSTRALIA
    # =====================================================

    {
        "id": "australia_dfat_x",
        "entity": "Australia",
        "entity_ar": "أستراليا",
        "organization": "Department of Foreign Affairs and Trade",
        "organization_ar": "وزارة الخارجية والتجارة الأسترالية",
        "platform": PLATFORM_X,
        "handle": "DFAT",
        "url": "https://x.com/DFAT",
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "foreign_policy",
            "diplomacy",
            "international_relations",
            "sanctions",
            "international_trade",
            "international_security",
        },
        "dedup_group": "australia_dfat",
        "priority": 100,
        "official_proof": "https://www.dfat.gov.au/",
        "notes": (
            "تُستبعد منه تحذيرات السفر والخدمات القنصلية الروتينية."
        ),
    },


    # =====================================================
    # INDIA
    # =====================================================

    {
        "id": "india_mea_x",
        "entity": "India",
        "entity_ar": "الهند",
        "organization": "Ministry of External Affairs",
        "organization_ar": "وزارة الشؤون الخارجية الهندية",
        "platform": PLATFORM_X,
        "handle": "MEAIndia",
        "url": "https://x.com/MEAIndia",
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "foreign_policy",
            "diplomacy",
            "international_relations",
            "strategic_affairs",
            "international_security",
            "conflict",
        },
        "dedup_group": "india_mea",
        "priority": 100,
        "official_proof": "https://www.mea.gov.in/",
        "notes": (
            "تُستبعد حسابات الجوازات والخدمات القنصلية "
            "ولا تضاف لهذا السجل."
        ),
    },


    # =====================================================
    # EUROPEAN UNION - EEAS
    # =====================================================

    {
        "id": "eu_eeas_x",
        "entity": "European Union",
        "entity_ar": "الاتحاد الأوروبي",
        "organization": "European External Action Service",
        "organization_ar": "دائرة العمل الخارجي الأوروبية",
        "platform": PLATFORM_X,
        "handle": "eu_eeas",
        "url": "https://x.com/eu_eeas",
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "foreign_policy",
            "diplomacy",
            "international_relations",
            "sanctions",
            "conflict",
            "international_security",
        },
        "dedup_group": "eu_eeas",
        "priority": 100,
        "official_proof": "https://www.eeas.europa.eu/",
        "notes": "المصدر الأساسي للسياسة الخارجية للاتحاد الأوروبي.",
    },

    {
        "id": "eu_security_defence_x",
        "entity": "European Union",
        "entity_ar": "الاتحاد الأوروبي",
        "organization": "EU Security and Defence",
        "organization_ar": "الأمن والدفاع في الاتحاد الأوروبي",
        "platform": PLATFORM_X,
        "handle": "EUSec_Defence",
        "url": "https://x.com/EUSec_Defence",
        "grade": GRADE_A,
        "role": ROLE_SPECIALIZED,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DEFENSE,
        "topics": {
            "defense",
            "military",
            "international_security",
            "military_operations",
        },
        "dedup_group": "eu_security_defence",
        "priority": 88,
        "official_proof": "https://www.eeas.europa.eu/",
        "notes": "يُستخدم فقط للمواد ذات القيمة الدفاعية والأمنية الدولية.",
    },


    # =====================================================
    # COUNCIL OF THE EUROPEAN UNION
    # =====================================================

    {
        "id": "eu_council_press_x",
        "entity": "European Union",
        "entity_ar": "الاتحاد الأوروبي",
        "organization": "Council of the European Union",
        "organization_ar": "مجلس الاتحاد الأوروبي",
        "platform": PLATFORM_X,
        "handle": "EUCouncilPress",
        "url": "https://x.com/EUCouncilPress",
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "sanctions",
            "foreign_policy",
            "international_decisions",
            "summits",
            "international_security",
            "defense",
        },
        "dedup_group": "eu_council",
        "priority": 100,
        "official_proof": "https://www.consilium.europa.eu/",
        "notes": (
            "قوي جدًا للعقوبات وقرارات المجلس "
            "والقمم والسياسة الخارجية."
        ),
    },


    # =====================================================
    # NATO
    # =====================================================

    {
        "id": "nato_press_x",
        "entity": "NATO",
        "entity_ar": "حلف شمال الأطلسي",
        "organization": "NATO Press",
        "organization_ar": "المكتب الصحفي لحلف شمال الأطلسي",
        "platform": PLATFORM_X,
        "handle": "NATOPress",
        "url": "https://x.com/NATOPress",
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DEFENSE,
        "topics": {
            "defense",
            "military",
            "military_operations",
            "military_alliances",
            "international_security",
            "conflict",
        },
        "dedup_group": "nato",
        "priority": 100,
        "official_proof": "https://www.nato.int/",
        "notes": "الأولوية لأخبار وبيانات الناتو ذات القيمة الدولية.",
    },

    {
        "id": "nato_general_x",
        "entity": "NATO",
        "entity_ar": "حلف شمال الأطلسي",
        "organization": "NATO",
        "organization_ar": "حلف شمال الأطلسي",
        "platform": PLATFORM_X,
        "handle": "NATO",
        "url": "https://x.com/NATO",
        "grade": GRADE_A,
        "role": ROLE_SUPPORT,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DEFENSE,
        "topics": {
            "defense",
            "military_alliances",
            "international_security",
        },
        "dedup_group": "nato",
        "priority": 82,
        "official_proof": "https://www.nato.int/",
        "notes": "يمنع تكراره إذا سبق NATOPress بنفس الحدث.",
    },

    {
        "id": "nato_cmc_x",
        "entity": "NATO",
        "entity_ar": "حلف شمال الأطلسي",
        "organization": "NATO Military Committee",
        "organization_ar": "اللجنة العسكرية لحلف شمال الأطلسي",
        "platform": PLATFORM_X,
        "handle": "CMC_NATO",
        "url": "https://x.com/CMC_NATO",
        "grade": GRADE_A,
        "role": ROLE_SPECIALIZED,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DEFENSE,
        "topics": {
            "defense",
            "military",
            "international_security",
            "strategic_affairs",
        },
        "dedup_group": "nato_military",
        "priority": 85,
        "official_proof": "https://www.nato.int/",
        "notes": "يُقبل عندما يقدم تطورًا عسكريًا دوليًا فريدًا.",
    },

    {
        "id": "nato_pascad_x",
        "entity": "NATO",
        "entity_ar": "حلف شمال الأطلسي",
        "organization": "NATO International Military Staff",
        "organization_ar": "الهيئة العسكرية الدولية في الناتو",
        "platform": PLATFORM_X,
        "handle": "NATO_PASCAD",
        "url": "https://x.com/NATO_PASCAD",
        "grade": GRADE_A,
        "role": ROLE_SPECIALIZED,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DEFENSE,
        "topics": {
            "defense",
            "military",
            "international_security",
        },
        "dedup_group": "nato_military",
        "priority": 82,
        "official_proof": "https://www.nato.int/",
        "notes": "مصدر عسكري متخصص، وليس بديلًا عن NATOPress.",
    },


    # =====================================================
    # IAEA
    # =====================================================

    {
        "id": "iaea_x",
        "entity": "IAEA",
        "entity_ar": "الوكالة الدولية للطاقة الذرية",
        "organization": "International Atomic Energy Agency",
        "organization_ar": "الوكالة الدولية للطاقة الذرية",
        "platform": PLATFORM_X,
        "handle": "IAEAorg",
        "url": "https://x.com/IAEAorg",
        "grade": GRADE_A_PLUS,
        "role": ROLE_PRIMARY,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DYNAMIC,
        "topics": {
            "nuclear",
            "nuclear_security",
            "nuclear_safety",
            "safeguards",
            "international_security",
            "conflict",
        },
        "dedup_group": "iaea",
        "priority": 100,
        "official_proof": "https://www.iaea.org/",
        "notes": (
            "مصدر أولي للضمانات والتفتيش والمنشآت النووية. "
            "القسم يتحدد حسب مضمون الحدث."
        ),
    },


    # =====================================================
    # UNITED NATIONS PEACEKEEPING
    # =====================================================

    {
        "id": "un_peacekeeping_x",
        "entity": "United Nations",
        "entity_ar": "الأمم المتحدة",
        "organization": "United Nations Peacekeeping",
        "organization_ar": "عمليات حفظ السلام التابعة للأمم المتحدة",
        "platform": PLATFORM_X,
        "handle": "UNPeacekeeping",
        "url": "https://x.com/UNPeacekeeping",
        "grade": GRADE_A,
        "role": ROLE_SPECIALIZED,
        "active": True,
        "language": "en",
        "routing_hint": ROUTE_DEFENSE,
        "topics": {
            "peacekeeping",
            "conflict",
            "ceasefire",
            "international_security",
        },
        "dedup_group": "un_peacekeeping",
        "priority": 86,
        "official_proof": "https://peacekeeping.un.org/",
        "notes": (
            "تُستبعد الحملات التوعوية والمناسبات والمحتوى الإنساني "
            "الذي لا يحمل تطورًا دوليًا."
        ),
    },
]


# =========================================================
# VERIFIED SOURCE INDEXES
# =========================================================

VERIFIED_SOURCE_BY_ID: Dict[str, Dict] = {
    source["id"]: source
    for source in VERIFIED_SOCIAL_SOURCES
}


VERIFIED_SOURCE_BY_PLATFORM_HANDLE: Dict[str, Dict] = {
    f'{source["platform"]}:{source["handle"].lower()}': source
    for source in VERIFIED_SOCIAL_SOURCES
}


# =========================================================
# HELPERS
# =========================================================

def normalize_handle(handle: str) -> str:
    return (handle or "").strip().lstrip("@").lower()


def get_source_by_id(source_id: str) -> Optional[Dict]:
    if not source_id:
        return None

    source = VERIFIED_SOURCE_BY_ID.get(source_id)

    if not source or not source.get("active"):
        return None

    return source


def get_source(
    platform: str,
    handle: str,
) -> Optional[Dict]:
    platform = (platform or "").strip().lower()
    handle = normalize_handle(handle)

    if not platform or not handle:
        return None

    source = VERIFIED_SOURCE_BY_PLATFORM_HANDLE.get(
        f"{platform}:{handle}"
    )

    if not source or not source.get("active"):
        return None

    return source


def is_verified_source(
    platform: str,
    handle: str,
) -> bool:
    return get_source(platform, handle) is not None


def get_active_sources(
    platform: Optional[str] = None,
    grade: Optional[str] = None,
    primary_only: bool = False,
) -> List[Dict]:

    results: List[Dict] = []

    for source in VERIFIED_SOCIAL_SOURCES:

        if not source.get("active"):
            continue

        if platform and source.get("platform") != platform:
            continue

        if grade and source.get("grade") != grade:
            continue

        if primary_only and source.get("role") != ROLE_PRIMARY:
            continue

        results.append(source)

    return sorted(
        results,
        key=lambda item: (
            item.get("priority", 0),
            item.get("grade") == GRADE_A_PLUS,
        ),
        reverse=True,
    )


def get_primary_source_for_group(
    dedup_group: str,
) -> Optional[Dict]:

    if not dedup_group:
        return None

    candidates = [
        source
        for source in VERIFIED_SOCIAL_SOURCES
        if source.get("active")
        and source.get("dedup_group") == dedup_group
    ]

    if not candidates:
        return None

    candidates.sort(
        key=lambda item: (
            item.get("role") == ROLE_PRIMARY,
            item.get("grade") == GRADE_A_PLUS,
            item.get("priority", 0),
        ),
        reverse=True,
    )

    return candidates[0]


def get_dedup_group(
    platform: str,
    handle: str,
) -> Optional[str]:

    source = get_source(platform, handle)

    if not source:
        return None

    return source.get("dedup_group")


def should_prefer_source(
    candidate: Dict,
    existing: Dict,
) -> bool:
    """
    True إذا كان candidate أفضل من existing
    عند تغطيتهما لنفس الحدث.
    """

    if not candidate:
        return False

    if not existing:
        return True

    candidate_role = candidate.get("role")
    existing_role = existing.get("role")

    if (
        candidate_role == ROLE_PRIMARY
        and existing_role != ROLE_PRIMARY
    ):
        return True

    if (
        existing_role == ROLE_PRIMARY
        and candidate_role != ROLE_PRIMARY
    ):
        return False

    candidate_grade = candidate.get("grade")
    existing_grade = existing.get("grade")

    if (
        candidate_grade == GRADE_A_PLUS
        and existing_grade != GRADE_A_PLUS
    ):
        return True

    if (
        existing_grade == GRADE_A_PLUS
        and candidate_grade != GRADE_A_PLUS
    ):
        return False

    return (
        candidate.get("priority", 0)
        >
        existing.get("priority", 0)
    )


def should_suppress_duplicate_source(
    candidate: Dict,
    selected_source: Dict,
) -> bool:
    """
    يمنع الحسابات اللغوية أو الثانوية من إعادة نفس الحدث.
    """

    if not candidate or not selected_source:
        return False

    candidate_group = candidate.get("dedup_group")
    selected_group = selected_source.get("dedup_group")

    if not candidate_group or not selected_group:
        return False

    if candidate_group != selected_group:
        return False

    return not should_prefer_source(
        candidate,
        selected_source,
    )


def source_has_international_value(source: Dict) -> bool:

    if not source:
        return False

    topics = set(source.get("topics") or set())

    return bool(topics & INTERNATIONAL_TOPICS)


def should_accept_content_type(
    content_type: Optional[str],
) -> bool:

    if not content_type:
        return True

    return content_type not in EXCLUDED_CONTENT_TYPES


def attribution_prefix(source: Dict) -> str:
    """
    صيغة آمنة لنسبة التصريح إلى الجهة الرسمية.
    """

    if not source:
        return "بحسب المصدر الرسمي"

    organization_ar = source.get("organization_ar")

    if organization_ar:
        return f"بحسب {organization_ar}"

    entity_ar = source.get("entity_ar")

    if entity_ar:
        return f"بحسب الجهة الرسمية في {entity_ar}"

    return "بحسب المصدر الرسمي"


# =========================================================
# REGISTRY VALIDATION
# =========================================================

def validate_registry() -> List[str]:

    errors: List[str] = []

    required_fields = {
        "id",
        "entity",
        "entity_ar",
        "organization",
        "organization_ar",
        "platform",
        "handle",
        "url",
        "grade",
        "role",
        "active",
        "language",
        "routing_hint",
        "topics",
        "dedup_group",
        "priority",
        "official_proof",
    }

    seen_ids: Set[str] = set()
    seen_handles: Set[str] = set()

    for source in VERIFIED_SOCIAL_SOURCES:

        source_id = source.get("id", "UNKNOWN")

        missing = required_fields - set(source.keys())

        if missing:
            errors.append(
                f"{source_id}: missing fields: {sorted(missing)}"
            )
            continue

        if source_id in seen_ids:
            errors.append(
                f"duplicate source id: {source_id}"
            )

        seen_ids.add(source_id)

        key = (
            f'{source["platform"]}:'
            f'{normalize_handle(source["handle"])}'
        )

        if key in seen_handles:
            errors.append(
                f"duplicate source handle: {key}"
            )

        seen_handles.add(key)

        if source["grade"] not in VALID_GRADES:
            errors.append(
                f"{source_id}: invalid grade"
            )

        if source["platform"] not in VALID_PLATFORMS:
            errors.append(
                f"{source_id}: invalid platform"
            )

        if source["role"] not in VALID_ROLES:
            errors.append(
                f"{source_id}: invalid role"
            )

        if source["routing_hint"] not in VALID_ROUTING_HINTS:
            errors.append(
                f"{source_id}: invalid routing_hint"
            )

        if not source_has_international_value(source):
            errors.append(
                f"{source_id}: no international-value topic"
            )

        if not str(source["url"]).startswith("https://"):
            errors.append(
                f"{source_id}: invalid source url"
            )

        if not str(source["official_proof"]).startswith(
            "https://"
        ):
            errors.append(
                f"{source_id}: invalid official proof"
            )

    return errors


def registry_stats() -> Dict[str, int]:

    active = [
        source
        for source in VERIFIED_SOCIAL_SOURCES
        if source.get("active")
    ]

    return {
        "total": len(active),
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
        "x": sum(
            1
            for source in active
            if source.get("platform") == PLATFORM_X
        ),
        "telegram": sum(
            1
            for source in active
            if source.get("platform") == PLATFORM_TELEGRAM
        ),
        "primary": sum(
            1
            for source in active
            if source.get("role") == ROLE_PRIMARY
        ),
        "support": sum(
            1
            for source in active
            if source.get("role") == ROLE_SUPPORT
        ),
        "specialized": sum(
            1
            for source in active
            if source.get("role") == ROLE_SPECIALIZED
        ),
    }


REGISTRY_ERRORS = validate_registry()

if REGISTRY_ERRORS:
    raise RuntimeError(
        "intel_sources registry validation failed:\n- "
        + "\n- ".join(REGISTRY_ERRORS)
    )
